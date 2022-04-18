// Copyright 1016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3rpc

import (
	"context"
	"fmt"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	adt "go.etcd.io/etcd/pkg/v3/adt"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"golang.org/x/time/rate"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTotalMemoryBudget            = 1 * 1024 * 1024 * 1024
	DefaultDegradedBandwidthBytesPerSec = 64 * 1024 * 1024
	DefaultBurstBytes                   = 64 * 1024 * 1024
	DefaultRespSize                     = 64 * 1024
	DefaultResetTimer                   = 1 * time.Minute
	DefaultAuditThresholdPercent        = 50
	SmallReqThreshold                   = 4 * 1024
)

type QueryType int64

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeRange
)

type Query struct {
	qid   string
	qsize uint64
	qtype QueryType
}

type QueryMonitor interface {
	//Start the monitoring
	Start()

	//Update counters
	UpdateUsage(req interface{}, resp interface{})

	// Decide if we can admit the request.
	AdmitReq(req interface{}) bool

	//Stop the monitoring
	Stop()
}

type BandwidthMonitor struct {
	totalMemoryBudget            uint64
	defaultRespSize              uint64
	degradedBandwidthBytesPerSec uint64
	burstBytes                   uint64
	budgetExhausted              bool
	resetTimer                   time.Duration
	estRespSize                  *adt.CountMinSketch
	auditThresholdPercent        uint64
	auditOn                      bool
	throttle                     *rate.Limiter
	mu                           sync.Mutex
	server                       *etcdserver.EtcdServer
}

func NewQueryMonitor(s *etcdserver.EtcdServer) QueryMonitor {
	var qm BandwidthMonitor
	qm.totalMemoryBudget = DefaultTotalMemoryBudget
	qm.defaultRespSize = DefaultRespSize
	qm.degradedBandwidthBytesPerSec = DefaultDegradedBandwidthBytesPerSec
	qm.burstBytes = DefaultBurstBytes
	qm.resetTimer = DefaultResetTimer
	qm.estRespSize, _ = adt.NewWithEstimates(0.0001, 0.9999)
	qm.auditOn = false
	qm.auditThresholdPercent = DefaultAuditThresholdPercent
	qm.throttle = rate.NewLimiter(rate.Every(time.Second/time.Duration(qm.degradedBandwidthBytesPerSec)), int(qm.burstBytes))
	qm.budgetExhausted = false
	qm.server = s
	return &qm
}

func (ctrl *BandwidthMonitor) Start() {
	go ctrl.start()
}

func (ctrl *BandwidthMonitor) start() {
	ctrl.update()
	ctrl.periodicReset()
}

func (ctrl *BandwidthMonitor) periodicReset() {
	ticker := time.NewTicker(ctrl.resetTimer)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctrl.update()
		case <-ctrl.server.StoppingNotify():
			return
		}
	}
}

func (ctrl *BandwidthMonitor) update() {
	//TODO err handling
	rss := getCurrentRssBytes()
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	ctrl.resetRespSizeUnsafe()
	ctrl.updateBudgetUnsafe(uint64(rss))
	ctrl.updateAuditFlagUnsafe(uint64(rss))
}

func (ctrl *BandwidthMonitor) resetRespSizeUnsafe() {
	ctrl.estRespSize, _ = adt.NewWithEstimates(0.0001, 0.9999)
}

func (ctrl *BandwidthMonitor) updateAuditFlagUnsafe(rss uint64) {
	ctrl.auditOn = true
}

func (ctrl *BandwidthMonitor) updateBudgetUnsafe(rss uint64) {
	if ctrl.totalMemoryBudget <= uint64(rss) {
		ctrl.budgetExhausted = true
		runtime.GC()
	} else {
		ctrl.budgetExhausted = false
	}
}

func (ctrl *BandwidthMonitor) isDeclinedHelper(q Query) (bool, uint64) {
	respSize := q.qsize
	if q.qtype == QueryTypeRange {
		respSize = ctrl.estRespSize.EstimateString(q.qid)
		if respSize == 0 {
			//We have not seen a response for this type of query.
			//Estimate based on default
			respSize = ctrl.defaultRespSize
		}
		q.qsize = respSize
	}
	if ctrl.budgetExhausted {
		//decline
		return true, respSize
	}

	//admit
	return false, respSize
}

func (ctrl *BandwidthMonitor) newQuery(req interface{}, resp interface{}) Query {
	var reqSize, respSize int
	var reqContent string
	qtype := QueryTypeUnknown
	switch _resp := resp.(type) {
	case *pb.RangeResponse:
		//TODO verify status is 200
		_req, ok := req.(*pb.RangeRequest)
		if ok {
			reqSize = _req.Size()
			reqContent = _req.String()
			qtype = QueryTypeRange
		}
		if _resp != nil {
			respSize = _resp.Size()
		}
	default:
		reqSize = 0
		respSize = 0
	}

	var q Query
	q.qid = reqContent
	q.qsize = uint64(reqSize + respSize)
	q.qtype = qtype
	return q
}

func (ctrl *BandwidthMonitor) newQueryFromReq(req interface{}) Query {
	var q Query
	switch req.(type) {
	case *pb.RangeRequest:
		_req, ok := req.(*pb.RangeRequest)
		if ok {
			q = ctrl.newQRange(_req)
		}
	default:
	}
	return q
}

func (ctrl *BandwidthMonitor) newQRange(r *pb.RangeRequest) Query {
	var q Query
	q.qid = r.String()
	q.qsize = 0
	q.qtype = QueryTypeRange
	return q
}

func (ctrl *BandwidthMonitor) UpdateUsage(req interface{}, resp interface{}) {
	q := ctrl.newQuery(req, resp)
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	ctrl.estRespSize.UpdateString(q.qid, q.qsize)
	if ctrl.auditOn && q.qtype != QueryTypeUnknown {
		//TODO use the logger passed from server cfg
		fmt.Printf("Audit Qid: %s Qsize: %d\n", q.qid, q.qsize)
	}
}

func (ctrl *BandwidthMonitor) AdmitReq(req interface{}) bool {
	q := ctrl.newQueryFromReq(req)
	declined, qsize := ctrl.isDeclined(q)
	if qsize > SmallReqThreshold && declined {
		err := ctrl.throttle.WaitN(context.TODO(), int(qsize))
		if err != nil {
			fmt.Printf("Throttled failed. Reject.%s\n", err)
			return false
		}
	}
	return true
}

func (ctrl *BandwidthMonitor) isDeclined(q Query) (bool, uint64) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	return ctrl.isDeclinedHelper(q)
}

func (ctrl *BandwidthMonitor) Stop() {
}

//TODO windows
func getCurrentRssBytes() uint64 {
	pid := os.Getpid()
	statm := fmt.Sprintf("/proc/%d/statm", pid)
	buf, err := ioutil.ReadFile(statm)
	if err != nil {
		fmt.Printf("Failed to read: %s err:%s\n", statm, err)
		return 0
	}

	fields := strings.Split(string(buf), " ")
	if len(fields) < 2 {
		fmt.Printf("Failed to parse: %s \n", statm)
		return 0
	}

	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		fmt.Printf("getCurrentRssBytes:cannot convert to int: err:%s\n", err)
		return 0
	}

	return rss * uint64(os.Getpagesize())
}

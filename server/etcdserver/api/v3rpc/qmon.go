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
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTotalMemoryBudget            = 1 * 1024 * 1024 * 1024
	DefaultDegradedBandwidthBytesPerSec = 64 * 1024 * 1024
	DefaultBurstBytes                   = 64 * 1024 * 1024
	DefaultRespSize                     = 4 * 1024 * 1024
	DefaultResetTimer                   = 10 * time.Second
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
	// Start the monitoring
	Start()

	// UpdateUsage : update counters
	UpdateUsage(req interface{}, resp interface{})

	// AdmitReq : Decide if we can admit the request.
	AdmitReq(req interface{}) bool

	// Stop the monitoring
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
	if s.Cfg.ExperimentalQmonMemoryBudgetMegabytes != 0 {
		qm.totalMemoryBudget = uint64(s.Cfg.ExperimentalQmonMemoryBudgetMegabytes)
	}
	qm.defaultRespSize = DefaultRespSize
	qm.degradedBandwidthBytesPerSec = DefaultDegradedBandwidthBytesPerSec
	if s.Cfg.ExperimentalQmonThrottleBandwidthMBPS != 0 {
		qm.degradedBandwidthBytesPerSec = uint64(s.Cfg.ExperimentalQmonThrottleBandwidthMBPS)
	}
	qm.burstBytes = DefaultBurstBytes
	qm.resetTimer = DefaultResetTimer
	if s.Cfg.ExperimentalQmonEvalInterval != 0 {
		qm.resetTimer = s.Cfg.ExperimentalQmonEvalInterval
	}
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
	rss := getCurrentRssBytes(ctrl.server.Cfg.Logger)
	if rss == 0 {
		ctrl.server.Cfg.Logger.Error("qmon: unexpected condition rss is zero.")
		return
	}
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
	if (ctrl.totalMemoryBudget*ctrl.auditThresholdPercent)/100 > uint64(rss) {
		ctrl.auditOn = true
	} else {
		ctrl.auditOn = false
	}
}

func (ctrl *BandwidthMonitor) updateBudgetUnsafe(rss uint64) {
	if ctrl.totalMemoryBudget <= uint64(rss) {
		ctrl.budgetExhausted = true
		debug.FreeOSMemory()
		ctrl.server.Cfg.Logger.Warn("qmon: Running FreeOSMemory.")
	} else {
		ctrl.budgetExhausted = false
	}
}

func (ctrl *BandwidthMonitor) isDeclinedUnsafe(q Query) (bool, uint64) {
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

func (ctrl *BandwidthMonitor) newQueryFromReqResp(req interface{}, resp interface{}) Query {
	var reqSize, respSize int
	var reqContent string
	qtype := QueryTypeUnknown
	switch _resp := resp.(type) {
	case *pb.RangeResponse:
		_req, ok := req.(*pb.RangeRequest)
		if ok {
			reqSize = _req.Size()
			reqContent = _req.String()
			qtype = QueryTypeRange
		}
		if _resp != nil && _resp.Count != 0 {
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
	q := ctrl.newQueryFromReqResp(req, resp)
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	ctrl.estRespSize.UpdateString(q.qid, q.qsize)
	if ctrl.auditOn && q.qtype != QueryTypeUnknown {
		ctrl.server.Cfg.Logger.Warn("qmon audit.", zap.String("qid", q.qid), zap.Uint64("qsize", q.qsize))
	}
}

func (ctrl *BandwidthMonitor) AdmitReq(req interface{}) bool {
	q := ctrl.newQueryFromReq(req)
	declined, qsize := ctrl.isDeclined(q)
	if qsize > SmallReqThreshold && declined {
		ctrl.server.Cfg.Logger.Warn("qmon throttling request.", zap.String("qid", q.qid), zap.Uint64("respsize", qsize))
		err := ctrl.throttle.WaitN(context.TODO(), int(qsize))
		if err != nil {
			ctrl.server.Cfg.Logger.Warn("qmon throttle rejecting request.", zap.Error(err), zap.String("qid", q.qid))
			return false
		}
	}
	return true
}

func (ctrl *BandwidthMonitor) isDeclined(q Query) (bool, uint64) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	return ctrl.isDeclinedUnsafe(q)
}

func (ctrl *BandwidthMonitor) Stop() {
}

//TODO windows
func getCurrentRssBytes(logger *zap.Logger) uint64 {
	pid := os.Getpid()
	statm := fmt.Sprintf("/proc/%d/statm", pid)
	buf, err := ioutil.ReadFile(statm)
	if err != nil {
		logger.Error("qmon failed to read statm file", zap.String("statm file", statm), zap.Error(err))
		return 0
	}

	fields := strings.Split(string(buf), " ")
	if len(fields) < 2 {
		logger.Error("qmon failed to parse statm file", zap.String("statm file", statm), zap.String("buff", string(buf)))
		return 0
	}

	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		logger.Error("qmon cannot convert rss to int", zap.String("statm file", statm), zap.Error(err))
		return 0
	}

	return rss * uint64(os.Getpagesize())
}

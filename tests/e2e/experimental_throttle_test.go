// Copyright 2016 The etcd Authors
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

package e2e

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

const ENV_TEST_QMON = "ETCD_TEST_EXP_QMON"
const ENV_TEST_QMON_VALUE = "yes"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestCtlV3RangeReqStorm(t *testing.T) {
	if os.Getenv(ENV_TEST_QMON) != ENV_TEST_QMON_VALUE {
		return
	}

	cfg := e2e.EtcdProcessClusterConfig{
		ClusterSize:             1,
		InitialToken:            "new",
		EnableBandwidthThrottle: true,
	}
	testCtl(t, rangeReqTest, withCfg(cfg), withDialTimeout(5*time.Minute))
}

func rangeReq(cx ctlCtx, kvs []kv) {

	tests := []struct {
		args []string

		wkv []kv
	}{
		{[]string{"key", "--prefix"}, kvs},
	}

	if err := ctlV3Get(cx, tests[0].args, tests[0].wkv...); err != nil {
		if cx.dialTimeout > 0 && !isGRPCTimedout(err) && !isThrottleError(err) {
			cx.t.Errorf("getTest ctlV3Get error (%v)", err)
		}
	}
	fmt.Printf("worker done\n")
}

func isThrottleError(err error) bool {
	e := strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "etcdserver: too many requests")
	if e {
		fmt.Printf("Throttle error seen. e:%s\n", err.Error())
	}
	return e
}

func rangeReqTest(cx ctlCtx) {

	var kvs []kv
	kvs = make([]kv, 10)
	quit := make(chan bool)

	go checkMem(quit)

	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key%d", i)
		v := randStringBytes(30 * 1024)
		if err := ctlV3Put(cx, k, v, ""); err != nil {
			cx.t.Fatalf("getTest #%d: ctlV3Put error (%v)", i, err)
		}
		kvs = append(kvs, kv{k, v})
	}

	var wg sync.WaitGroup
	for j := 0; j < 100; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rangeReq(cx, kvs)
		}()
	}

	wg.Wait()
	quit <- true
}

func checkMem(quit chan bool) {
	for {
		select {
		case <-quit:
			return
		default:
			rss := getCurrentRssBytes()
			fmt.Printf("rss:%d\n", rss)
			var oneGig uint64
			oneGig = 1 * 1024 * 1024 * 1024
			if rss > oneGig {
				s := fmt.Sprintf("rss:%d too high\n", rss)
				panic(s)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func getPidOfEtcd() string {
	cmd := exec.Command("pidof", "etcd")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return ""
	}
	pid := strings.TrimSuffix(out.String(), "\n")
	return pid
}

func getCurrentRssBytes() uint64 {
	pid := getPidOfEtcd()
	statm := fmt.Sprintf("/proc/%s/statm", pid)
	buf, err := ioutil.ReadFile(statm)
	if err != nil {
		return 0
	}

	fields := strings.Split(string(buf), " ")
	if len(fields) < 2 {
		return 0
	}

	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}

	return rss * uint64(os.Getpagesize())
}

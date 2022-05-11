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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func TestCtlV3RangeReqStorm(t *testing.T) {
	cfg := e2e.EtcdProcessClusterConfig{
		ClusterSize:           1,
		InitialToken:          "new",
		EnableBandwidthThrottle: true,
	}
	cx := withCfg(cfg)
	cx = withDialTimeout(5 * time.Minute)
	testCtl(t, rangeReqTest, cx)
}

func rangeReq(cx ctlCtx, kvs []kv) {

	tests := []struct {
		args []string

		wkv []kv
	}{
		{[]string{"", "--prefix"}, kvs},
	}

	if err := ctlV3Get(cx, tests[0].args, tests[0].wkv...); err != nil {
		if cx.dialTimeout > 0 && !isGRPCTimedout(err) && !isThrottleError(err) {
			cx.t.Errorf("getTest ctlV3Get error (%v)", err)
		}
	}
}

func isThrottleError(err error) bool {
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func rangeReqTest(cx ctlCtx) {

	var kvs []kv
	kvs = make([]kv, 10)


	for i := 0; i<1000; i++ {
		k := fmt.Sprintf("key%d", i)
		fmt.Printf("Put key %s\n", k)
		v := randStringBytes(50 * 1024)
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
		} ()
	}

	wg.Wait()
}


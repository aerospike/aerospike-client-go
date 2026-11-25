// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike_test

import (
	"net"
	"strconv"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Connection salvage after a timeout", func() {

	const followUps = 6

	var ns = *namespace
	var set = ""
	var proxy *pausableNodeProxy
	var pclient *as.Client
	var keys []*as.Key

	readPolicy := func(timeoutDelay time.Duration) *as.BasePolicy {
		p := as.NewPolicy()
		p.TotalTimeout = 5 * time.Second
		p.SocketTimeout = 2 * time.Second
		p.TimeoutDelay = timeoutDelay
		p.MaxRetries = 2
		p.ReadModeSC = client.DefaultPolicy.ReadModeSC
		return p
	}

	timeoutPolicy := func(timeoutDelay time.Duration, maxRetries int) *as.BasePolicy {
		p := as.NewPolicy()
		p.TotalTimeout = 300 * time.Millisecond
		p.SocketTimeout = 100 * time.Millisecond
		p.TimeoutDelay = timeoutDelay
		p.MaxRetries = maxRetries
		p.ReadModeSC = client.DefaultPolicy.ReadModeSC
		return p
	}

	recovered := func() float64 {
		stats, err := pclient.Stats()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return stats["cluster-aggregated-stats"].(map[string]any)["connections-recovered"].(float64)
	}

	isClosed := func(ch <-chan struct{}) func() bool {
		return func() bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		}
	}

	expectOwnRecords := func(timeoutDelay time.Duration) {
		for i := 1; i <= followUps; i++ {
			rec, err := pclient.Get(readPolicy(timeoutDelay), keys[i])
			if err != nil {
				continue
			}
			gm.Expect(rec.Bins["idx"]).To(gm.Equal(i), "read %d returned the record of another command", i)
		}
	}

	timeOutRead := func(policy *as.BasePolicy) {
		_, err := pclient.Get(policy, keys[0])
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.TIMEOUT)).To(gm.BeTrue(), err.Error())
		gm.Eventually(proxy.responsePaused.Load).Should(gm.BeTrue())
	}

	gg.BeforeEach(func() {
		seed := dbHosts[0]
		proxy = newPausableNodeProxy(net.JoinHostPort(seed.Name, strconv.Itoa(seed.Port)))

		cp := *clientPolicy
		// one slot for the tend connection, one for commands
		cp.ConnectionQueueSize = 2
		cp.MinConnectionsPerNode = 0
		cp.FailIfNotConnected = true
		// peers advertise addresses the test host can't reach, so seed the other nodes directly
		seeds := append([]*as.Host{proxy.host(seed.TLSName)}, dbHosts[1:]...)
		var err error
		pclient, err = as.NewClientWithPolicyAndHost(&cp, seeds...)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		pclient.EnableMetrics(nil)
		gm.Expect(pclient.GetNodes()).To(gm.HaveLen(len(client.GetNodes())))

		var proxied *as.Node
		for _, n := range pclient.GetNodes() {
			if n.GetHost().Port == proxy.port() {
				proxied = n
			}
		}
		gm.Expect(proxied).ToNot(gm.BeNil())

		keys = keys[:0]
		for i := 0; len(keys) <= followUps; i++ {
			key, err := as.NewKey(ns, set, randString(20))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			ptn, err := as.PartitionForRead(pclient.Cluster(), readPolicy(0), key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			node, err := ptn.GetNodeRead(pclient.Cluster())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			if node.GetName() != proxied.GetName() {
				continue
			}
			gm.Expect(client.PutBins(nil, key, as.NewBin("idx", len(keys)), as.NewBin("pad", randString(512)))).To(gm.Succeed())
			keys = append(keys, key)
		}

		rec, err := pclient.Get(readPolicy(0), keys[0])
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["idx"]).To(gm.Equal(0))
	})

	gg.AfterEach(func() {
		pclient.Close()
		proxy.close()
	})

	gg.It("must close, not pool, a connection whose read timed out before the response header arrived", func() {
		before := recovered()

		closed := proxy.pauseNextResponse(0)
		timeOutRead(timeoutPolicy(2*time.Second, 0))
		proxy.resumeResponse()
		time.Sleep(300 * time.Millisecond)

		expectOwnRecords(2 * time.Second)

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})

	gg.It("must salvage a connection when the outstanding response is fully drained", func() {
		before := recovered()

		closed := proxy.pauseNextResponse(8)
		timeOutRead(timeoutPolicy(3*time.Second, 1))
		proxy.resumeResponse()

		gm.Eventually(recovered, 2*time.Second).Should(gm.Equal(before + 1))
		gm.Expect(isClosed(closed)()).To(gm.BeFalse())

		expectOwnRecords(3 * time.Second)
		gm.Expect(isClosed(closed)()).To(gm.BeFalse())
	})

	gg.It("must count a salvaged connection when metrics are disabled", func() {
		pclient.DisableMetrics()
		before := recovered()

		closed := proxy.pauseNextResponse(8)
		timeOutRead(timeoutPolicy(3*time.Second, 1))
		proxy.resumeResponse()

		gm.Eventually(recovered, 2*time.Second).Should(gm.Equal(before + 1))
		gm.Expect(isClosed(closed)()).To(gm.BeFalse())
	})

	gg.It("must close the connection when the drain deadline expires", func() {
		before := recovered()

		closed := proxy.pauseNextResponse(8)
		timeOutRead(timeoutPolicy(300*time.Millisecond, 0))

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		proxy.resumeResponse()

		expectOwnRecords(300 * time.Millisecond)
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})

	gg.It("must close the connection without salvaging when TimeoutDelay is 0", func() {
		before := recovered()

		closed := proxy.pauseNextResponse(0)
		timeOutRead(timeoutPolicy(0, 0))

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		proxy.resumeResponse()

		expectOwnRecords(0)
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})
})

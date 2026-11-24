// Copyright 2014-2026 Aerospike, Inc.
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

package aerospike

import (
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5833: errors accumulate over an ErrorRateWindow and validateErrorCount
// is read-only. Only the per-window reset in tend adjusts the threshold and
// clears the count. No server needed.
var _ = gg.Describe("Node circuit breaker window (CLIENT-5833)", func() {

	newBreakerNode := func(maxErrorRate int) *Node {
		policy := NewClientPolicy()
		policy.MaxErrorRate = maxErrorRate
		clientPolicy := &atomic.Pointer[ClientPolicy]{}
		clientPolicy.Store(policy)

		cluster := &Cluster{clientPolicy: clientPolicy}
		cluster.maxErrorCount.Set(maxErrorRate)

		node := &Node{cluster: cluster}
		node.maxErrorCount.Set(maxErrorRate)
		node.active.Set(true)
		return node
	}

	// simulate mimics a node where every allowed attempt fails with one counted
	// error, e.g. a rejected TLS handshake. resetEvery > 0 closes the window
	// every resetEvery attempts, as tend does.
	simulate := func(node *Node, attempts, resetEvery int) (errors, blocked int) {
		for i := 1; i <= attempts; i++ {
			if err := node.validateErrorCount(); err != nil {
				gm.Expect(err.Matches(types.MAX_ERROR_RATE)).To(gm.BeTrue())
				blocked++
			} else {
				node.incrErrorCount()
				errors++
			}
			if resetEvery > 0 && i%resetEvery == 0 {
				node.resetErrorRate(node.cluster.clientPolicy.Load().MaxErrorRate)
			}
		}
		return errors, blocked
	}

	gg.It("opens after MaxErrorRate+1 errors and stays open for the window", func() {
		node := newBreakerNode(5)

		errors, blocked := simulate(node, 16300, 0)
		gm.Expect(errors).To(gm.Equal(6))
		gm.Expect(blocked).To(gm.Equal(16294))
		gm.Expect(node.stats.CircuitBreakerHits.Get()).To(gm.Equal(blocked))
		gm.Expect(node.errorCount.Get()).To(gm.Equal(6))
		gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(5))
	})

	gg.It("allows MaxErrorRate+1 errors with MaxErrorRate=100", func() {
		node := newBreakerNode(100)

		errors, blocked := simulate(node, 16300, 0)
		gm.Expect(errors).To(gm.Equal(101))
		gm.Expect(blocked).To(gm.Equal(16300 - 101))
		gm.Expect(node.stats.CircuitBreakerHits.Get()).To(gm.Equal(blocked))
	})

	gg.It("allows threshold+1 errors per window and halves the threshold after each breached window", func() {
		node := newBreakerNode(100)

		// 17 windows: thresholds 100, 50, 25, 12, 6, 3, 1, 1, ...
		errors, blocked := simulate(node, 16300, 1000)
		gm.Expect(errors).To(gm.Equal(101 + 51 + 26 + 13 + 7 + 4 + 11*2))
		gm.Expect(blocked).To(gm.Equal(16300 - errors))
		gm.Expect(node.stats.CircuitBreakerHits.Get()).To(gm.Equal(blocked))
		gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(1))
	})

	gg.It("does not count errors or trip when MaxErrorRate is 0", func() {
		node := newBreakerNode(0)

		errors, blocked := simulate(node, 1000, 0)
		gm.Expect(errors).To(gm.Equal(1000))
		gm.Expect(blocked).To(gm.Equal(0))
		gm.Expect(node.errorCount.Get()).To(gm.Equal(0))
	})

	gg.It("does not trip a breached node once MaxErrorRate is disabled", func() {
		node := newBreakerNode(5)
		simulate(node, 10, 0)
		gm.Expect(node.validateErrorCount()).ToNot(gm.BeNil())

		policy := *node.cluster.clientPolicy.Load()
		policy.MaxErrorRate = 0
		node.cluster.clientPolicy.Store(&policy)
		gm.Expect(node.validateErrorCount()).To(gm.BeNil())
	})

	gg.Context("window reset", func() {
		gg.It("halves the threshold on a breached window, with a floor of 1", func() {
			node := newBreakerNode(100)

			for _, want := range []int{50, 25, 12, 6, 3, 1, 1} {
				for node.validateErrorCount() == nil {
					node.incrErrorCount()
				}
				node.resetErrorRate(100)
				gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(want))
				gm.Expect(node.errorCount.Get()).To(gm.Equal(0))
			}
		})

		gg.It("doubles the threshold on a clean window, capped at MaxErrorRate", func() {
			node := newBreakerNode(100)
			node.maxErrorCount.Set(1)

			for _, want := range []int{2, 4, 8, 16, 32, 64, 100, 100} {
				node.incrErrorCount()
				gm.Expect(node.validateErrorCount()).To(gm.BeNil())
				node.resetErrorRate(100)
				gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(want))
				gm.Expect(node.errorCount.Get()).To(gm.Equal(0))
			}
		})

		gg.It("treats a window at exactly the threshold as clean", func() {
			node := newBreakerNode(100)
			node.maxErrorCount.Set(10)

			for i := 0; i < 10; i++ {
				node.incrErrorCount()
			}
			gm.Expect(node.validateErrorCount()).To(gm.BeNil())
			node.resetErrorRate(100)
			gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(20))
		})

		gg.It("blocks commands until the window reset and allows them after it", func() {
			node := newBreakerNode(5)

			for i := 0; i < 6; i++ {
				node.incrErrorCount()
			}
			for i := 0; i < 100; i++ {
				gm.Expect(node.validateErrorCount()).ToNot(gm.BeNil())
			}
			gm.Expect(node.errorCount.Get()).To(gm.Equal(6))
			gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(5))
			gm.Expect(node.stats.CircuitBreakerHits.Get()).To(gm.Equal(100))

			node.resetErrorRate(5)
			gm.Expect(node.validateErrorCount()).To(gm.BeNil())

			// next window is gated on the halved threshold
			gm.Expect(node.maxErrorCount.Get()).To(gm.Equal(2))
			for i := 0; i < 2; i++ {
				node.incrErrorCount()
				gm.Expect(node.validateErrorCount()).To(gm.BeNil())
			}
			node.incrErrorCount()
			gm.Expect(node.validateErrorCount()).ToNot(gm.BeNil())
		})
	})

	gg.It("counts a server DEVICE_OVERLOAD result as a node error", func() {
		node := newBreakerNode(100)
		node.connections = *newConnectionHeap(0, 1)

		cmd := newBufferProbeCommand(gg.GinkgoT(), "small", false)
		cmd.fakeConn.conn = &bytesConn{buf: markRollForwardWire(byte(types.DEVICE_OVERLOAD))}
		cmd.fakeConn.limitReader.R = cmd.fakeConn.conn
		cmd.node = node

		err := cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1)
		gm.Expect(err).ToNot(gm.BeNil())
		gm.Expect(err.Matches(types.DEVICE_OVERLOAD)).To(gm.BeTrue())
		gm.Expect(node.errorCount.Get()).To(gm.Equal(1))
	})
})

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
	"net"
	"sync/atomic"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Zero-node re-seed", gg.Label("discovery"), func() {

	deadSeed := NewHost("127.0.0.1", 1)

	policy := func(provider SeedCandidateProvider, mode DiscoveryExecutionMode, refreshInterval time.Duration) *ClientPolicy {
		clientPolicy := NewClientPolicy()
		clientPolicy.FailIfNotConnected = false
		clientPolicy.Timeout = 100 * time.Millisecond
		clientPolicy.TendInterval = time.Second
		clientPolicy.DiscoveryRefreshInterval = refreshInterval
		clientPolicy.DiscoveryExecutionMode = mode
		clientPolicy.SeedProvider = provider
		return clientPolicy
	}

	newZeroNodeClient := func(clientPolicy *ClientPolicy, seed *Host) *Client {
		client, _ := NewClientWithPolicyAndHost(clientPolicy, seed)
		gm.Expect(client).NotTo(gm.BeNil())
		gg.DeferCleanup(client.Close)
		gm.Expect(client.GetNodes()).To(gm.HaveLen(0))
		return client
	}

	listeningSeed := func() (*Host, *atomic.Int64) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.DeferCleanup(listener.Close)

		var accepts atomic.Int64
		go func() {
			for {
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				accepts.Add(1)
				conn.Close()
			}
		}()

		return NewHost("127.0.0.1", listener.Addr().(*net.TCPAddr).Port), &accepts
	}

	gg.It("refreshes a tend-mode provider again while no node is reachable", func() {
		provider := &scriptedSeedProvider{
			results:  [][]*Host{{deadSeed}},
			periodic: true,
			deadline: true,
		}
		client := newZeroNodeClient(policy(provider, DiscoveryExecutionTend, time.Second), deadSeed)

		for range 5 {
			_ = client.cluster.tend()
		}

		gm.Expect(provider.calls.Load()).To(gm.BeNumerically(">=", 3),
			"tend() returned before tendTick, so a tend-mode provider never refreshes while the cluster is empty")
		gm.Expect(client.GetNodes()).To(gm.HaveLen(0))
	})

	gg.It("refreshes a thread-mode provider again while no node is reachable", func() {
		provider := &scriptedSeedProvider{
			results:  [][]*Host{{deadSeed}},
			periodic: true,
		}
		client := newZeroNodeClient(policy(provider, DiscoveryExecutionThread, time.Hour), deadSeed)

		for range 5 {
			_ = client.cluster.tend()
		}

		gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.BeNumerically(">=", 3))
		gm.Expect(client.GetNodes()).To(gm.HaveLen(0))
	})

	gg.It("re-seeds from the snapshot in hand without waiting for the refresh interval", func() {
		seed, accepts := listeningSeed()
		provider := &scriptedSeedProvider{
			results:  [][]*Host{{seed}},
			periodic: true,
			deadline: true,
		}
		client := newZeroNodeClient(policy(provider, DiscoveryExecutionTend, time.Minute), seed)
		callsAfterStart := provider.calls.Load()

		for range 3 {
			_ = client.cluster.tend()
		}

		gm.Expect(accepts.Load()).To(gm.BeNumerically(">=", 3))
		gm.Expect(provider.calls.Load()).To(gm.Equal(callsAfterStart))
		gm.Expect(client.GetNodes()).To(gm.HaveLen(0))
	})

	gg.It("re-seeds from the snapshot in hand in thread mode without waiting for the refresh interval", func() {
		seed, accepts := listeningSeed()
		provider := &scriptedSeedProvider{
			results:  [][]*Host{{seed}},
			periodic: true,
		}
		client := newZeroNodeClient(policy(provider, DiscoveryExecutionThread, time.Hour), seed)

		for range 3 {
			_ = client.cluster.tend()
		}

		gm.Expect(accepts.Load()).To(gm.BeNumerically(">=", 3))
		gm.Expect(client.GetNodes()).To(gm.HaveLen(0))
	})
})

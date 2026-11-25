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

package aerospike_test

import (
	"net"
	"strconv"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Force one failed attempt then a successful retry via holdProxy
// (same inject path as connection-salvage tests). Expects per-attempt
// errors, one command.count, retries, and latency only on success.
var _ = gg.Describe("Metrics multi-attempt retry", func() {

	var (
		ns      = *namespace
		proxy   *holdProxy
		pclient *as.Client
		key     *as.Key
	)

	gg.BeforeEach(func() {
		seed := dbHosts[0]
		proxy = newHoldProxy(net.JoinHostPort(seed.Name, strconv.Itoa(seed.Port)))

		cp := *clientPolicy
		cp.ConnectionQueueSize = 2
		cp.MinConnectionsPerNode = 0
		cp.FailIfNotConnected = true
		seeds := append([]*as.Host{proxy.host(seed.TLSName)}, dbHosts[1:]...)
		var err error
		pclient, err = as.NewClientWithPolicyAndHost(&cp, seeds...)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		mp := as.DefaultMetricsPolicy()
		mp.Exporter = as.MetricsExporterNone
		mp.LatencyUnit = as.LatencyMicroseconds
		pclient.EnableMetrics(mp)

		var proxied *as.Node
		for _, n := range pclient.GetNodes() {
			if n.GetHost().Port == proxy.port() {
				proxied = n
			}
		}
		gm.Expect(proxied).ToNot(gm.BeNil())

		readPol := as.NewPolicy()
		readPol.ReadModeSC = client.DefaultPolicy.ReadModeSC
		for {
			k, err := as.NewKey(ns, "metricsSet", randString(20))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			ptn, err := as.PartitionForRead(pclient.Cluster(), readPol, k)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			node, err := ptn.GetNodeRead(pclient.Cluster())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			if node.GetName() != proxied.GetName() {
				continue
			}
			gm.Expect(client.Put(nil, k, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())
			key = k
			break
		}
	})

	gg.AfterEach(func() {
		if pclient != nil {
			pclient.Close()
		}
		if proxy != nil {
			proxy.close()
		}
	})

	gg.It("records per-attempt timeout, one command.count, retry, and success latency", func() {
		before := pclient.MetricsSnapshot()

		_ = proxy.holdNext(0)
		pol := as.NewPolicy()
		pol.SocketTimeout = 100 * time.Millisecond
		pol.TotalTimeout = 5 * time.Second
		pol.MaxRetries = 2
		pol.TimeoutDelay = 2 * time.Second
		pol.ReadModeSC = client.DefaultPolicy.ReadModeSC

		rec, err := pclient.Get(pol, key)
		proxy.releaseHeld()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec).ToNot(gm.BeNil())

		after := pclient.MetricsSnapshot()
		gm.Expect(after.CommandCount - before.CommandCount).To(gm.Equal(int64(1)))
		gm.Expect(after.CommandRetries - before.CommandRetries).To(gm.BeNumerically(">=", 1))
		gm.Expect(clientTimeouts(after) - clientTimeouts(before)).To(gm.BeNumerically(">=", 1))
		gm.Expect(latencyCount(after, as.LatencyRead) - latencyCount(before, as.LatencyRead)).
			To(gm.BeNumerically(">=", 1), "latency only after successful attempt")
	})
})

func clientTimeouts(snap *as.MetricsSnapshot) int64 {
	if snap == nil {
		return 0
	}
	var n int64
	for _, node := range snap.Nodes {
		for _, ns := range node.Namespaces {
			n += ns.ClientTimeouts
		}
	}
	return n
}

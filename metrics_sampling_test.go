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
	"fmt"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Metrics sampling integration", func() {

	gg.BeforeEach(func() {
		// Suite enables metrics with defaults; reset to a known full-rate policy.
		p := as.DefaultMetricsPolicy()
		p.UsageEnabled = true
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)
	})

	gg.AfterEach(func() {
		client.EnableMetrics(as.DefaultMetricsPolicy())
	})

	gg.It("counts one command per single-key Get", func() {
		before := client.MetricsSnapshot()
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-get-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.Put(nil, key, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())
		_, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		after := client.MetricsSnapshot()
		gm.Expect(after.CommandCount - before.CommandCount).To(gm.BeNumerically(">=", 2)) // put + get
		var readCount uint64
		for _, lat := range after.Latencies {
			if lat.Type == as.LatencyRead {
				readCount = lat.Count
				break
			}
		}
		var beforeRead uint64
		for _, lat := range before.Latencies {
			if lat.Type == as.LatencyRead {
				beforeRead = lat.Count
				break
			}
		}
		gm.Expect(readCount - beforeRead).To(gm.BeNumerically(">=", 1))
	})

	gg.It("one-key batch records cluster.latency.batch once, not point read", func() {
		before := client.MetricsSnapshot()
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-batch1-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.Put(nil, key, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())

		beforeBatch := client.MetricsSnapshot()
		recs, err := client.BatchGet(nil, []*as.Key{key})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(recs).To(gm.HaveLen(1))
		after := client.MetricsSnapshot()

		gm.Expect(after.CommandCount - beforeBatch.CommandCount).To(gm.Equal(int64(1)))

		deltaBatch := latencyCount(after, as.LatencyBatch) - latencyCount(beforeBatch, as.LatencyBatch)
		deltaRead := latencyCount(after, as.LatencyRead) - latencyCount(beforeBatch, as.LatencyRead)
		gm.Expect(deltaBatch).To(gm.Equal(uint64(1)))
		gm.Expect(deltaRead).To(gm.Equal(uint64(0)), "executeSingle must not attribute as point read")

		_ = before // silence if unused in some builds
	})

	gg.It("sampler threshold 0 records no command.count on Get", func() {
		p := as.DefaultMetricsPolicy()
		p.SetSamplerNever()
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)

		before := client.MetricsSnapshot()
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-nosamp-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, _ = client.Get(nil, key) // miss is fine
		after := client.MetricsSnapshot()
		gm.Expect(after.CommandCount).To(gm.Equal(before.CommandCount))
	})

	gg.It("concurrent Enable/Disable does not hang", func() {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				p := as.DefaultMetricsPolicy()
				p.Exporter = as.MetricsExporterNone
				if i%2 == 0 {
					client.EnableMetrics(p)
				} else {
					client.DisableMetrics()
				}
			}(i)
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			gg.Fail("Enable/Disable deadlock")
		}
		client.EnableMetrics(as.DefaultMetricsPolicy())
	})

	gg.It("MetricsSnapshot Nodes[0] not corrupted by ClusterAggregated", func() {
		snap := client.MetricsSnapshot()
		gm.Expect(snap.Nodes).NotTo(gm.BeEmpty())
		if snap.ClusterAggregated == nil || len(snap.Nodes) < 2 {
			gg.Skip("need ≥2 nodes to exercise aggregate aliasing")
		}
		n0 := snap.Nodes[0]
		agg := snap.ClusterAggregated
		// Per-node identity fields must remain set on Nodes[0].
		gm.Expect(n0.NodeName).NotTo(gm.BeEmpty())
		gm.Expect(agg.NodeName).To(gm.BeEmpty())
		// If aliasing existed, Nodes[0] namespace counters would equal the aggregate.
		if len(n0.Namespaces) > 0 && len(agg.Namespaces) > 0 {
			gm.Expect(n0.Namespaces[0].BytesIn).To(gm.BeNumerically("<=", agg.Namespaces[0].BytesIn))
		}
	})

	gg.It("batch.partial_failure stays 0 at sampler threshold 0", func() {
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-pf-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.Put(nil, key, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())

		p := as.DefaultMetricsPolicy()
		p.SetSamplerNever()
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)

		before := client.MetricsSnapshot()
		bwp := as.NewBatchWritePolicy()
		bwp.RecordExistsAction = as.CREATE_ONLY
		bw := as.NewBatchWrite(bwp, key, as.PutOp(as.NewBin("a", 2)))
		policy := as.NewBatchPolicy()
		policy.RespondAllKeys = true
		_ = client.BatchOperate(policy, []as.BatchRecordIfc{bw})
		after := client.MetricsSnapshot()
		gm.Expect(after.BatchPartialFailure).To(gm.Equal(before.BatchPartialFailure))
	})

	gg.It("CpuPercent is not stuck near 100 under idle", func() {
		p := as.DefaultMetricsPolicy()
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)
		_ = client.MetricsSnapshot() // prime CPU sample
		time.Sleep(200 * time.Millisecond)
		snap := client.MetricsSnapshot()
		gm.Expect(snap.CpuPercent).To(gm.BeNumerically("<", 95),
			"idle process CPU should not look like total-including-idle (~100%%)")
	})
})

func latencyCount(snap *as.MetricsSnapshot, lt as.LatencyType) uint64 {
	if snap == nil {
		return 0
	}
	for _, lat := range snap.Latencies {
		if lat.Type == lt {
			return lat.Count
		}
	}
	return 0
}

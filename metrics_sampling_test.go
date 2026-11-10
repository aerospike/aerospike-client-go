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
	"os"
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

	gg.It("CPUPercent is not stuck near 100 under idle", func() {
		p := as.DefaultMetricsPolicy()
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)
		_ = client.MetricsSnapshot() // prime CPU sample
		time.Sleep(200 * time.Millisecond)
		snap := client.MetricsSnapshot()
		gm.Expect(snap.CPUPercent).To(gm.BeNumerically("<", 95),
			"idle process CPU should not look like total-including-idle (~100%%)")
	})

	gg.It("FILE exporter writes under ReportDir while commands run", func() {
		dir, err := os.MkdirTemp("", "as-metrics-file-*")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer os.RemoveAll(dir)

		p := as.DefaultMetricsPolicy()
		p.Interval = 50 * time.Millisecond
		p.Exporter = as.MetricsExporterFile
		p.ReportDir = dir
		client.EnableMetrics(p)

		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-file-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.Put(nil, key, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())

		gm.Eventually(func() int {
			ents, e := os.ReadDir(dir)
			if e != nil {
				return 0
			}
			return len(ents)
		}, "2s", "50ms").Should(gm.BeNumerically(">=", 1))
	})

	gg.It("DisableMetrics freezes command.count on subsequent Gets", func() {
		client.DisableMetrics()
		before := client.MetricsSnapshot()
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-off-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, _ = client.Get(nil, key)
		after := client.MetricsSnapshot()
		gm.Expect(after.CommandCount).To(gm.Equal(before.CommandCount))
		gm.Expect(after.MetricsEnabled).To(gm.BeFalse())
	})

	gg.It("latency sum is present when read count advances", func() {
		// Millisecond unit truncates sub-ms RTTs to 0 (count advances, sum stays 0).
		// Use microseconds so latency sum is observable on a fast local Get.
		p := as.DefaultMetricsPolicy()
		p.LatencyUnit = as.LatencyMicroseconds
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)

		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-sum-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.Put(nil, key, as.BinMap{"a": 1})).ToNot(gm.HaveOccurred())
		before := client.MetricsSnapshot()
		_, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		after := client.MetricsSnapshot()

		delta := latencyCount(after, as.LatencyRead) - latencyCount(before, as.LatencyRead)
		gm.Expect(delta).To(gm.BeNumerically(">=", 1))
		gm.Expect(latencySum(after, as.LatencyRead)).To(gm.BeNumerically(">", latencySum(before, as.LatencyRead)))
	})

	gg.It("usage feature.api.blocking advances on Get when usage enabled", func() {
		p := as.DefaultMetricsPolicy()
		p.UsageEnabled = true
		p.Exporter = as.MetricsExporterNone
		client.EnableMetrics(p)

		before := client.MetricsSnapshot()
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-usage-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, _ = client.Get(nil, key)
		after := client.MetricsSnapshot()
		gm.Expect(after.Usage).NotTo(gm.BeNil())
		gm.Expect(before.Usage).NotTo(gm.BeNil())
		gm.Expect(after.Usage.Blocking - before.Usage.Blocking).To(gm.BeNumerically(">=", 1))
		gm.Expect(after.Usage.ShapePoint - before.Usage.ShapePoint).To(gm.BeNumerically(">=", 1))
	})

	// Sampler behavior remains visible across successive short-interval exports.
	gg.It("FILE exports stay flat under sampler never then advance at full rate", func() {
		dir, err := os.MkdirTemp("", "as-metrics-f4-*")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer os.RemoveAll(dir)

		never := as.DefaultMetricsPolicy()
		never.SetSamplerNever()
		never.Interval = 50 * time.Millisecond
		never.Exporter = as.MetricsExporterFile
		never.ReportDir = dir
		client.EnableMetrics(never)

		gm.Eventually(func() int {
			ents, e := os.ReadDir(dir)
			if e != nil {
				return 0
			}
			return len(ents)
		}, "2s", "50ms").Should(gm.BeNumerically(">=", 1))

		before := client.MetricsSnapshot().CommandCount
		key, err := as.NewKey(*namespace, "metricsSet", fmt.Sprintf("m-f4-%d", time.Now().UnixNano()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for i := 0; i < 5; i++ {
			_, _ = client.Get(nil, key)
		}
		time.Sleep(150 * time.Millisecond)
		gm.Expect(client.MetricsSnapshot().CommandCount).To(gm.Equal(before))

		full := as.DefaultMetricsPolicy()
		full.Interval = 50 * time.Millisecond
		full.Exporter = as.MetricsExporterFile
		full.ReportDir = dir
		client.EnableMetrics(full)
		mid := client.MetricsSnapshot().CommandCount
		_, _ = client.Get(nil, key)
		_, _ = client.Get(nil, key)
		time.Sleep(150 * time.Millisecond)
		gm.Expect(client.MetricsSnapshot().CommandCount).To(gm.BeNumerically(">=", mid+2))
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

func latencySum(snap *as.MetricsSnapshot, lt as.LatencyType) float64 {
	if snap == nil {
		return 0
	}
	for _, lat := range snap.Latencies {
		if lat.Type == lt {
			return lat.Sum
		}
	}
	return 0
}

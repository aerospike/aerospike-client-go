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

import "testing"

// These benchmarks measure the per-command metric helpers
// applyTransactionErrorMetrics / applyTransactionRetryMetrics /
// applyConnectionRecoveredMetrics. They were gated on Cluster.metricsEnabled
// by CLIENT-3968 and ungated again by CLIENT-5762, since Client.Stats()
// reports these counters regardless of EnableMetrics. They only run on error,
// retry and connection-recovery paths.
//
// The legacy* functions below are the pre-CLIENT-3968 unconditional
// increment; "after" calls the real implementations from command.go.
//
// The three helpers are structurally identical, so applyTransactionErrorMetrics
// is benchmarked as the representative; the retry / connection-recovered
// helpers behave the same.

func legacyApplyTransactionErrorMetrics(node *Node) {
	if node != nil {
		node.stats.TransactionErrorCount.GetAndIncrement()
	}
}

func legacyApplyTransactionRetryMetrics(node *Node) {
	if node != nil {
		node.stats.TransactionRetryCount.GetAndIncrement()
	}
}

func legacyApplyConnectionRecoveredMetrics(node *Node) {
	if node != nil {
		node.stats.ConnectionsRecovered.GetAndIncrement()
	}
}

// newBenchNode builds a minimal *Node whose cluster gate is set to the given
// state. Only the fields the helpers touch (cluster.metricsEnabled and
// stats.*Count) are populated.
func newBenchNode(metricsEnabled bool) *Node {
	n := &Node{
		cluster: &Cluster{},
		stats:   *newNodeStats(DefaultMetricsPolicy()),
	}
	n.cluster.metricsEnabled.Store(metricsEnabled)
	return n
}

// BenchmarkApplyTransactionMetrics measures the single-goroutine cost.
// Every path pays the mutex Lock/Unlock per call. No path allocates.
func BenchmarkApplyTransactionMetrics(b *testing.B) {
	b.Run("before/unconditional", func(b *testing.B) {
		node := newBenchNode(false) // legacy ignores the gate; always increments
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			legacyApplyTransactionErrorMetrics(node)
		}
	})

	b.Run("after/metricsOff", func(b *testing.B) {
		node := newBenchNode(false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			applyTransactionErrorMetrics(node)
		}
	})

	b.Run("after/metricsOn", func(b *testing.B) {
		node := newBenchNode(true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			applyTransactionErrorMetrics(node)
		}
	})
}

// BenchmarkApplyTransactionMetricsParallel is the contention case: the
// internal/atomic.Int is mutex-backed, so under many goroutines the increment
// serializes on a single Lock.
func BenchmarkApplyTransactionMetricsParallel(b *testing.B) {
	b.Run("before/unconditional", func(b *testing.B) {
		node := newBenchNode(false)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				legacyApplyTransactionErrorMetrics(node)
			}
		})
	})

	b.Run("after/metricsOff", func(b *testing.B) {
		node := newBenchNode(false)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				applyTransactionErrorMetrics(node)
			}
		})
	})

	b.Run("after/metricsOn", func(b *testing.B) {
		node := newBenchNode(true)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				applyTransactionErrorMetrics(node)
			}
		})
	})
}

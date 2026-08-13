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

// These benchmarks measure the per-command hot-path helpers
// applyTransactionErrorMetrics / applyTransactionRetryMetrics /
// applyConnectionRecoveredMetrics before and after gating them on
// Cluster.metricsEnabled.
//
// "before" is reproduced by the legacy* functions below, which increment the
// mutex-backed internal/atomic.Int unconditionally (only nil-checking the
// node) — exactly what command.go did prior to the gate. "after" calls the
// real, gated implementations from command.go.
//
// The three helpers are structurally identical, so applyTransactionErrorMetrics
// is benchmarked as the representative; the retry / connection-recovered
// helpers behave the same.

func legacyApplyTransactionErrorMetrics(node *Node) {
	if node != nil {
		node.stats.TransactionErrorCount.GetAndIncrement()
	}
}

// newBenchNode builds a minimal *Node whose cluster gate is set to the given
// state. Only the fields the helpers touch (cluster.metricsEnabled and
// stats.*Count) are populated.
func newBenchNode(metricsEnabled bool) *Node {
	n := &Node{
		cluster: &Cluster{},
		stats:   newNodeStats(DefaultMetricsPolicy()),
	}
	n.cluster.metricsEnabled.Store(metricsEnabled)
	return n
}

// BenchmarkApplyTransactionMetrics measures the single-goroutine cost.
// Expect: before/* and after/metricsOn pay the mutex Lock/Unlock per call;
// after/metricsOff short-circuits to an atomic-bool load + branch. No path
// allocates.
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

// BenchmarkApplyTransactionMetricsParallel is the contention case the gate
// actually addresses: the internal/atomic.Int is mutex-backed, so under many
// goroutines the unconditional increment serializes on a single Lock. With
// metrics off, the gated helper never reaches the increment and scales freely.
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

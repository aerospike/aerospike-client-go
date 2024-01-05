package aerospike

import (
	"time"

	iatomic "github.com/aerospike/aerospike-client-go/v6/internal/atomic"
)

type LatencyBuckets struct {
	buckets      []iatomic.Int
	latencyShift int
}

func newLatencyBuckets(latencyColumns int, latencyShift int) *LatencyBuckets {
	buckets := &LatencyBuckets{
		buckets:      make([]iatomic.Int, latencyColumns),
		latencyShift: latencyShift,
	}
	return buckets
}

func (nm *LatencyBuckets) add(elapsed time.Duration) {
	index := nm.getIndex(elapsed)
	nm.buckets[index].GetAndIncrement()
}

func (nm *LatencyBuckets) getIndex(elapsed time.Duration) int {
	elapsedMs := elapsed.Milliseconds()
	limit := 1
	lastBucket := len(nm.buckets) - 1
	for i := 0; i < lastBucket-1; i++ {
		if int(elapsedMs) <= limit {
			return i
		}
		limit <<= nm.latencyShift
	}

	return lastBucket
}

type NodeMetrics struct {
	latency []*LatencyBuckets
}

func newNodeMetrics(policy *MetricsPolicy) *NodeMetrics {
	max := LATENCY_NONE
	latency := make([]*LatencyBuckets, max)
	for i := 0; i < max; i++ {
		latency[i] = newLatencyBuckets(policy.latencyColumns, policy.latencyShift)
	}
	return &NodeMetrics{
		latency: latency,
	}
}

func (nm *NodeMetrics) addLatency(latencyType int, elapsed time.Duration) {
	nm.latency[latencyType].add(elapsed)
}

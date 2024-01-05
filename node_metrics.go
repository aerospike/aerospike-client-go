package aerospike

import (
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

type NodeMetrics struct {
	latency []LatencyBuckets
}

func newNodeMetrics(policy *MetricsPolicy) *NodeMetrics {
	return newLatencyBuckets(policy.latencyColumns, policy.latencyShift)
}

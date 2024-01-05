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

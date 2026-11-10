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

import "time"

// LatencyUnit is the unit used when recording shared latency histograms.
type LatencyUnit int

const (
	// LatencyMilliseconds is the default shared latency unit.
	LatencyMilliseconds LatencyUnit = iota
	// LatencyMicroseconds records shared latency in microseconds.
	LatencyMicroseconds
)

func (u LatencyUnit) String() string {
	if u == LatencyMicroseconds {
		return "microseconds"
	}
	return "milliseconds"
}

func parseLatencyUnit(s string) LatencyUnit {
	if s == "microseconds" || s == "us" || s == "µs" {
		return LatencyMicroseconds
	}
	return LatencyMilliseconds
}

// LatencyType is the shared MUST latency class (conn/read/write/batch/query).
// Fine-grained per-command histograms are language-optional and are not part
// of this enum.
type LatencyType int

const (
	LatencyConn LatencyType = iota
	LatencyRead
	LatencyWrite
	LatencyBatch
	LatencyQuery
	latencyTypeMax
)

func (t LatencyType) String() string {
	switch t {
	case LatencyConn:
		return "conn"
	case LatencyRead:
		return "read"
	case LatencyWrite:
		return "write"
	case LatencyBatch:
		return "batch"
	case LatencyQuery:
		return "query"
	default:
		return "unknown"
	}
}

func latencyTypeForCommand(ct commandType) LatencyType {
	switch ct {
	case ttGet, ttGetHeader, ttExists:
		return LatencyRead
	case ttPut, ttDelete, ttOperate, ttUDF:
		return LatencyWrite
	case ttBatchRead, ttBatchWrite:
		return LatencyBatch
	case ttQuery, ttScan:
		return LatencyQuery
	default:
		return LatencyWrite
	}
}

// MetricsSnapshot is the structured point-in-time object passed to exporters
// and returned by Client.MetricsSnapshot.
//
// Counters and histogram buckets are cumulative since the last off→on
// EnableMetrics edge (reset then) and are not reset on on-demand read or on
// export. When metrics are disabled, counters stay frozen at those values.
//
// Dual latency scopes: Latencies on MetricsSnapshot is
// cluster.latency.{read,write,batch,query}; NamespaceMetricsSnapshot.Latencies
// is namespace.latency.{read,write,batch,query}; NodeMetricsSnapshot.ConnLatency
// is node.latency.conn. Go also keeps fine-grained / per-command and phase
// histograms on the snapshot additively (language-optional).
type MetricsSnapshot struct {
	Timestamp time.Time `json:"timestamp"`

	MetricsEnabled            bool `json:"metrics_enabled"`
	OperationalMetricsEnabled bool `json:"operational_metrics_enabled"`
	UsageMetricsEnabled       bool `json:"usage_metrics_enabled"`

	ClusterName   string            `json:"cluster_name"`
	ClientType    string            `json:"client_type"`
	ClientVersion string            `json:"client_version"`
	AppID         string            `json:"app_id"`
	Labels        map[string]string `json:"labels,omitempty"`

	TotalNodes int `json:"total_nodes"`
	// RecoverQueueSize is sockets currently in TimeoutDelay salvage (in-flight
	// go salvageConn). Same gauge as Java recoverQueue size / Rust
	// connections_recovering — Go has no deque, only the live count.
	RecoverQueueSize int `json:"recover_queue_size"`
	InvalidNodeCount int `json:"invalid_node_count"`
	// DelayQueueTimeoutCount is omitted: this client has no async delay queue
	// (do not emit empty placeholders for async/event-loop fields).
	OpenConnections int64 `json:"open_connections"`

	CommandCount   int64 `json:"command_count"`
	CommandRetries int64 `json:"command_retries"`
	// RetryCount is a legacy API alias of CommandRetries.
	// The Go metrics file writes command_retries.
	RetryCount int64 `json:"retry_count"`
	// ExceededMaxRetries / ExceededTotalTimeout are Go legacy diagnostics
	// (not shared MUST).
	ExceededMaxRetries   int64   `json:"exceeded_max_retries"`
	ExceededTotalTimeout int64   `json:"exceeded_total_timeout"`
	CPUPercent           float64 `json:"cpu_percent"`
	MemoryBytes          int64   `json:"memory_bytes"`

	// Pre-Node only (seed / NodeValidator that never becomes a Node).
	// Pool/setup failures on an existing Node live on NodeMetricsSnapshot.
	// open.failure rolls up tls/auth in that home.
	ValidationOpenFailure         int64 `json:"validation_open_failure"`
	ValidationTlsHandshakeFailure int64 `json:"validation_tls_handshake_failure"`
	ValidationAuthFailure         int64 `json:"validation_auth_failure"`

	Nodes         []NodeMetricsSnapshot `json:"nodes"`
	NodesDeparted []NodeMetricsSnapshot `json:"nodes_departed"`
	// NodesDepartedPossiblyMore is set when the departed-node set hit the
	// soft-cap (256) and additional departures were not retained.
	NodesDepartedPossiblyMore bool                 `json:"nodes_departed_possibly_more"`
	ClusterAggregated         *NodeMetricsSnapshot `json:"cluster_aggregated,omitempty"`

	// Latencies is cluster.latency.{read,write,batch,query}.
	// LatencyConn slot is unused — connection-open lives on NodeMetricsSnapshot.ConnLatency.
	Latencies []LatencySnapshot `json:"latencies"`

	// BatchPartialFailure is batch.partial_failure (SHOULD).
	BatchPartialFailure int64 `json:"batch_partial_failure"`

	Usage *UsageMetricsSnapshot `json:"usage,omitempty"`

	LatencyUnit    LatencyUnit `json:"latency_unit"`
	LatencyColumns int         `json:"latency_columns"`
	LatencyShift   int         `json:"latency_shift"`
}

// NodeMetricsSnapshot is per-node snapshot data.
type NodeMetricsSnapshot struct {
	NodeName    string `json:"node_name"`
	NodeAddress string `json:"node_address"`
	NodePort    int    `json:"node_port"`

	Connections ConnectionMetricsSnapshot `json:"connections"`

	// On existing Node (not sampler-gated). Pre-Node → MetricsSnapshot validation.*.
	// open.failure rolls up tls/auth on this node.
	ConnectionOpenFailure         int64 `json:"connection_open_failure"`
	ConnectionTlsHandshakeFailure int64 `json:"connection_tls_handshake_failure"`
	ConnectionAuthFailure         int64 `json:"connection_auth_failure"`

	// Legacy diagnostics retained on this client.
	CircuitBreakerHits       int64 `json:"circuit_breaker_hits"`
	ConnectionPoolEmptyCount int64 `json:"connection_pool_empty_count"`
	// ConnectionPoolExhaustedCount is node.connections.pool.exhausted (SHOULD).
	ConnectionPoolExhaustedCount int64 `json:"connection_pool_exhausted_count"`
	TendsTotal                   int64 `json:"tends_total"`
	PartitionMapUpdates          int64 `json:"partition_map_updates"`
	// ErrorRate is the circuit-breaker window error count (node.error_rate).
	ErrorRate int64 `json:"error_rate"`

	// Language-optional fine-grained command histograms (exporter legacy
	// extensions). Not part of the shared MUST latency taxonomy.
	GetMetrics        HistogramSnapshot `json:"get_metrics"`
	GetHeaderMetrics  HistogramSnapshot `json:"get_header_metrics"`
	ExistsMetrics     HistogramSnapshot `json:"exists_metrics"`
	PutMetrics        HistogramSnapshot `json:"put_metrics"`
	DeleteMetrics     HistogramSnapshot `json:"delete_metrics"`
	OperateMetrics    HistogramSnapshot `json:"operate_metrics"`
	QueryMetrics      HistogramSnapshot `json:"query_metrics"`
	ScanMetrics       HistogramSnapshot `json:"scan_metrics"`
	UDFMetrics        HistogramSnapshot `json:"udf_metrics"`
	BatchReadMetrics  HistogramSnapshot `json:"batch_read_metrics"`
	BatchWriteMetrics HistogramSnapshot `json:"batch_write_metrics"`

	// ConnLatency is node.latency.conn (successful new socket open). Not sampler-gated.
	ConnLatency LatencySnapshot `json:"conn_latency"`

	Namespaces []NamespaceMetricsSnapshot `json:"namespaces"`
}

// ConnectionMetricsSnapshot is the Tier 0 sync pool series.
type ConnectionMetricsSnapshot struct {
	InUse  int `json:"in_use"`
	InPool int `json:"in_pool"`
	Opened int `json:"opened"`
	Closed int `json:"closed"`
	// Close-reason breakdown (SHOULD). Closed remains the total.
	ClosedError       int `json:"closed_error"`
	ClosedIdle        int `json:"closed_idle"`
	ClosedPoolShrink  int `json:"closed_pool_shrink"`
	ClosedNodeRemoved int `json:"closed_node_removed"`
}

// NamespaceMetricsSnapshot is operational per-namespace data.
type NamespaceMetricsSnapshot struct {
	Namespace string `json:"namespace"`

	Errors         int64 `json:"errors"`
	ClientTimeouts int64 `json:"client_timeouts"`
	ServerTimeouts int64 `json:"server_timeouts"`
	KeyBusy        int64 `json:"key_busy"`
	RecordTooBig   int64 `json:"record_too_big"`
	DeviceOverload int64 `json:"device_overload"`
	BytesIn        int64 `json:"bytes_in"`
	BytesOut       int64 `json:"bytes_out"`

	// Shared MUST namespace.latency.{read,write,batch,query}. LatencyConn slot
	// is unused here (node.latency.conn on NodeMetricsSnapshot).
	Latencies []LatencySnapshot `json:"latencies"`

	// Language-optional per-command phase histograms for this namespace.
	// Empty when no detailed metrics were recorded.
	Commands []CommandMetricsSnapshot `json:"commands,omitempty"`
}

// LatencySnapshot is one shared latency class.
type LatencySnapshot struct {
	Type         LatencyType `json:"type"`
	BucketCounts []uint64    `json:"bucket_counts"`
	Min          uint64      `json:"min"`
	Max          uint64      `json:"max"`
	Sum          float64     `json:"sum"`
	Count        uint64      `json:"count"`
}

// HistogramSnapshot is a language-optional fine-grained / phase histogram.
type HistogramSnapshot struct {
	BucketCounts []uint64 `json:"bucket_counts"`
	Min          uint64   `json:"min"`
	Max          uint64   `json:"max"`
	Sum          float64  `json:"sum"`
	Count        uint64   `json:"count"`
}

// CommandMetricsSnapshot is Go's per-namespace, per-command phase metrics
// (connection acquire, total latency, parse, bytes). Language-optional.
type CommandMetricsSnapshot struct {
	Command       string            `json:"command"` // get, put, batch-read, …
	ConnectionAq  HistogramSnapshot `json:"connection_aq"`
	Latency       HistogramSnapshot `json:"latency"`
	Parsing       HistogramSnapshot `json:"parsing"`
	BytesSent     HistogramSnapshot `json:"bytes_sent"`
	BytesReceived HistogramSnapshot `json:"bytes_received"`
}

// UsageMetricsSnapshot is client-wide feature.* counters.
// Object-mapping counters are omitted — this client has no OM APIs.
type UsageMetricsSnapshot struct {
	Blocking   int64 `json:"blocking"`
	Deferred   int64 `json:"deferred"`
	Background int64 `json:"background"`
	ShapePoint int64 `json:"shape_point"`
	ShapeBatch int64 `json:"shape_batch"`
	ShapeQuery int64 `json:"shape_query"`
	FilterAEL  int64 `json:"filter_ael"`
	FilterExp  int64 `json:"filter_exp"`
}

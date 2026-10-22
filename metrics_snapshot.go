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
// of this enum (collection Decision 5).
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
// Counters and histogram buckets are cumulative since enable and are not reset
// on on-demand read or on export.
//
// Dual latency scopes (Decision 15): Latencies on MetricsSnapshot is
// cluster.latency.{read,write,batch,query}; NamespaceMetricsSnapshot.Latencies
// is namespace.latency.{read,write,batch,query}; NodeMetricsSnapshot.ConnLatency
// is node.latency.conn. Go also keeps fine-grained / per-command and phase
// histograms on the snapshot additively (language-optional, Decision 5).
type MetricsSnapshot struct {
	Timestamp time.Time

	MetricsEnabled            bool
	OperationalMetricsEnabled bool
	UsageMetricsEnabled       bool

	ClusterName   string
	ClientType    string
	ClientVersion string
	AppID         string
	Labels        map[string]string

	TotalNodes             int
	RecoverQueueSize       int
	InvalidNodeCount       int
	DelayQueueTimeoutCount int64
	OpenConnections        int64

	CommandCount   int64
	CommandRetries int64
	// RetryCount is a legacy API alias of CommandRetries (collection Decision 9).
	// The Go metrics file writes command_retries.
	RetryCount int64
	// ExceededMaxRetries / ExceededTotalTimeout are Go legacy diagnostics
	// (not shared MUST).
	ExceededMaxRetries   int64
	ExceededTotalTimeout int64
	CpuPercent           float64
	MemoryBytes          int64

	// CM-8 pre-Node only (seed / NodeValidator that never becomes a Node).
	// Pool/setup failures on an existing Node live on NodeMetricsSnapshot.
	// open.failure rolls up tls/auth in that home.
	ValidationOpenFailure         int64
	ValidationTlsHandshakeFailure int64
	ValidationAuthFailure         int64

	Nodes         []NodeMetricsSnapshot
	NodesDeparted []NodeMetricsSnapshot
	// NodesDepartedPossiblyMore is set when the departed-node set hit the
	// soft-cap (256) and additional departures were not retained.
	NodesDepartedPossiblyMore bool
	ClusterAggregated         *NodeMetricsSnapshot

	// Latencies is cluster.latency.{read,write,batch,query} (Decision 15).
	// LatencyConn slot is unused — connection-open lives on NodeMetricsSnapshot.ConnLatency.
	Latencies []LatencySnapshot

	// BatchPartialFailure is batch.partial_failure (SHOULD / metrics.md §4.7).
	BatchPartialFailure int64

	Usage *UsageMetricsSnapshot

	LatencyUnit    LatencyUnit
	LatencyColumns int
	LatencyShift   int
}

// NodeMetricsSnapshot is per-node snapshot data.
type NodeMetricsSnapshot struct {
	NodeName    string
	NodeAddress string
	NodePort    int

	Connections ConnectionMetricsSnapshot

	// CM-8 on existing Node (not sampler-gated). Pre-Node → MetricsSnapshot validation.*.
	// open.failure rolls up tls/auth on this node.
	ConnectionOpenFailure         int64
	ConnectionTlsHandshakeFailure int64
	ConnectionAuthFailure         int64

	// Legacy diagnostics retained on this client.
	CircuitBreakerHits       int64
	ConnectionPoolEmptyCount int64
	TendsTotal               int64
	PartitionMapUpdates      int64

	// Language-optional fine-grained command histograms (Decision 5 / exporter
	// legacy extensions). Not part of the shared MUST latency taxonomy.
	GetMetrics        HistogramSnapshot
	GetHeaderMetrics  HistogramSnapshot
	ExistsMetrics     HistogramSnapshot
	PutMetrics        HistogramSnapshot
	DeleteMetrics     HistogramSnapshot
	OperateMetrics    HistogramSnapshot
	QueryMetrics      HistogramSnapshot
	ScanMetrics       HistogramSnapshot
	UDFMetrics        HistogramSnapshot
	BatchReadMetrics  HistogramSnapshot
	BatchWriteMetrics HistogramSnapshot

	// ConnLatency is node.latency.conn (successful new socket open). Not sampler-gated.
	ConnLatency LatencySnapshot

	Namespaces []NamespaceMetricsSnapshot
}

// ConnectionMetricsSnapshot is the Tier 0 sync pool series.
type ConnectionMetricsSnapshot struct {
	InUse  int
	InPool int
	Opened int
	Closed int
}

// NamespaceMetricsSnapshot is operational per-namespace data.
type NamespaceMetricsSnapshot struct {
	Namespace string

	Errors         int64
	ClientTimeouts int64
	ServerTimeouts int64
	KeyBusy        int64
	RecordTooBig   int64
	DeviceOverload int64
	BytesIn        int64
	BytesOut       int64

	// Shared MUST namespace.latency.{read,write,batch,query}. LatencyConn slot
	// is unused here (node.latency.conn on NodeMetricsSnapshot).
	Latencies []LatencySnapshot

	// Language-optional per-command phase histograms for this namespace
	// (Decision 5). Empty when no detailed metrics were recorded.
	Commands []CommandMetricsSnapshot
}

// LatencySnapshot is one shared latency class.
type LatencySnapshot struct {
	Type         LatencyType
	BucketCounts []uint64
	Min          uint64
	Max          uint64
	Sum          float64
	Count        uint64
}

// HistogramSnapshot is a language-optional fine-grained / phase histogram.
type HistogramSnapshot struct {
	BucketCounts []uint64
	Min          uint64
	Max          uint64
	Sum          float64
	Count        uint64
}

// CommandMetricsSnapshot is Go's per-namespace, per-command phase metrics
// (connection acquire, total latency, parse, bytes). Language-optional.
type CommandMetricsSnapshot struct {
	Command       string // get, put, batch-read, …
	ConnectionAq  HistogramSnapshot
	Latency       HistogramSnapshot
	Parsing       HistogramSnapshot
	BytesSent     HistogramSnapshot
	BytesReceived HistogramSnapshot
}

// UsageMetricsSnapshot is client-wide feature.* counters (CM-17 / CM-27).
// Object-mapping counters are omitted — this client has no OM APIs.
type UsageMetricsSnapshot struct {
	Blocking   int64
	Deferred   int64
	Background int64
	ShapePoint int64
	ShapeBatch int64
	ShapeQuery int64
	FilterAEL  int64
	FilterExp  int64
}

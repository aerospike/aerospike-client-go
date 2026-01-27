// Copyright 2014-2022 Aerospike, Inc.
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

import (
	"encoding/json"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/aerospike/aerospike-client-go/v8/types"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	amap "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
	hist "github.com/aerospike/aerospike-client-go/v8/types/histogram"
)

type detailedMetricsArray = [ttMaxCommandTypes]atomic.Pointer[commandMetric]
type detailedResultCodeArray = [ttMaxCommandTypes]atomic.Pointer[commandResultCodeMetric]

// Note: ttMaxCommandTypes is automatically calculated via iota in the commandType enum
// defined in command.go. It represents the total count of command types and is used
// for sizing preallocated arrays for metrics per command type.

// nodeStats keeps track of client's internal node statistics
// These statistics are aggregated once per tend in the cluster object
// and then are served to the end-user.
type nodeStats struct {
	// MetricsPolicy contains policy, default values for histograms, which are used on node_stats init time
	metricPolicy *MetricsPolicy

	// Labels sourced once at applications start and passed down to metrics when metrics are enabled
	StatLabels *Labels `json:"labels,omitempty"`
	// Attempts to open a connection (failed + successful)
	ConnectionsAttempts iatomic.Int `json:"connections-attempts"`
	// Successful attempts to open a connection
	ConnectionsSuccessful iatomic.Int `json:"connections-successful"`
	// Failed attempts to use a connection (includes all errors)
	ConnectionsFailed iatomic.Int `json:"connections-failed"`
	// Connection Timeout errors
	ConnectionsTimeoutErrors iatomic.Int `json:"connections-error-timeout"`
	// Connection errors other than timeouts
	ConnectionsOtherErrors iatomic.Int `json:"connections-error-other"`
	// Number of times circuit breaker was hit
	CircuitBreakerHits iatomic.Int `json:"circuit-breaker-hits"`
	// The command polled the connection pool, but no connections were in the pool
	ConnectionsPoolEmpty iatomic.Int `json:"connections-pool-empty"`
	// The command offered the connection to the pool, but the pool was full and the connection was closed
	ConnectionsPoolOverflow iatomic.Int `json:"connections-pool-overflow"`
	// The connection was idle and was dropped
	ConnectionsIdleDropped iatomic.Int `json:"connections-idle-dropped"`
	// Number of open connections at a given time
	ConnectionsOpen iatomic.Int `json:"open-connections"`
	// Number of connections that were closed, for any reason (idled out, errored out, etc)
	ConnectionsClosed iatomic.Int `json:"closed-connections"`
	// Total number of attempted tends (failed + success)
	TendsTotal iatomic.Int `json:"tends-total"`
	// Total number of successful tends
	TendsSuccessful iatomic.Int `json:"tends-successful"`
	// Total number of failed tends
	TendsFailed iatomic.Int `json:"tends-failed"`
	// Total number of partition map updates
	PartitionMapUpdates iatomic.Int `json:"partition-map-updates"`
	// Total number of times nodes were added to the client (not the same as actual nodes added. Network disruptions between client and server may cause a node being dropped and re-added client-side)
	NodeAdded iatomic.Int `json:"node-added-count"`
	// Total number of times nodes were removed from the client (not the same as actual nodes removed. Network disruptions between client and server may cause a node being dropped client-side)
	NodeRemoved iatomic.Int `json:"node-removed-count"`
	// Total number of command retries
	TransactionRetryCount iatomic.Int `json:"transaction-retry-count"`
	// Total number of command errors
	TransactionErrorCount iatomic.Int `json:"transaction-error-count"`
	// Total number of connections recovered from the pool
	ConnectionsRecovered iatomic.Int `json:"connections-recovered"`
	// Metrics for Get commands
	GetMetrics hist.SyncHistogram[uint64] `json:"get-metrics"`
	// Metrics for GetHeader commands
	GetHeaderMetrics hist.SyncHistogram[uint64] `json:"get-header-metrics"`
	// Metrics for Exists commands
	ExistsMetrics hist.SyncHistogram[uint64] `json:"exists-metrics"`
	// Metrics for Put commands
	PutMetrics hist.SyncHistogram[uint64] `json:"put-metrics"`
	// Metrics for Delete commands
	DeleteMetrics hist.SyncHistogram[uint64] `json:"delete-metrics"`
	// Metrics for Operate commands
	OperateMetrics hist.SyncHistogram[uint64] `json:"operate-metrics"`
	// Metrics for Query commands
	QueryMetrics hist.SyncHistogram[uint64] `json:"query-metrics"`
	// Metrics for Scan commands
	ScanMetrics hist.SyncHistogram[uint64] `json:"scan-metrics"`
	// Metrics for UDFMetrics commands
	UDFMetrics hist.SyncHistogram[uint64] `json:"udf-metrics"`
	// Metrics for Read only Batch commands
	BatchReadMetrics hist.SyncHistogram[uint64] `json:"batch-read-metrics"`
	// Metrics for Batch commands containing writes
	BatchWriteMetrics hist.SyncHistogram[uint64] `json:"batch-write-metrics"`
	// Error counts for each command - using preallocated arrays for commandType (lock-free)
	DetailedResultCodeCounts sync.Map // map[string]*detailedResultCodeArray
	// Detailed metrics for per namespace and per command type - using preallocated arrays for commandType (lock-free)
	DetailedMetrics sync.Map // map[string]*detailedMetricsArray
}

// commandResultCodeMetric keeps track of the ResultCode counts for a given command
type commandResultCodeMetric struct {
	ResultCodeCounts amap.Map[types.ResultCode, uint64] `json:"resultcode-counts"`
}

// commandMetric keeps track of detailed metrics for a given command
type commandMetric struct {
	ConnectionAq  hist.SyncHistogram[uint64] `json:"connection-aq"`
	Latency       hist.SyncHistogram[uint64] `json:"latency"`
	Parsing       hist.SyncHistogram[uint64] `json:"parsing"`
	BytesSent     hist.SyncHistogram[uint64] `json:"bytes-sent"`
	BytesReceived hist.SyncHistogram[uint64] `json:"bytes-received"`
}

// newCommandMetric creates a new CommandMetric object
func (n *nodeStats) newCommandMetric() *commandMetric {
	return &commandMetric{
		ConnectionAq:  *hist.NewSync[uint64](n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns),
		Latency:       *hist.NewSync[uint64](n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns),
		Parsing:       *hist.NewSync[uint64](n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns),
		BytesSent:     *hist.NewSync[uint64](n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns),
		BytesReceived: *hist.NewSync[uint64](n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns),
	}
}

// newNodeStats creates a new NodeStats object
func newNodeStats(policy *MetricsPolicy) *nodeStats {
	if policy == nil {
		policy = DefaultMetricsPolicy()
	}

	return &nodeStats{
		metricPolicy:     policy,
		StatLabels:       NewLabels(),
		GetMetrics:       *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		GetHeaderMetrics: *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		ExistsMetrics:    *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		PutMetrics:       *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		DeleteMetrics:    *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		OperateMetrics:   *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		QueryMetrics:     *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		ScanMetrics:      *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		UDFMetrics:       *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		BatchReadMetrics:  *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
		BatchWriteMetrics: *hist.NewSync[uint64](policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns),
	}
}

// newCommandResultCodeMetric creates a new CommandErrorMetric object
func (n *nodeStats) newCommandResultCodeMetric() *commandResultCodeMetric {
	return &commandResultCodeMetric{
		ResultCodeCounts: *amap.NewWithValue[types.ResultCode, uint64](0, 0),
	}
}

// newCommandResultCodeMetricWithValue creates a new CommandErrorMetric object with the given ResultCode
func (n *nodeStats) newCommandResultCodeMetricWithValue(resultCode types.ResultCode) *commandResultCodeMetric {
	return &commandResultCodeMetric{
		ResultCodeCounts: *amap.NewWithValue[types.ResultCode, uint64](resultCode, 0),
	}
}

// getOrCreateMetricsArray returns the metrics array for a namespace, creating it if needed (lock-free)
func (n *nodeStats) getOrCreateMetricsArray(namespace string) *detailedMetricsArray {
	if val, ok := n.DetailedMetrics.Load(namespace); ok {
		return val.(*detailedMetricsArray)
	}
	newArr := &detailedMetricsArray{}
	actual, _ := n.DetailedMetrics.LoadOrStore(namespace, newArr)
	return actual.(*detailedMetricsArray)
}

// getOrCreateResultCodeArray returns the result code array for a namespace, creating it if needed (lock-free)
func (n *nodeStats) getOrCreateResultCodeArray(namespace string) *detailedResultCodeArray {
	if val, ok := n.DetailedResultCodeCounts.Load(namespace); ok {
		return val.(*detailedResultCodeArray)
	}
	newArr := &detailedResultCodeArray{}
	actual, _ := n.DetailedResultCodeCounts.LoadOrStore(namespace, newArr)
	return actual.(*detailedResultCodeArray)
}

// getOrCreateCommandMetric returns the command metric for a slot, creating it if needed (lock-free CAS)
func (n *nodeStats) getOrCreateCommandMetric(arr *detailedMetricsArray, ct commandType) *commandMetric {
	if cm := arr[ct].Load(); cm != nil {
		return cm
	}
	newCm := n.newCommandMetric()
	if arr[ct].CompareAndSwap(nil, newCm) {
		return newCm
	}
	return arr[ct].Load()
}

// getOrCreateResultCodeMetric returns the result code metric for a slot, creating it if needed (lock-free CAS)
func (n *nodeStats) getOrCreateResultCodeMetric(arr *detailedResultCodeArray, ct commandType, resultCode types.ResultCode) *commandResultCodeMetric {
	if m := arr[ct].Load(); m != nil {
		return m
	}
	newM := n.newCommandResultCodeMetricWithValue(resultCode)
	if arr[ct].CompareAndSwap(nil, newM) {
		return newM
	}
	return arr[ct].Load()
}

// latest returns the latest values to be used in aggregation and then resets the values
func (ns *nodeStats) getAndReset() *nodeStats {
	res := &nodeStats{
		metricPolicy:             ns.metricPolicy,
		StatLabels:               NewLabels(),
		ConnectionsAttempts:      ns.ConnectionsAttempts.CloneAndSet(0),
		ConnectionsSuccessful:    ns.ConnectionsSuccessful.CloneAndSet(0),
		ConnectionsFailed:        ns.ConnectionsFailed.CloneAndSet(0),
		ConnectionsTimeoutErrors: ns.ConnectionsTimeoutErrors.CloneAndSet(0),
		ConnectionsOtherErrors:   ns.ConnectionsOtherErrors.CloneAndSet(0),
		CircuitBreakerHits:       ns.CircuitBreakerHits.CloneAndSet(0),
		ConnectionsPoolEmpty:     ns.ConnectionsPoolEmpty.CloneAndSet(0),
		ConnectionsPoolOverflow:  ns.ConnectionsPoolOverflow.CloneAndSet(0),
		ConnectionsIdleDropped:   ns.ConnectionsIdleDropped.CloneAndSet(0),
		ConnectionsOpen:          ns.ConnectionsOpen.CloneAndSet(0),
		ConnectionsClosed:        ns.ConnectionsClosed.CloneAndSet(0),
		ConnectionsRecovered:     ns.ConnectionsRecovered.CloneAndSet(0),
		TendsTotal:               ns.TendsTotal.CloneAndSet(0),
		TendsSuccessful:          ns.TendsSuccessful.CloneAndSet(0),
		TendsFailed:              ns.TendsFailed.CloneAndSet(0),
		PartitionMapUpdates:      ns.PartitionMapUpdates.CloneAndSet(0),
		NodeAdded:                ns.NodeAdded.CloneAndSet(0),
		NodeRemoved:              ns.NodeRemoved.CloneAndSet(0),

		TransactionRetryCount: ns.TransactionRetryCount.CloneAndSet(0),
		TransactionErrorCount: ns.TransactionErrorCount.CloneAndSet(0),

		GetMetrics:       *ns.GetMetrics.CloneAndReset(),
		GetHeaderMetrics: *ns.GetHeaderMetrics.CloneAndReset(),
		ExistsMetrics:    *ns.ExistsMetrics.CloneAndReset(),
		PutMetrics:       *ns.PutMetrics.CloneAndReset(),
		DeleteMetrics:    *ns.DeleteMetrics.CloneAndReset(),
		OperateMetrics:   *ns.OperateMetrics.CloneAndReset(),
		QueryMetrics:     *ns.QueryMetrics.CloneAndReset(),
		ScanMetrics:      *ns.ScanMetrics.CloneAndReset(),
		UDFMetrics:       *ns.UDFMetrics.CloneAndReset(),
		BatchReadMetrics:  *ns.BatchReadMetrics.CloneAndReset(),
		BatchWriteMetrics: *ns.BatchWriteMetrics.CloneAndReset(),
	}

	ns.cloneAndResetDetailedResultCodeCountsInto(&res.DetailedResultCodeCounts)
	ns.cloneAndResetDetailedMetricsInto(&res.DetailedMetrics)

	return res
}

func (ns *nodeStats) clone() nodeStats {
	res := nodeStats{
		metricPolicy:             ns.metricPolicy,
		StatLabels:               NewLabels(),
		ConnectionsAttempts:      ns.ConnectionsAttempts.Clone(),
		ConnectionsSuccessful:    ns.ConnectionsSuccessful.Clone(),
		ConnectionsFailed:        ns.ConnectionsFailed.Clone(),
		ConnectionsTimeoutErrors: ns.ConnectionsTimeoutErrors.Clone(),
		ConnectionsOtherErrors:   ns.ConnectionsOtherErrors.Clone(),
		CircuitBreakerHits:       ns.CircuitBreakerHits.Clone(),
		ConnectionsPoolEmpty:     ns.ConnectionsPoolEmpty.Clone(),
		ConnectionsPoolOverflow:  ns.ConnectionsPoolOverflow.Clone(),
		ConnectionsIdleDropped:   ns.ConnectionsIdleDropped.Clone(),
		ConnectionsOpen:          ns.ConnectionsOpen.Clone(),
		ConnectionsClosed:        ns.ConnectionsClosed.Clone(),
		ConnectionsRecovered:     ns.ConnectionsRecovered.Clone(),
		TendsTotal:               ns.TendsTotal.Clone(),
		TendsSuccessful:          ns.TendsSuccessful.Clone(),
		TendsFailed:              ns.TendsFailed.Clone(),
		PartitionMapUpdates:      ns.PartitionMapUpdates.Clone(),
		NodeAdded:                ns.NodeAdded.Clone(),
		NodeRemoved:              ns.NodeRemoved.Clone(),

		TransactionRetryCount: ns.TransactionRetryCount.Clone(),
		TransactionErrorCount: ns.TransactionErrorCount.Clone(),

		GetMetrics:       *ns.GetMetrics.Clone(),
		GetHeaderMetrics: *ns.GetHeaderMetrics.Clone(),
		ExistsMetrics:    *ns.ExistsMetrics.Clone(),
		PutMetrics:       *ns.PutMetrics.Clone(),
		DeleteMetrics:    *ns.DeleteMetrics.Clone(),
		OperateMetrics:   *ns.OperateMetrics.Clone(),
		QueryMetrics:     *ns.QueryMetrics.Clone(),
		ScanMetrics:      *ns.ScanMetrics.Clone(),
		UDFMetrics:       *ns.UDFMetrics.Clone(),
		BatchReadMetrics:  *ns.BatchReadMetrics.Clone(),
		BatchWriteMetrics: *ns.BatchWriteMetrics.Clone(),
	}

	ns.cloneDetailedResultCodeCountsInto(&res.DetailedResultCodeCounts)
	ns.cloneDetailedMetricsInto(&res.DetailedMetrics)

	return res
}

// cloneDetailedMetricsInto clones DetailedMetrics into the target sync.Map
func (ns *nodeStats) cloneDetailedMetricsInto(target *sync.Map) {
	ns.DetailedMetrics.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedMetricsArray)

		tgtArr := &detailedMetricsArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}
			tgtMetric := ns.newCommandMetric()
			tgtMetric.ConnectionAq = *srcMetric.ConnectionAq.Clone()
			tgtMetric.Latency = *srcMetric.Latency.Clone()
			tgtMetric.Parsing = *srcMetric.Parsing.Clone()
			tgtMetric.BytesSent = *srcMetric.BytesSent.Clone()
			tgtMetric.BytesReceived = *srcMetric.BytesReceived.Clone()
			tgtArr[ct].Store(tgtMetric)
		}
		target.Store(namespace, tgtArr)
		return true
	})
}

// cloneDetailedResultCodeCountsInto clones DetailedResultCodeCounts into the target sync.Map
func (ns *nodeStats) cloneDetailedResultCodeCountsInto(target *sync.Map) {
	ns.DetailedResultCodeCounts.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedResultCodeArray)

		tgtArr := &detailedResultCodeArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}
			tgtMetric := ns.newCommandResultCodeMetric()
			tgtMetric.ResultCodeCounts = *srcMetric.ResultCodeCounts.CloneMap()
			tgtArr[ct].Store(tgtMetric)
		}
		target.Store(namespace, tgtArr)
		return true
	})
}

// cloneAndResetDetailedMetricsInto clones and resets DetailedMetrics into the target sync.Map
func (ns *nodeStats) cloneAndResetDetailedMetricsInto(target *sync.Map) {
	ns.DetailedMetrics.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedMetricsArray)

		tgtArr := &detailedMetricsArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}
			tgtMetric := ns.newCommandMetric()
			tgtMetric.ConnectionAq = *srcMetric.ConnectionAq.CloneAndReset()
			tgtMetric.Latency = *srcMetric.Latency.CloneAndReset()
			tgtMetric.Parsing = *srcMetric.Parsing.CloneAndReset()
			tgtMetric.BytesSent = *srcMetric.BytesSent.CloneAndReset()
			tgtMetric.BytesReceived = *srcMetric.BytesReceived.CloneAndReset()
			tgtArr[ct].Store(tgtMetric)
		}
		target.Store(namespace, tgtArr)
		return true
	})
}

// cloneAndResetDetailedResultCodeCountsInto clones and resets DetailedResultCodeCounts into the target sync.Map
func (ns *nodeStats) cloneAndResetDetailedResultCodeCountsInto(target *sync.Map) {
	ns.DetailedResultCodeCounts.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedResultCodeArray)

		tgtArr := &detailedResultCodeArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}
			tgtMetric := ns.newCommandResultCodeMetric()
			tgtMetric.ResultCodeCounts = *srcMetric.ResultCodeCounts.CloneAndResetMap()
			tgtArr[ct].Store(tgtMetric)
		}
		target.Store(namespace, tgtArr)
		return true
	})
}

func (ns *nodeStats) reshape(policy *MetricsPolicy) {
	ns.metricPolicy = policy
	ns.StatLabels = NewLabels()
	ns.GetMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.GetHeaderMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.ExistsMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.PutMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.DeleteMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.OperateMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.QueryMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.ScanMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.UDFMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.BatchReadMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.BatchWriteMetrics.Reshape(policy.HistogramType, uint64(policy.LatencyBase), policy.LatencyColumns)
	ns.reshapeDetailedResultCodeCounts()
	ns.reshapeDetailedMetrics()
}

func (ns *nodeStats) aggregate(newStats *nodeStats) {
	ns.StatLabels = NewLabels()
	ns.ConnectionsAttempts.AddAndGet(newStats.ConnectionsAttempts.Get())
	ns.ConnectionsSuccessful.AddAndGet(newStats.ConnectionsSuccessful.Get())
	ns.ConnectionsFailed.AddAndGet(newStats.ConnectionsFailed.Get())
	ns.ConnectionsTimeoutErrors.AddAndGet(newStats.ConnectionsTimeoutErrors.Get())
	ns.ConnectionsOtherErrors.AddAndGet(newStats.ConnectionsOtherErrors.Get())
	ns.CircuitBreakerHits.AddAndGet(newStats.CircuitBreakerHits.Get())
	ns.ConnectionsPoolEmpty.AddAndGet(newStats.ConnectionsPoolEmpty.Get())
	ns.ConnectionsPoolOverflow.AddAndGet(newStats.ConnectionsPoolOverflow.Get())
	ns.ConnectionsIdleDropped.AddAndGet(newStats.ConnectionsIdleDropped.Get())
	ns.ConnectionsOpen.AddAndGet(newStats.ConnectionsOpen.Get())
	ns.ConnectionsClosed.AddAndGet(newStats.ConnectionsClosed.Get())
	ns.ConnectionsRecovered.AddAndGet(newStats.ConnectionsRecovered.Get())
	ns.TendsTotal.AddAndGet(newStats.TendsTotal.Get())
	ns.TendsSuccessful.AddAndGet(newStats.TendsSuccessful.Get())
	ns.TendsFailed.AddAndGet(newStats.TendsFailed.Get())
	ns.PartitionMapUpdates.AddAndGet(newStats.PartitionMapUpdates.Get())
	ns.NodeAdded.AddAndGet(newStats.NodeAdded.Get())
	ns.NodeRemoved.AddAndGet(newStats.NodeRemoved.Get())

	ns.TransactionRetryCount.AddAndGet(newStats.TransactionRetryCount.Get())
	ns.TransactionErrorCount.AddAndGet(newStats.TransactionErrorCount.Get())

	ns.GetMetrics.Merge(&newStats.GetMetrics)
	ns.GetHeaderMetrics.Merge(&newStats.GetHeaderMetrics)
	ns.ExistsMetrics.Merge(&newStats.ExistsMetrics)
	ns.PutMetrics.Merge(&newStats.PutMetrics)
	ns.DeleteMetrics.Merge(&newStats.DeleteMetrics)
	ns.OperateMetrics.Merge(&newStats.OperateMetrics)
	ns.QueryMetrics.Merge(&newStats.QueryMetrics)
	ns.ScanMetrics.Merge(&newStats.ScanMetrics)
	ns.UDFMetrics.Merge(&newStats.UDFMetrics)
	ns.BatchReadMetrics.Merge(&newStats.BatchReadMetrics)
	ns.BatchWriteMetrics.Merge(&newStats.BatchWriteMetrics)
	ns.mergeCommandResultCodeMetric(newStats)
	ns.mergeDetailedMetrics(newStats)
}

func (ns nodeStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		StatsLabels              []map[string]string                  `json:"labels,omitempty"`
		ConnectionsAttempts      int                                  `json:"connections-attempts"`
		ConnectionsSuccessful    int                                  `json:"connections-successful"`
		ConnectionsFailed        int                                  `json:"connections-failed"`
		ConnectionsTimeoutErrors int                                  `json:"connections-error-timeout"`
		ConnectionsOtherErrors   int                                  `json:"connections-error-other"`
		CircuitBreakerHits       int                                  `json:"circuit-breaker-hits"`
		ConnectionsPoolEmpty     int                                  `json:"connections-pool-empty"`
		ConnectionsPoolOverflow  int                                  `json:"connections-pool-overflow"`
		ConnectionsIdleDropped   int                                  `json:"connections-idle-dropped"`
		ConnectionsOpen          int                                  `json:"open-connections"`
		ConnectionsClosed        int                                  `json:"closed-connections"`
		ConnectionsRecovered     int                                  `json:"connections-recovered"`
		TendsTotal               int                                  `json:"tends-total"`
		TendsSuccessful          int                                  `json:"tends-successful"`
		TendsFailed              int                                  `json:"tends-failed"`
		PartitionMapUpdates      int                                  `json:"partition-map-updates"`
		NodeAdded                int                                  `json:"node-added-count"`
		NodeRemoved              int                                  `json:"node-removed-count"`
		RetryCount               int                                  `json:"transaction-retry-count"`
		ErrorCount               int                                  `json:"transaction-error-count"`
		GetMetrics               hist.SyncHistogram[uint64]           `json:"get-metrics"`
		GetHeaderMetrics         hist.SyncHistogram[uint64]           `json:"get-header-metrics"`
		ExistsMetrics            hist.SyncHistogram[uint64]           `json:"exists-metrics"`
		PutMetrics               hist.SyncHistogram[uint64]           `json:"put-metrics"`
		DeleteMetrics            hist.SyncHistogram[uint64]           `json:"delete-metrics"`
		OperateMetrics           hist.SyncHistogram[uint64]           `json:"operate-metrics"`
		QueryMetrics             hist.SyncHistogram[uint64]           `json:"query-metrics"`
		ScanMetrics              hist.SyncHistogram[uint64]           `json:"scan-metrics"`
		UDFMetrics               hist.SyncHistogram[uint64]           `json:"udf-metrics"`
		BatchReadMetrics         hist.SyncHistogram[uint64]           `json:"batch-read-metrics"`
		BatchWriteMetrics        hist.SyncHistogram[uint64]           `json:"batch-write-metrics"`
		ErrorCounts              map[string]map[string]map[string]int `json:"detailed-resultcode-counts"`
		DetailedMetrics          map[string]map[string]*commandMetric `json:"detailed-metrics"`
	}{
		*ns.StatLabels,
		ns.ConnectionsAttempts.Get(),
		ns.ConnectionsSuccessful.Get(),
		ns.ConnectionsFailed.Get(),
		ns.ConnectionsTimeoutErrors.Get(),
		ns.ConnectionsOtherErrors.Get(),
		ns.CircuitBreakerHits.Get(),
		ns.ConnectionsPoolEmpty.Get(),
		ns.ConnectionsPoolOverflow.Get(),
		ns.ConnectionsIdleDropped.Get(),
		ns.ConnectionsOpen.Get(),
		ns.ConnectionsClosed.Get(),
		ns.ConnectionsRecovered.Get(),
		ns.TendsTotal.Get(),
		ns.TendsSuccessful.Get(),
		ns.TendsFailed.Get(),
		ns.PartitionMapUpdates.Get(),
		ns.NodeAdded.Get(),
		ns.NodeRemoved.Get(),

		ns.TransactionRetryCount.Get(),
		ns.TransactionErrorCount.Get(),

		ns.GetMetrics,
		ns.GetHeaderMetrics,
		ns.ExistsMetrics,
		ns.PutMetrics,
		ns.DeleteMetrics,
		ns.OperateMetrics,
		ns.QueryMetrics,
		ns.ScanMetrics,
		ns.UDFMetrics,
		ns.BatchReadMetrics,
		ns.BatchWriteMetrics,
		ns.marshalResultCodeCounts(),
		ns.marshalDetailedMetrics(),
	})
}

// Serializes DetailedMetrics for json encoding
func (ns *nodeStats) marshalDetailedMetrics() map[string]map[string]*commandMetric {
	result := make(map[string]map[string]*commandMetric)
	ns.DetailedMetrics.Range(func(key, value any) bool {
		namespace := key.(string)
		arr := value.(*detailedMetricsArray)
		metrics := make(map[string]*commandMetric)
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			if cm := arr[ct].Load(); cm != nil {
				metrics[ct.String()] = cm
			}
		}
		if len(metrics) > 0 {
			result[namespace] = metrics
		}
		return true
	})
	return result
}

// Serializes DetailedResultCodeCounts for json encoding
func (ns *nodeStats) marshalResultCodeCounts() map[string]map[string]map[string]int {
	result := make(map[string]map[string]map[string]int)
	ns.DetailedResultCodeCounts.Range(func(key, value any) bool {
		namespace := key.(string)
		arr := value.(*detailedResultCodeArray)
		commandMap := make(map[string]map[string]int)
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			if m := arr[ct].Load(); m != nil {
				resultCodeMap := make(map[string]int)
				for _, rc := range m.ResultCodeCounts.Keys() {
					resultCodeMap[rc.String()] = int(m.ResultCodeCounts.Get(rc))
				}
				if len(resultCodeMap) > 0 {
					commandMap[ct.String()] = resultCodeMap
				}
			}
		}
		if len(commandMap) > 0 {
			result[namespace] = commandMap
		}
		return true
	})
	return result
}

func (ns *nodeStats) UnmarshalJSON(data []byte) error {
	aux := struct {
		ConnectionsAttempts      int `json:"connections-attempts"`
		ConnectionsSuccessful    int `json:"connections-successful"`
		ConnectionsFailed        int `json:"connections-failed"`
		ConnectionsTimeoutErrors int `json:"connections-error-timeout"`
		ConnectionsOtherErrors   int `json:"connections-error-other"`
		CircuitBreakerHits       int `json:"circuit-breaker-hits"`
		ConnectionsPoolEmpty     int `json:"connections-pool-empty"`
		ConnectionsPoolOverflow  int `json:"connections-pool-overflow"`
		ConnectionsIdleDropped   int `json:"connections-idle-dropped"`
		ConnectionsOpen          int `json:"open-connections"`
		ConnectionsClosed        int `json:"closed-connections"`
		ConnectionsRecovered     int `json:"connections-recovered"`
		TendsTotal               int `json:"tends-total"`
		TendsSuccessful          int `json:"tends-successful"`
		TendsFailed              int `json:"tends-failed"`
		PartitionMapUpdates      int `json:"partition-map-updates"`
		NodeAdded                int `json:"node-added-count"`
		NodeRemoved              int `json:"node-removed-count"`

		RetryCount int `json:"transaction-retry-count"`
		ErrorCount int `json:"transaction-error-count"`

		GetMetrics               hist.SyncHistogram[uint64]                                     `json:"get-metrics"`
		GetHeaderMetrics         hist.SyncHistogram[uint64]                                     `json:"get-header-metrics"`
		ExistsMetrics            hist.SyncHistogram[uint64]                                     `json:"exists-metrics"`
		PutMetrics               hist.SyncHistogram[uint64]                                     `json:"put-metrics"`
		DeleteMetrics            hist.SyncHistogram[uint64]                                     `json:"delete-metrics"`
		OperateMetrics           hist.SyncHistogram[uint64]                                     `json:"operate-metrics"`
		QueryMetrics             hist.SyncHistogram[uint64]                                     `json:"query-metrics"`
		ScanMetrics              hist.SyncHistogram[uint64]                                     `json:"scan-metrics"`
		UDFMetrics               hist.SyncHistogram[uint64]                                     `json:"udf-metrics"`
		BatchReadMetrics         hist.SyncHistogram[uint64]                                     `json:"batch-read-metrics"`
		BatchWriteMetrics        hist.SyncHistogram[uint64]                                     `json:"batch-write-metrics"`
		DetailedResultCodeCounts amap.Map[string, *[ttMaxCommandTypes]*commandResultCodeMetric] `json:"detailed-resultcode-counts"`
		DetailedMetrics          amap.Map[string, *[ttMaxCommandTypes]*commandMetric]           `json:"detailed-metrics"`
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ns.ConnectionsAttempts.Set(aux.ConnectionsAttempts)
	ns.ConnectionsSuccessful.Set(aux.ConnectionsSuccessful)
	ns.ConnectionsFailed.Set(aux.ConnectionsFailed)
	ns.ConnectionsTimeoutErrors.Set(aux.ConnectionsTimeoutErrors)
	ns.ConnectionsOtherErrors.Set(aux.ConnectionsOtherErrors)
	ns.CircuitBreakerHits.Set(aux.CircuitBreakerHits)
	ns.ConnectionsPoolEmpty.Set(aux.ConnectionsPoolEmpty)
	ns.ConnectionsPoolOverflow.Set(aux.ConnectionsPoolOverflow)
	ns.ConnectionsIdleDropped.Set(aux.ConnectionsIdleDropped)
	ns.ConnectionsOpen.Set(aux.ConnectionsOpen)
	ns.ConnectionsClosed.Set(aux.ConnectionsClosed)
	ns.ConnectionsRecovered.Set(aux.ConnectionsRecovered)
	ns.TendsTotal.Set(aux.TendsTotal)
	ns.TendsSuccessful.Set(aux.TendsSuccessful)
	ns.TendsFailed.Set(aux.TendsFailed)
	ns.PartitionMapUpdates.Set(aux.PartitionMapUpdates)
	ns.NodeAdded.Set(aux.NodeAdded)
	ns.NodeRemoved.Set(aux.NodeRemoved)

	ns.TransactionRetryCount.Set(aux.RetryCount)
	ns.TransactionErrorCount.Set(aux.ErrorCount)

	ns.GetMetrics = aux.GetMetrics
	ns.GetHeaderMetrics = aux.GetHeaderMetrics
	ns.ExistsMetrics = aux.ExistsMetrics
	ns.PutMetrics = aux.PutMetrics
	ns.DeleteMetrics = aux.DeleteMetrics
	ns.OperateMetrics = aux.OperateMetrics
	ns.QueryMetrics = aux.QueryMetrics
	ns.ScanMetrics = aux.ScanMetrics
	ns.UDFMetrics = aux.UDFMetrics
	ns.BatchReadMetrics = aux.BatchReadMetrics
	ns.BatchWriteMetrics = aux.BatchWriteMetrics

	// Convert amap.Map to sync.Map for DetailedMetrics
	for _, namespace := range aux.DetailedMetrics.Keys() {
		srcArr := aux.DetailedMetrics.Get(namespace)
		if srcArr == nil {
			continue
		}
		tgtArr := &detailedMetricsArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			if srcArr[ct] != nil {
				tgtArr[ct].Store(srcArr[ct])
			}
		}
		ns.DetailedMetrics.Store(namespace, tgtArr)
	}

	// Convert amap.Map to sync.Map for DetailedResultCodeCounts
	for _, namespace := range aux.DetailedResultCodeCounts.Keys() {
		srcArr := aux.DetailedResultCodeCounts.Get(namespace)
		if srcArr == nil {
			continue
		}
		tgtArr := &detailedResultCodeArray{}
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			if srcArr[ct] != nil {
				tgtArr[ct].Store(srcArr[ct])
			}
		}
		ns.DetailedResultCodeCounts.Store(namespace, tgtArr)
	}

	return nil
}

// mergeDetailedMetrics merges detailed metrics from the incoming stats into the current stats.
func (n *nodeStats) mergeDetailedMetrics(ns *nodeStats) {
	ns.DetailedMetrics.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedMetricsArray)

		tgtArr := n.getOrCreateMetricsArray(namespace)

		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}

			tgtMetric := n.getOrCreateCommandMetric(tgtArr, ct)
			tgtMetric.ConnectionAq.Merge(&srcMetric.ConnectionAq)
			tgtMetric.Latency.Merge(&srcMetric.Latency)
			tgtMetric.Parsing.Merge(&srcMetric.Parsing)
			tgtMetric.BytesSent.Merge(&srcMetric.BytesSent)
			tgtMetric.BytesReceived.Merge(&srcMetric.BytesReceived)
		}
		return true
	})
}

// mergeCommandResultCodeMetric merges detailed error metrics from the incoming stats into the current stats.
func (n *nodeStats) mergeCommandResultCodeMetric(ns *nodeStats) {
	ns.DetailedResultCodeCounts.Range(func(key, value any) bool {
		namespace := key.(string)
		srcArr := value.(*detailedResultCodeArray)

		tgtArr := n.getOrCreateResultCodeArray(namespace)

		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			srcMetric := srcArr[ct].Load()
			if srcMetric == nil {
				continue
			}

			tgtMetric := tgtArr[ct].Load()
			if tgtMetric == nil {
				newM := n.newCommandResultCodeMetric()
				if tgtArr[ct].CompareAndSwap(nil, newM) {
					tgtMetric = newM
				} else {
					tgtMetric = tgtArr[ct].Load()
				}
			}

			for _, resultCode := range srcMetric.ResultCodeCounts.Keys() {
				delta := srcMetric.ResultCodeCounts.Get(resultCode)
				if cur := tgtMetric.ResultCodeCounts.Get(resultCode); cur > 0 {
					tgtMetric.ResultCodeCounts.Set(resultCode, cur+delta)
				} else {
					tgtMetric.ResultCodeCounts.Set(resultCode, delta)
				}
			}
		}
		return true
	})
}

// reshapeDetailedMetrics reshapes the detailed metrics as defined by `hist.SyncHistogram`
func (n *nodeStats) reshapeDetailedMetrics() {
	n.DetailedMetrics.Range(func(key, value any) bool {
		arr := value.(*detailedMetricsArray)
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			metric := arr[ct].Load()
			if metric == nil {
				continue
			}
			metric.ConnectionAq.Reshape(n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns)
			metric.Latency.Reshape(n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns)
			metric.Parsing.Reshape(n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns)
			metric.BytesSent.Reshape(n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns)
			metric.BytesReceived.Reshape(n.metricPolicy.HistogramType, uint64(n.metricPolicy.LatencyBase), n.metricPolicy.LatencyColumns)
		}
		return true
	})
}

// reshapeDetailedResultCodeCounts reshapes the detailed error metrics
func (n *nodeStats) reshapeDetailedResultCodeCounts() {
	n.DetailedResultCodeCounts.Range(func(key, value any) bool {
		arr := value.(*detailedResultCodeArray)
		for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
			metric := arr[ct].Load()
			if metric == nil {
				continue
			}
			for _, resultCode := range metric.ResultCodeCounts.Keys() {
				metric.ResultCodeCounts.Set(resultCode, 0)
			}
		}
		return true
	})
}

// updateOrInsert updates result code counts (lock-free)
func (n *nodeStats) updateOrInsert(namespace *string, namespaces iter.Seq2[string, uint64], ct commandType, resultCode types.ResultCode) {
	if namespace != nil {
		arr := n.getOrCreateResultCodeArray(*namespace)
		m := n.getOrCreateResultCodeMetric(arr, ct, resultCode)
		if cur := m.ResultCodeCounts.Get(resultCode); cur > 0 {
			m.ResultCodeCounts.Set(resultCode, cur+1)
		} else {
			m.ResultCodeCounts.Set(resultCode, 1)
		}
	} else {
		for ns := range namespaces {
			arr := n.getOrCreateResultCodeArray(ns)
			m := n.getOrCreateResultCodeMetric(arr, ct, resultCode)
			if cur := m.ResultCodeCounts.Get(resultCode); cur > 0 {
				m.ResultCodeCounts.Set(resultCode, cur+1)
			} else {
				m.ResultCodeCounts.Set(resultCode, 1)
			}
		}
	}
}

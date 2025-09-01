package aerospike

import (
	"iter"
	"sync/atomic"
	"testing"
	"time"

	amap "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

type fakeCommand struct {
	baseCommand
	namespace  string
	namespaces []string
	ct         commandType
}

func (fc *fakeCommand) getNamespaces() iter.Seq2[string, uint64] {
	return fc.nsIter
}

func (fc *fakeCommand) nsIter(yield func(string, uint64) bool) {
	for i := range fc.namespaces {
		if !yield(fc.namespaces[i], 1) {
			return
		}
	}
}

func (fc *fakeCommand) getNamespace() *string {
	return nil
}

func (fc *fakeCommand) commandType() commandType {
	return fc.ct
}

func (fn *fakeCommand) Execute() Error {
	return nil
}

func (fn *fakeCommand) getPolicy(ifc command) Policy {
	return nil
}

func (fn *fakeCommand) writeBuffer(ifc command) Error {
	return nil
}

func (fn *fakeCommand) getNode(ifc command) (*Node, Error) {
	return nil, nil
}

func (fn *fakeCommand) getConnection(policy Policy) (*Connection, Error) {
	return nil, nil
}

func (fn *fakeCommand) putConnection(conn *Connection) {

}

func (fn *fakeCommand) parseResult(ifc command, conn *Connection) Error {
	return nil
}

func (fn *fakeCommand) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	return false, nil
}

func (fn *fakeCommand) prepareRetry(ifc command, isTimeout bool) bool {
	return false
}

func (fn *fakeCommand) isRead() bool {
	return false
}

func (fn *fakeCommand) onInDoubt() {

}

func (fn *fakeCommand) execute(ifc command) Error {
	return nil
}

func (fn *fakeCommand) executeIter(ifc command, iter int) Error {
	return nil
}

func (fn *fakeCommand) executeAt(ifc command, policy *BasePolicy, deadline time.Time, iterations int) Error {
	return nil
}

func (fn *fakeCommand) canPutConnBack() bool {
	return false
}

func (fn *fakeCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {

}

func newStub() *fakeCommand {
	mp := DefaultMetricsPolicy()
	nodeStats := newNodeStats(mp)
	me.Store(true)
	// setup a fake node and baseCommand with metrics enabled.
	node := &Node{
		cluster: &Cluster{
			metricsEnabled: me,
		},
		stats: *nodeStats,
	}

	// baseCommand to be used for the benchmark.
	cmd := baseCommand{
		node: node,
	}

	// Use a non-empty namespace and a command type (for example, ttGet).
	fcmd := fakeCommand{
		baseCommand: cmd,
		namespace:   "test",
		ct:          ttGet,
	}
	return &fcmd
}

func newStubWithNamespaces() *fakeCommand {
	mp := DefaultMetricsPolicy()
	nodeStats := newNodeStats(mp)
	me.Store(true)
	// setup a fake node and baseCommand with metrics enabled.
	node := &Node{
		cluster: &Cluster{
			metricsEnabled: me,
		},
		stats: *nodeStats,
	}

	// baseCommand to be used for the benchmark.
	cmd := baseCommand{
		node: node,
	}

	// Use a non-empty namespace and a command type (for example, ttGet).
	fcmd := fakeCommand{
		baseCommand: cmd,
		namespaces:  []string{"test", "testTwo", "testThree", "testFour", "testFive", "testSix"},
		ct:          ttGet,
	}
	return &fcmd
}

var me atomic.Bool

func BenchmarkApplyDetailedMetricsDataSizeAndLatency(b *testing.B) {
	b.StopTimer()
	fcmd := newStubWithNamespaces()
	// fcmd := newFakeCmdSingle()

	now := time.Now()

	// bytesSent value to simulate.
	bytesSent := 100
	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Call the function under test.
		fcmd.applyDetailedMetricsDataSizeAndLatencyOnWrite(fcmd, bytesSent, now)
	}
}

func BenchmarkApplyDetailedConnectionAq(b *testing.B) {
	b.StopTimer()
	fcmd := newStub()

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Call the function under test.
		fcmd.applyDetailedMetricsConnectionAq(fcmd, time.Now())
	}
}

func BenchmarkApplyDetailedParsing(b *testing.B) {
	b.StopTimer()
	fcmd := newStub()

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	bytesReceived := 100
	for i := 0; i < b.N; i++ {
		// Call the function under test.
		fcmd.applyDetailedMetricsParsing(fcmd, time.Now(), int64(bytesReceived))
	}
}

func BenchmarkCommandMergeCommandResultCodeMetrics(b *testing.B) {
	b.StopTimer()
	mp := DefaultMetricsPolicy()
	targetNodeStats := newNodeStats(mp)
	sourceNodeStats := newNodeStats(mp)

	sourceNodeStats.updateOrInsert(&fakeCommand{namespace: "test", ct: ttGet}, types.ResultCode(0))
	sourceNodeStats.updateOrInsert(&fakeCommand{namespace: "testTwo", ct: ttPut}, types.ResultCode(0))
	sourceNodeStats.updateOrInsert(&fakeCommand{namespace: "testThree", ct: ttExists}, types.ResultCode(0))

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		targetNodeStats.mergeCommandResultCodeMetric(sourceNodeStats)
	}
}

func BenchmarkCommandMergeDetailMetrics(b *testing.B) {
	b.StopTimer()
	policy := DefaultMetricsPolicy()
	sourceNodeStats := newNodeStats(policy)
	targetNodeStats := newNodeStats(policy)

	b.StopTimer()
	operations := []commandType{ttExists}

	for _, operation := range operations {
		sourceNodeStats.DetailedMetrics.UpdateOrInsertFn("test", func(inner *amap.Map[commandType, *commandMetric]) *amap.Map[commandType, *commandMetric] {
			// Insert metrics for ttGet
			inner.UpdateOrInsertFn(operation, func(cm *commandMetric) *commandMetric {
				// Simulate metrics data
				cm.Parsing.Add(uint64(1))
				cm.BytesSent.Add(uint64(1))
				cm.ConnectionAq.Add(uint64(1))
				cm.Latency.Add(uint64(1))
				return cm
			}, func() *commandMetric {
				return sourceNodeStats.newCommandMetric()
			})
			return inner
		}, func() *amap.Map[commandType, *commandMetric] {
			return amap.NewWithValue(operation, sourceNodeStats.newCommandMetric())
		})
	}

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Merge incoming detailed metrics into target.
		targetNodeStats.mergeDetailedMetrics(sourceNodeStats)
	}
}

func BenchmarkCloneDetailedMetrics(b *testing.B) {
	b.StopTimer()
	policy := DefaultMetricsPolicy()

	ns := newNodeStats(policy)

	const testNamespace = "test"
	sampleMap := amap.New[commandType, *commandMetric](0)
	sampleMap.UpdateOrInsertFn(ttGet, func(old *commandMetric) *commandMetric {
		return old
	}, func() *commandMetric {
		return ns.newCommandMetric()
	})
	// Insert the sample map under the test namespace.
	ns.DetailedMetrics.UpdateOrInsertFn(testNamespace, func(old *amap.Map[commandType, *commandMetric]) *amap.Map[commandType, *commandMetric] {
		return old
	}, func() *amap.Map[commandType, *commandMetric] {
		return sampleMap
	})

	now := time.Now()
	ns.DetailedMetrics.Get(testNamespace).Get(ttGet).Latency.Add(uint64(now.UnixNano()))
	ns.DetailedMetrics.Get(testNamespace).Get(ttGet).BytesSent.Add(1024)
	ns.DetailedMetrics.Get(testNamespace).Get(ttGet).Parsing.Add(uint64(now.UnixNano()))
	ns.DetailedMetrics.Get(testNamespace).Get(ttGet).ConnectionAq.Add(5)

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cloned := ns.cloneDetailedMetrics()
		// Use cloned in some trivial way to prevent compiler optimizations.
		if cloned == nil || cloned.Length() == 0 {
			b.Fatal("cloned DetailedMetrics is nil or empty")
		}
	}
}

func BenchmarkCloneAndResetDetailedResultCodeCounts(b *testing.B) {
	b.StopTimer()
	policy := DefaultMetricsPolicy()
	ns := newNodeStats(policy)
	ns.DetailedResultCodeCounts.UpdateOrInsertFn("test", func(inner *amap.Map[commandType, *commandResultCodeMetric]) *amap.Map[commandType, *commandResultCodeMetric] {
		inner.UpdateOrInsertFn(ttGet, func(metric *commandResultCodeMetric) *commandResultCodeMetric {
			metric.ResultCodeCounts.UpdateOrInsert(types.ResultCode(0), func(val uint64) uint64 {
				return val + 1
			}, 0)
			return metric
		}, func() *commandResultCodeMetric {
			return ns.newCommandResultCodeMetricWithValue(types.ResultCode(0))
		})
		return inner
	}, func() *amap.Map[commandType, *commandResultCodeMetric] {
		return amap.NewWithValue(ttGet, ns.newCommandResultCodeMetricWithValue(types.ResultCode(0)))
	})

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cloned := ns.cloneAndResetDetailedResultCodeCounts()
		if cloned.Length() == 0 {
			b.Fatal("cloned DetailedResultCodeCounts is empty")
		}
	}
}

type fakeCmdSingle struct {
	fakeCommand
}

func (fc *fakeCmdSingle) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (fc *fakeCmdSingle) getNamespace() *string {
	return &fc.namespace
}

func newFakeCmdSingle() *fakeCmdSingle {
	return &fakeCmdSingle{
		fakeCommand: *newStub(),
	}
}

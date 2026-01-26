package aerospike

import (
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	fcmd1 := &fakeCommand{namespace: "test", ct: ttGet}
	fcmd2 := &fakeCommand{namespace: "testTwo", ct: ttPut}
	fcmd3 := &fakeCommand{namespace: "testThree", ct: ttExists}
	sourceNodeStats.updateOrInsert(fcmd1.getNamespace(), fcmd1.getNamespaces(), fcmd1.commandType(), types.ResultCode(0))
	sourceNodeStats.updateOrInsert(fcmd2.getNamespace(), fcmd2.getNamespaces(), fcmd2.commandType(), types.ResultCode(0))
	sourceNodeStats.updateOrInsert(fcmd3.getNamespace(), fcmd3.getNamespaces(), fcmd3.commandType(), types.ResultCode(0))

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
		arr := sourceNodeStats.DetailedMetrics.Get("test")
		if arr == nil {
			arr = &[ttMaxCommandTypes]*commandMetric{}
			sourceNodeStats.DetailedMetrics.Set("test", arr)
		}
		
		cm := arr[operation]
		if cm == nil {
			cm = sourceNodeStats.newCommandMetric()
			arr[operation] = cm
		}
		
		// Simulate metrics data
		cm.Parsing.Add(uint64(1))
		cm.BytesSent.Add(uint64(1))
		cm.ConnectionAq.Add(uint64(1))
		cm.Latency.Add(uint64(1))
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
	
	// Create and populate array with metrics
	arr := &[ttMaxCommandTypes]*commandMetric{}
	arr[ttGet] = ns.newCommandMetric()
	ns.DetailedMetrics.Set(testNamespace, arr)

	now := time.Now()
	ns.DetailedMetrics.Get(testNamespace)[ttGet].Latency.Add(uint64(now.UnixNano()))
	ns.DetailedMetrics.Get(testNamespace)[ttGet].BytesSent.Add(1024)
	ns.DetailedMetrics.Get(testNamespace)[ttGet].Parsing.Add(uint64(now.UnixNano()))
	ns.DetailedMetrics.Get(testNamespace)[ttGet].ConnectionAq.Add(5)

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
	
	// Create and populate array with result code metrics
	arr := &[ttMaxCommandTypes]*commandResultCodeMetric{}
	arr[ttGet] = ns.newCommandResultCodeMetricWithValue(types.ResultCode(0))
	arr[ttGet].ResultCodeCounts.Set(types.ResultCode(0), 1)
	ns.DetailedResultCodeCounts.Set("test", arr)

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

// BenchmarkUpdateOrInsertHighConcurrency benchmarks updateOrInsert with high concurrency across many namespaces
func BenchmarkUpdateOrInsertHighConcurrency(b *testing.B) {
	const (
		numNamespaces = 200
		numGoroutines = 100
	)

	b.StopTimer()

	// Create metrics policy and node stats
	mp := DefaultMetricsPolicy()
	nodeStats := newNodeStats(mp)
	me.Store(true)

	node := &Node{
		cluster: &Cluster{
			metricsEnabled: me,
		},
		stats: *nodeStats,
	}

	// Generate namespace names: ns_0, ns_1, ..., ns_199
	namespaces := make([]string, numNamespaces)
	for i := 0; i < numNamespaces; i++ {
		namespaces[i] = fmt.Sprintf("ns_%d", i)
	}

	commandTypes := []commandType{ttGet, ttPut, ttExists, ttDelete, ttBatchRead}

	fakeCommands := make([]*fakeCommand, 0, numNamespaces*len(commandTypes))
	for _, ns := range namespaces {
		for _, ct := range commandTypes {
			cmd := &fakeCommand{
				baseCommand: baseCommand{node: node},
				namespace:   ns,
				ct:          ct,
			}
			fakeCommands = append(fakeCommands, cmd)
		}
	}

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Divide work across goroutines
		workPerGoroutine := len(fakeCommands) / numGoroutines
		if workPerGoroutine == 0 {
			workPerGoroutine = 1
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			start := g * workPerGoroutine
			end := start + workPerGoroutine
			if g == numGoroutines-1 {
				end = len(fakeCommands)
			}
			if start >= len(fakeCommands) {
				wg.Done()
				continue
			}
			if end > len(fakeCommands) {
				end = len(fakeCommands)
			}

			go func(cmds []*fakeCommand) {
				defer wg.Done()
				for _, cmd := range cmds {
					resultCode := types.ResultCode(0) // OK
					nodeStats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), cmd.commandType(), resultCode)
				}
			}(fakeCommands[start:end])
		}

		wg.Wait()
	}
}

// BenchmarkUpdateOrInsertHighConcurrencyContended benchmarks with maximum contention
// where all goroutines compete for the same namespace
func BenchmarkUpdateOrInsertHighConcurrencyContended(b *testing.B) {
	const (
		numNamespaces   = 200
		numGoroutines   = 100
		opsPerGoroutine = 1000
	)

	b.StopTimer()

	mp := DefaultMetricsPolicy()
	nodeStats := newNodeStats(mp)
	me.Store(true)

	node := &Node{
		cluster: &Cluster{
			metricsEnabled: me,
		},
		stats: *nodeStats,
	}

	// Generate namespace names
	namespaces := make([]string, numNamespaces)
	for i := 0; i < numNamespaces; i++ {
		namespaces[i] = fmt.Sprintf("ns_%d", i)
	}

	commandTypes := []commandType{ttGet, ttPut, ttExists, ttDelete, ttBatchRead}

	b.StartTimer()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				// Each goroutine performs many operations
				for op := 0; op < opsPerGoroutine; op++ {
					// Rotate through namespaces and command types
					nsIdx := (goroutineID*opsPerGoroutine + op) % len(namespaces)
					ctIdx := (goroutineID*opsPerGoroutine + op) % len(commandTypes)

					cmd := &fakeCommand{
						baseCommand: baseCommand{node: node},
						namespace:   namespaces[nsIdx],
						ct:          commandTypes[ctIdx],
					}

					// Vary result codes
					// Common result codes that are most common. Could add more but leaving at 5 for now.
					resultCode := types.ResultCode(op % 5)
					nodeStats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), cmd.commandType(), resultCode)
				}
			}(g)
		}

		wg.Wait()
	}
}

// BenchmarkUpdateOrInsertScalability tests scalability with varying goroutine counts
func BenchmarkUpdateOrInsertScalability(b *testing.B) {
	goroutineCounts := []int{1, 10, 50, 100, 200, 500}

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("goroutines_%d", numGoroutines), func(b *testing.B) {
			const numNamespaces = 200

			b.StopTimer()

			mp := DefaultMetricsPolicy()
			nodeStats := newNodeStats(mp)
			me.Store(true)

			node := &Node{
				cluster: &Cluster{
					metricsEnabled: me,
				},
				stats: *nodeStats,
			}

			namespaces := make([]string, numNamespaces)
			for i := 0; i < numNamespaces; i++ {
				namespaces[i] = fmt.Sprintf("ns_%d", i)
			}

			b.StartTimer()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				for g := 0; g < numGoroutines; g++ {
					go func(id int) {
						defer wg.Done()
						ns := namespaces[id%len(namespaces)]
						cmd := &fakeCommand{
							baseCommand: baseCommand{node: node},
							namespace:   ns,
							ct:          ttGet,
						}
						nodeStats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), cmd.commandType(), types.ResultCode(0))
					}(g)
				}

				wg.Wait()
			}
		})
	}
}

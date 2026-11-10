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

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

func TestAggregateNodeSnapshotsDoesNotAliasFirstNode(t *testing.T) {
	n0 := NodeMetricsSnapshot{
		NodeName: "A",
		Namespaces: []NamespaceMetricsSnapshot{{
			Namespace: "test",
			BytesIn:   10,
			Latencies: []LatencySnapshot{{
				Type:         LatencyRead,
				BucketCounts: []uint64{1, 0, 0},
				Min:          0,
				Max:          0,
				Count:        1,
			}},
		}},
		ConnLatency: LatencySnapshot{
			Type:         LatencyConn,
			BucketCounts: []uint64{2, 0},
			Min:          0,
			Count:        2,
		},
	}
	n1 := NodeMetricsSnapshot{
		NodeName: "B",
		Namespaces: []NamespaceMetricsSnapshot{{
			Namespace: "test",
			BytesIn:   20,
			Latencies: []LatencySnapshot{{
				Type:         LatencyRead,
				BucketCounts: []uint64{0, 1, 0},
				Min:          2,
				Max:          2,
				Count:        1,
			}},
		}},
		ConnLatency: LatencySnapshot{
			Type:         LatencyConn,
			BucketCounts: []uint64{0, 3},
			Min:          1,
			Count:        3,
		},
	}

	nodes := []NodeMetricsSnapshot{cloneNodeMetricsSnapshot(n0), cloneNodeMetricsSnapshot(n1)}
	agg := cloneNodeMetricsSnapshot(nodes[0])
	agg.NodeName = ""
	aggregateNodeSnapshots(&agg, &nodes[1])

	if nodes[0].Namespaces[0].BytesIn != 10 {
		t.Fatalf("Nodes[0] BytesIn corrupted: got %d want 10", nodes[0].Namespaces[0].BytesIn)
	}
	if nodes[0].Namespaces[0].Latencies[0].BucketCounts[0] != 1 {
		t.Fatalf("Nodes[0] latency buckets aliased/corrupted: %v", nodes[0].Namespaces[0].Latencies[0].BucketCounts)
	}
	if nodes[0].ConnLatency.BucketCounts[0] != 2 {
		t.Fatalf("Nodes[0] ConnLatency aliased: %v", nodes[0].ConnLatency.BucketCounts)
	}
	if agg.Namespaces[0].BytesIn != 30 {
		t.Fatalf("agg BytesIn: got %d want 30", agg.Namespaces[0].BytesIn)
	}
	if agg.Namespaces[0].Latencies[0].BucketCounts[0] != 1 || agg.Namespaces[0].Latencies[0].BucketCounts[1] != 1 {
		t.Fatalf("agg latency buckets: %v", agg.Namespaces[0].Latencies[0].BucketCounts)
	}
	// Min=0 from n0 must survive merge with n1.Min=2.
	if agg.Namespaces[0].Latencies[0].Min != 0 {
		t.Fatalf("agg Min should stay 0, got %d", agg.Namespaces[0].Latencies[0].Min)
	}
}

func TestMergeLatencyPreservesZeroMin(t *testing.T) {
	d := LatencySnapshot{BucketCounts: []uint64{1}, Min: 0, Max: 0, Count: 1}
	s := LatencySnapshot{BucketCounts: []uint64{1}, Min: 5, Max: 5, Count: 1}
	mergeLatency(&d, &s)
	if d.Min != 0 {
		t.Fatalf("Min=0 must be preserved, got %d", d.Min)
	}
	if d.Count != 2 || d.Max != 5 {
		t.Fatalf("unexpected merge result: %+v", d)
	}
}

func TestDecideOperationalSample(t *testing.T) {
	clstr := &Cluster{}
	clstr.reshapeClusterLatencies(DefaultMetricsPolicy())

	clstr.operationalEnabled.Store(false)
	if clstr.decideOperationalSample() {
		t.Fatal("operational off must not sample")
	}

	clstr.operationalEnabled.Store(true)
	p := DefaultMetricsPolicy()
	p.SetSamplerNever()
	clstr.metricsPolicy.Set(p)
	if clstr.decideOperationalSample() {
		t.Fatal("threshold 0 must not sample")
	}

	p = DefaultMetricsPolicy() // range=1 threshold=1
	clstr.metricsPolicy.Set(p)
	if !clstr.decideOperationalSample() {
		t.Fatal("full-rate must sample")
	}

	// ~10% sample rate: range=10, threshold=1 → band [8%, 12%].
	p = DefaultMetricsPolicy()
	p.SamplerRange = 10
	p.SamplerThreshold = 1
	clstr.metricsPolicy.Set(p)
	const n = 10_000
	var hits10 int
	for i := 0; i < n; i++ {
		if clstr.decideOperationalSample() {
			hits10++
		}
	}
	rate10 := float64(hits10) / float64(n)
	if rate10 < 0.08 || rate10 > 0.12 {
		t.Fatalf("expected ~0.10 sample rate, got %.3f (%d/%d)", rate10, hits10, n)
	}

	// Extra non-10% band: ~30% at range=10 threshold=3.
	p = DefaultMetricsPolicy()
	p.SamplerRange = 10
	p.SamplerThreshold = 3
	clstr.metricsPolicy.Set(p)
	var hits30 int
	for i := 0; i < n; i++ {
		if clstr.decideOperationalSample() {
			hits30++
		}
	}
	rate30 := float64(hits30) / float64(n)
	if rate30 < 0.25 || rate30 > 0.35 {
		t.Fatalf("expected ~0.30 sample rate, got %.3f (%d/%d)", rate30, hits30, n)
	}
}

func TestMetricsPolicyEqualAndHistogramChanged(t *testing.T) {
	a := DefaultMetricsPolicy()
	b := a.copy()
	if !metricsPolicyEqual(a, b) {
		t.Fatal("identical policies must compare equal")
	}
	b.Interval = 5 * time.Second
	if metricsPolicyEqual(a, b) {
		t.Fatal("interval change must differ")
	}
	if !metricsHistogramChanged(nil, a) {
		t.Fatal("nil prev → reshape")
	}
	b = a.copy()
	b.LatencyUnit = LatencyMicroseconds
	if !metricsHistogramChanged(a, b) {
		t.Fatal("unit change must reshape")
	}
	b = a.copy()
	if metricsHistogramChanged(a, b) {
		t.Fatal("unchanged shape must not reshape")
	}
}

func TestIsClientOriginTimeout(t *testing.T) {
	if !isClientOriginTimeout(ErrTimeout.err()) {
		t.Fatal("ErrTimeout is client-origin")
	}
	srv := newServerError(types.TIMEOUT, "server timed out", types.SubCodeNone, nil)
	if isClientOriginTimeout(srv) {
		t.Fatal("server TIMEOUT must not count as client timeout")
	}
	if isClientOriginTimeout(newError(types.KEY_BUSY)) {
		t.Fatal("non-timeout must be false")
	}
}

func TestSetSamplerProbabilityClamp(t *testing.T) {
	p := DefaultMetricsPolicy()
	p.SetSamplerProbability(math.NaN())
	if p.SamplerThreshold != 0 {
		t.Fatalf("NaN → 0, got %d", p.SamplerThreshold)
	}
	p.SetSamplerProbability(-1)
	if p.SamplerThreshold != 0 {
		t.Fatalf("neg → 0, got %d", p.SamplerThreshold)
	}
	p.SetSamplerProbability(2)
	if p.SamplerThreshold != 1_000_000 {
		t.Fatalf(">1 → full, got %d", p.SamplerThreshold)
	}
	p.SetSamplerProbability(math.Inf(1))
	if p.SamplerThreshold != 0 {
		t.Fatalf("Inf → 0, got %d", p.SamplerThreshold)
	}
}

type unhashableExporter struct {
	m map[string]int
}

func (e unhashableExporter) Export(*MetricsSnapshot) {}

func TestInstallExportersUnhashableNoPanic(t *testing.T) {
	clstr := &Cluster{}
	clstr.reshapeClusterLatencies(DefaultMetricsPolicy())
	e := &unhashableExporter{m: map[string]int{"x": 1}}
	p := DefaultMetricsPolicy()
	p.Exporter = MetricsExporterCustom
	p.AddExporter(e)
	p.AddExporter(e) // pointer identity dedupes
	gmExpectNoPanic(t, func() { clstr.installExporters(p) })
	if len(clstr.exporters) != 1 {
		t.Fatalf("expected 1 exporter after dedupe, got %d", len(clstr.exporters))
	}
	// Value-typed unhashable exporter must not panic either (not deduped).
	p2 := DefaultMetricsPolicy()
	p2.Exporter = MetricsExporterCustom
	p2.AddExporter(unhashableExporter{m: map[string]int{"y": 2}})
	gmExpectNoPanic(t, func() { clstr.installExporters(p2) })
}

func gmExpectNoPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	fn()
}

func TestMapDynamicOperationalDefaults(t *testing.T) {
	trueVal := true
	falseVal := false

	// Legacy metrics.enable → operational on.
	mp := DefaultMetricsPolicy()
	mp.OperationalEnabled = false
	dc := &DynConfig{
		configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
		logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
		config: &dynconfig.Config{
			Dynamic: &dynconfig.DynamicConfig{
				Metrics: &dynconfig.Metrics{Enable: &trueVal},
			},
		},
	}
	out := mp.copy().mapDynamic(dc)
	if !out.OperationalEnabled {
		t.Fatal("legacy enable should turn operational on")
	}

	// v3 metrics.enabled without extended → operational off.
	mp = DefaultMetricsPolicy()
	dc.config.Dynamic.Metrics = &dynconfig.Metrics{Enabled: &trueVal}
	out = mp.copy().mapDynamic(dc)
	if out.OperationalEnabled {
		t.Fatal("v3 enabled without extended must leave operational off")
	}

	// extended.operational.enabled explicit.
	mp = DefaultMetricsPolicy()
	mp.OperationalEnabled = false
	dc.config.Dynamic.Metrics = &dynconfig.Metrics{
		Enabled: &trueVal,
		Extended: &dynconfig.MetricsExtended{
			Operational: &dynconfig.MetricsOperational{Enabled: &trueVal},
		},
	}
	out = mp.copy().mapDynamic(dc)
	if !out.OperationalEnabled {
		t.Fatal("explicit operational.enabled=true")
	}

	dc.config.Dynamic.Metrics.Extended.Operational.Enabled = &falseVal
	out = mp.copy().mapDynamic(dc)
	if out.OperationalEnabled {
		t.Fatal("explicit operational.enabled=false")
	}
}

func TestClearDepartedAfterSuccessfulExport(t *testing.T) {
	// successful export clears the whole nodes_departed set.
	clstr := &Cluster{}
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "a"})
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "b"})
	clstr.clearDeparted()
	out, overflow := clstr.peekDeparted()
	if len(out) != 0 || overflow {
		t.Fatalf("expected empty departed after clear, got %+v overflow=%v", out, overflow)
	}
}

func TestPeekDepartedOmitsRejoinedLiveNode(t *testing.T) {
	clstr := &Cluster{}
	live := &Node{name: "n1", host: &Host{Name: "127.0.0.1", Port: 3000}}
	clstr.nodes.Set([]*Node{live})
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "n1", NodePort: 3000})
	out, _ := clstr.peekDeparted()
	if len(out) != 0 {
		t.Fatalf("rejoined live node must not appear in nodes_departed, got %+v", out)
	}
	clstr.clearDepartedForLiveNode(live)
	if len(clstr.departed) != 0 {
		t.Fatal("clearDepartedForLiveNode should drop rejoined entry")
	}
}

func TestCaptureDepartedRequiresMetricsEnabled(t *testing.T) {
	clstr := &Cluster{stats: map[string]*nodeStats{}}
	nd := &Node{cluster: clstr, host: &Host{Name: "127.0.0.1", Port: 3000}}
	nd.stats = *newNodeStats(DefaultMetricsPolicy())
	clstr.captureDepartedNode(nd)
	if len(clstr.departed) != 0 {
		t.Fatal("must not capture when metrics disabled")
	}
	clstr.metricsEnabled.Store(true)
	clstr.captureDepartedNode(nd)
	if len(clstr.departed) != 1 {
		t.Fatal("should capture when metrics enabled")
	}
}

type recordingExporter struct {
	mu    sync.Mutex
	count int
}

func (e *recordingExporter) Export(s *MetricsSnapshot) {
	e.mu.Lock()
	e.count++
	e.mu.Unlock()
}

func TestEnableDisableExportLifecycle(t *testing.T) {
	clstr := &Cluster{}
	cp := &atomic.Pointer[ClientPolicy]{}
	cp.Store(&ClientPolicy{})
	clstr.clientPolicy = cp
	clstr.stats = map[string]*nodeStats{}

	rec := &recordingExporter{}
	p := DefaultMetricsPolicy()
	p.Interval = 50 * time.Millisecond
	p.Exporter = MetricsExporterCustom
	p.AddExporter(rec)

	clstr.EnableMetrics(p)
	time.Sleep(120 * time.Millisecond)
	clstr.DisableMetrics()

	rec.mu.Lock()
	n := rec.count
	rec.mu.Unlock()
	if n < 1 {
		t.Fatalf("expected at least one Export tick, got %d", n)
	}
	if clstr.MetricsEnabled() || clstr.usageEnabled.Load() {
		t.Fatal("DisableMetrics should clear metrics and usage flags")
	}
}

func TestBatchNsIterUsesOffsetsOnly(t *testing.T) {
	cmd := &batchCommandGet{
		batchCommand: batchCommand{batch: &batchNode{offsets: []int{1}}},
		keys: []*Key{
			{namespace: "nsA"},
			{namespace: "nsB"},
			{namespace: "nsA"},
		},
	}
	var got []string
	for ns := range cmd.getNamespaces() {
		got = append(got, ns)
	}
	if len(got) != 1 || got[0] != "nsB" {
		t.Fatalf("nsIter must yield only this node offsets, got %v", got)
	}
}

func TestCoerceExportIntervalAndExporterToken(t *testing.T) {
	d, ok := coerceExportInterval("30s")
	if !ok || d != 30*time.Second {
		t.Fatalf("30s: got %v ok=%v", d, ok)
	}
	d, ok = coerceExportInterval(15)
	if !ok || d != 15*time.Second {
		t.Fatalf("int seconds: got %v ok=%v", d, ok)
	}
	if _, ok := coerceExportInterval("nope"); ok {
		t.Fatal("invalid duration must fail")
	}
	if normalizeExporterToken("") != MetricsExporterFile {
		t.Fatal("empty → FILE")
	}
	if normalizeExporterToken("learn_metrics_file") != MetricsExporterFile {
		t.Fatal("alias → FILE")
	}
	if normalizeExporterToken("NONE") != MetricsExporterNone {
		t.Fatal("NONE")
	}
	if normalizeExporterToken("weird") != MetricsExporterCustom {
		t.Fatal("unknown → CUSTOM")
	}
}

type disableFromExportExporter struct {
	clstr *Cluster
	once  sync.Once
	done  chan struct{}
}

func (e *disableFromExportExporter) Export(*MetricsSnapshot) {
	e.once.Do(func() {
		e.clstr.DisableMetrics()
		close(e.done)
	})
}

func TestDisableMetricsFromInsideExport(t *testing.T) {
	clstr := &Cluster{}
	cp := &atomic.Pointer[ClientPolicy]{}
	cp.Store(&ClientPolicy{})
	clstr.clientPolicy = cp
	clstr.stats = map[string]*nodeStats{}

	ex := &disableFromExportExporter{clstr: clstr, done: make(chan struct{})}
	p := DefaultMetricsPolicy()
	p.Interval = 30 * time.Millisecond
	p.Exporter = MetricsExporterCustom
	p.AddExporter(ex)
	clstr.EnableMetrics(p)
	select {
	case <-ex.done:
	case <-time.After(2 * time.Second):
		t.Fatal("DisableMetrics from Export deadlocked or never ran")
	}
	time.Sleep(50 * time.Millisecond)
	if clstr.MetricsEnabled() {
		t.Fatal("expected metrics disabled")
	}
}

func TestExporterSuspendProbesEvery10thInterval(t *testing.T) {
	clstr := &Cluster{}
	failing := &failNExporter{failLeft: 100}
	reg := &registeredExporter{exporter: failing}
	clstr.exporters = []*registeredExporter{reg}
	snap := &MetricsSnapshot{}

	// 3 consecutive failures → suspend.
	for i := 0; i < metricsExporterFailureThreshold; i++ {
		if clstr.fanOutExport(snap) {
			t.Fatalf("expected failure on attempt %d", i+1)
		}
	}
	if reg.skipRemaining != metricsExporterSuspendIntervals {
		t.Fatalf("after N failures skipRemaining want %d got %d", metricsExporterSuspendIntervals, reg.skipRemaining)
	}
	callsAfterSuspend := failing.calls

	// Next 9 ticks must skip (no Export call).
	for i := 0; i < metricsExporterSuspendIntervals-1; i++ {
		clstr.fanOutExport(snap)
	}
	if failing.calls != callsAfterSuspend {
		t.Fatalf("suspended exporter must not be probed before 10th interval: calls %d→%d", callsAfterSuspend, failing.calls)
	}
	// 10th interval probes.
	clstr.fanOutExport(snap)
	if failing.calls != callsAfterSuspend+1 {
		t.Fatalf("expected probe on 10th interval, calls=%d", failing.calls)
	}
	if reg.skipRemaining != metricsExporterSuspendIntervals {
		t.Fatalf("failed probe should re-suspend for %d, got %d", metricsExporterSuspendIntervals, reg.skipRemaining)
	}

	// Succeed on next probe.
	failing.failLeft = 0
	for i := 0; i < metricsExporterSuspendIntervals-1; i++ {
		clstr.fanOutExport(snap)
	}
	if !clstr.fanOutExport(snap) {
		t.Fatal("successful probe should return true")
	}
	if reg.consecutiveFails != 0 || reg.skipRemaining != 0 {
		t.Fatalf("success should clear suspend state, fails=%d skip=%d", reg.consecutiveFails, reg.skipRemaining)
	}
}

type failNExporter struct {
	failLeft int
	calls    int
}

func (e *failNExporter) Export(*MetricsSnapshot) {
	e.calls++
	if e.failLeft > 0 {
		e.failLeft--
		panic("export fail")
	}
}

func TestPerExporterDepartedDrain(t *testing.T) {
	clstr := &Cluster{}
	a := &recordingExporter{}
	b := &failNExporter{failLeft: 100}
	ra := &registeredExporter{exporter: a}
	rb := &registeredExporter{exporter: b}
	clstr.exporters = []*registeredExporter{ra, rb}
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "n1"})

	snap := &MetricsSnapshot{}
	snap.NodesDeparted, _ = clstr.peekDeparted()
	if len(snap.NodesDeparted) != 1 {
		t.Fatalf("expected 1 departed, got %d", len(snap.NodesDeparted))
	}
	clstr.fanOutExport(snap)

	if _, ok := ra.drainedDeparted["n1"]; !ok {
		t.Fatal("successful exporter A should mark n1 drained")
	}
	if len(rb.drainedDeparted) != 0 {
		t.Fatal("failed exporter B must not drain")
	}
	out, _ := clstr.peekDeparted()
	if len(out) != 1 {
		t.Fatalf("shared departed must remain until all exporters drain, got %d", len(out))
	}

	// A alone should not see n1 again.
	filtered := filterDepartedForExporter(snap, ra)
	if len(filtered.NodesDeparted) != 0 {
		t.Fatal("A should filter already-drained key")
	}
}

func TestParseReportSizeLimit(t *testing.T) {
	n, ok := parseReportSizeLimit("10mb")
	if !ok || n != 10_000_000 {
		t.Fatalf("10mb: got %d ok=%v", n, ok)
	}
	n, ok = parseReportSizeLimit("1g")
	if !ok || n != 1_000_000_000 {
		t.Fatalf("1g: got %d ok=%v", n, ok)
	}
	n, ok = coerceReportSizeLimit(1024)
	if !ok || n != 1024 {
		t.Fatalf("int: got %d ok=%v", n, ok)
	}
	if _, ok := parseReportSizeLimit("nope"); ok {
		t.Fatal("invalid size must fail")
	}
}

func TestNormalizeSamplerUnsetToOneOne(t *testing.T) {
	mp := &MetricsPolicy{
		LatencyColumns:     7,
		LatencyBase:        2,
		LatencyShift:       1,
		HistogramType:      1,
		OperationalEnabled: true,
		Interval:           time.Second,
		Exporter:           MetricsExporterNone,
	}
	out := normalizeMetricsPolicy(mp)
	if out.SamplerRange != 1 || out.SamplerThreshold != 1 {
		t.Fatalf("unset sampler → 1/1, got %d/%d", out.SamplerRange, out.SamplerThreshold)
	}
	mp.SetSamplerNever()
	mp.SamplerRange = 1_000_000
	out = normalizeMetricsPolicy(mp)
	if out.SamplerThreshold != 0 {
		t.Fatal("SetSamplerNever must keep threshold 0")
	}
}

func TestFileMetricsExporterBracketBalance(t *testing.T) {
	dir := t.TempDir()
	e := NewFileMetricsExporter(dir)
	snap := &MetricsSnapshot{
		Timestamp:      time.Now(),
		ClusterName:    "c1",
		ClientType:     "go",
		ClientVersion:  "test",
		AppID:          "app",
		LatencyColumns: 7,
		LatencyShift:   1,
		Nodes: []NodeMetricsSnapshot{{
			NodeName:    "A",
			NodeAddress: "127.0.0.1",
			NodePort:    3000,
			Namespaces: []NamespaceMetricsSnapshot{
				{Namespace: "ns1", Latencies: []LatencySnapshot{{Type: LatencyRead, BucketCounts: make([]uint64, 7), Count: 1}}},
				{Namespace: "ns2", Latencies: []LatencySnapshot{{Type: LatencyWrite, BucketCounts: make([]uint64, 7), Count: 1}}},
			},
		}},
	}
	e.Export(snap)
	e.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.Contains(line, " header(") {
			continue
		}
		opens := strings.Count(line, "[")
		closes := strings.Count(line, "]")
		if opens != closes {
			t.Fatalf("unbalanced brackets opens=%d closes=%d line=%s", opens, closes, line)
		}
	}
}

// One failing exporter must not skip the remaining exporters.
func TestFanOutExportContinuesAfterOneFailure(t *testing.T) {
	clstr := &Cluster{}
	ok := &recordingExporter{}
	failing := &failNExporter{failLeft: 100}
	ra := &registeredExporter{exporter: ok}
	rb := &registeredExporter{exporter: failing}
	clstr.exporters = []*registeredExporter{rb, ra} // failing first
	snap := &MetricsSnapshot{}

	clstr.fanOutExport(snap)

	ok.mu.Lock()
	n := ok.count
	ok.mu.Unlock()
	if n != 1 {
		t.Fatalf("successful exporter must still run after a sibling failure, got %d", n)
	}
	if failing.calls != 1 {
		t.Fatalf("failing exporter should have been attempted once, calls=%d", failing.calls)
	}
}

// SetSamplerProbability(0.10) hit rate in [8%, 12%] over large N.
func TestDecideOperationalSampleProbabilityTenPercent(t *testing.T) {
	clstr := &Cluster{}
	clstr.reshapeClusterLatencies(DefaultMetricsPolicy())
	clstr.operationalEnabled.Store(true)
	p := DefaultMetricsPolicy()
	p.SetSamplerProbability(0.10)
	clstr.metricsPolicy.Set(p)

	const n = 10_000
	var hits int
	for i := 0; i < n; i++ {
		if clstr.decideOperationalSample() {
			hits++
		}
	}
	rate := float64(hits) / float64(n)
	if rate < 0.08 || rate > 0.12 {
		t.Fatalf("expected ~0.10 from SetSamplerProbability(0.10), got %.3f (%d/%d)", rate, hits, n)
	}
}

// holdExporter blocks inside Export until release is closed (watchdog tests).
type holdExporter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *holdExporter) Export(*MetricsSnapshot) {
	e.once.Do(func() { close(e.entered) })
	<-e.release
}

// Export overrun past the watchdog counts as failure (custom exporters only).
func TestExportWithWatchdogTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	exp := &holdExporter{entered: entered, release: release}

	done := make(chan exportWatchdogResult, 1)
	go func() {
		done <- exportWithWatchdog(exp, &MetricsSnapshot{}, 40*time.Millisecond)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Export never started")
	}

	select {
	case r := <-done:
		if r != exportTimedOut {
			t.Fatalf("expected exportTimedOut, got %v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not fire")
	}
	close(release) // let the orphaned Export goroutine exit
}

func TestExportWithWatchdogOK(t *testing.T) {
	rec := &recordingExporter{}
	if got := exportWithWatchdog(rec, &MetricsSnapshot{}, time.Second); got != exportOK {
		t.Fatalf("expected exportOK, got %v", got)
	}
	rec.mu.Lock()
	n := rec.count
	rec.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected one Export, got %d", n)
	}
}

func TestExportWithWatchdogPanic(t *testing.T) {
	failing := &failNExporter{failLeft: 1}
	if got := exportWithWatchdog(failing, &MetricsSnapshot{}, time.Second); got != exportFailed {
		t.Fatalf("expected exportFailed on panic, got %v", got)
	}
}

// Non-file callExporter maps watchdog failure to false (feeds suspend path).
func TestCallExporterNonFileTimeoutReturnsFalse(t *testing.T) {
	// callExporter uses the production 10s watchdog; exercise the same failure
	// mapping with exportWithWatchdog + the callExporter panic/timeout branches
	// by using a short-limit helper path equivalent: timed-out Export is !ok.
	entered := make(chan struct{})
	release := make(chan struct{})
	exp := &holdExporter{entered: entered, release: release}

	okCh := make(chan bool, 1)
	go func() {
		// Mirror callExporter's non-file branch with a test-scale deadline.
		switch exportWithWatchdog(exp, &MetricsSnapshot{}, 40*time.Millisecond) {
		case exportOK:
			okCh <- true
		default:
			okCh <- false
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Export never started")
	}
	select {
	case ok := <-okCh:
		if ok {
			t.Fatal("timeout must count as Export failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not fire")
	}
	close(release)
}

func TestCallExporterNonFilePanicReturnsFalse(t *testing.T) {
	if callExporter(&failNExporter{failLeft: 1}, &MetricsSnapshot{}, false) {
		t.Fatal("panic during custom Export must return false")
	}
}

// Client vs server TIMEOUT land on different namespace split counters.
func TestSharedErrorClientVsServerTimeoutSplit(t *testing.T) {
	ns := newNodeStats(DefaultMetricsPolicy())
	ns.addSharedError("test", types.TIMEOUT, true)
	ns.addSharedError("test", types.TIMEOUT, false)
	op := ns.getOrCreateNsOp("test")
	if op.ClientTimeouts.Get() != 1 {
		t.Fatalf("client timeout: got %d", op.ClientTimeouts.Get())
	}
	if op.ServerTimeouts.Get() != 1 {
		t.Fatalf("server timeout: got %d", op.ServerTimeouts.Get())
	}
	if op.Errors.Get() != 0 {
		t.Fatalf("TIMEOUT must not also increment undifferentiated errors, got %d", op.Errors.Get())
	}
}

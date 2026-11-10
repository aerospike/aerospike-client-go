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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

func TestDefaultMetricsPolicySamplerAndHistogram(t *testing.T) {
	p := DefaultMetricsPolicy()
	if p.LatencyColumns != 7 || p.LatencyShift != 1 {
		t.Fatalf("expected columns=7 shift=1, got %d %d", p.LatencyColumns, p.LatencyShift)
	}
	if p.LatencyUnit != LatencyMilliseconds {
		t.Fatalf("expected milliseconds unit")
	}
	if p.SamplerRange != 1 || p.SamplerThreshold != 1 {
		t.Fatalf("expected full-rate sampler, got %d/%d", p.SamplerRange, p.SamplerThreshold)
	}
	if p.Interval != 30*time.Second {
		t.Fatalf("expected 30s export interval, got %s", p.Interval)
	}
	if !p.OperationalEnabled || p.UsageEnabled {
		t.Fatalf("legacy EnableMetrics mapping: operational on, usage off")
	}
	if p.Exporter != MetricsExporterFile {
		t.Fatalf("expected FILE exporter token, got %q", p.Exporter)
	}
	// report_dir alone does not enable collection; empty report_dir installs no file writer
	if p.fileExporterRequested() {
		t.Fatalf("empty ReportDir must not request the file exporter")
	}
	p.ReportDir = "/tmp/metrics"
	if !p.fileExporterRequested() {
		t.Fatalf("non-empty ReportDir with FILE should request the file exporter")
	}
	p.Exporter = MetricsExporterNone
	if p.periodicExportEnabled() || p.fileExporterRequested() {
		t.Fatalf("NONE must disable periodic export and the file writer")
	}
	p.Exporter = MetricsExporterCustom
	p.ReportDir = "/tmp/metrics"
	if p.fileExporterRequested() {
		t.Fatalf("CUSTOM must not install the file writer from report_dir")
	}
	if !p.periodicExportEnabled() {
		t.Fatalf("CUSTOM should still periodically Export addExporter targets")
	}
}

func TestSetSamplerProbability(t *testing.T) {
	p := DefaultMetricsPolicy()
	p.SetSamplerProbability(0.1)
	if p.SamplerRange != 1_000_000 || p.SamplerThreshold != 100_000 {
		t.Fatalf("got range=%d threshold=%d", p.SamplerRange, p.SamplerThreshold)
	}
}

func TestParseExportInterval(t *testing.T) {
	d, ok := parseExportInterval("30s")
	if !ok || d != 30*time.Second {
		t.Fatalf("30s: got %v ok=%v", d, ok)
	}
	d, ok = parseExportInterval("500ms")
	if !ok || d != 500*time.Millisecond {
		t.Fatalf("500ms: got %v ok=%v", d, ok)
	}
	d, ok = coerceExportInterval(30)
	if !ok || d != 30*time.Second {
		t.Fatalf("bare 30: got %v ok=%v", d, ok)
	}
}

func TestFileMetricsExporterWritesSnakeCaseHeader(t *testing.T) {
	dir := t.TempDir()
	e := NewFileMetricsExporter(dir)
	snap := &MetricsSnapshot{
		Timestamp:      time.Now(),
		ClusterName:    "c1",
		ClientType:     "go",
		ClientVersion:  "test",
		AppID:          "not-set",
		Labels:         map[string]string{"region": "us"},
		LatencyColumns: 7,
		LatencyShift:   1,
		Nodes: []NodeMetricsSnapshot{{
			NodeName:    "A",
			NodeAddress: "127.0.0.1",
			NodePort:    3000,
			Namespaces: []NamespaceMetricsSnapshot{{
				Namespace: "test",
				KeyBusy:   1,
				BytesIn:   2,
				Latencies: []LatencySnapshot{{
					Type:         LatencyRead,
					BucketCounts: []uint64{1, 0, 0, 0, 0, 0, 0},
				}},
			}},
		}},
	}
	e.Export(snap)
	e.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 metrics file, got %d", len(entries))
	}
	body, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if !strings.Contains(s, " header(4)") {
		t.Fatalf("expected header(4) snake_case v3 schema, got:\n%s", s)
	}
	if strings.Contains(s, "delay_queue_timeout_count") {
		t.Fatalf("native file must omit async delay_queue_timeout_count, got:\n%s", s)
	}
	if !strings.Contains(s, "recover_queue_size") {
		t.Fatalf("expected recover_queue_size in header, got:\n%s", s)
	}
	if !strings.Contains(s, "key_busy") || !strings.Contains(s, "bytes_in") || !strings.Contains(s, "client_type") {
		t.Fatalf("expected snake_case file schema, got:\n%s", s)
	}
	if strings.Contains(s, "keyBusy") || strings.Contains(s, "bytesIn") || strings.Contains(s, "clientType") {
		t.Fatalf("legacy camelCase must not appear in Go metrics file:\n%s", s)
	}
	if !strings.Contains(s, " cluster[") {
		t.Fatalf("expected cluster line, got:\n%s", s)
	}
	if !strings.Contains(s, "client_timeouts") || !strings.Contains(s, "cluster[name,client_type") {
		t.Fatalf("expected timeout/error splits and snake_case cluster header, got:\n%s", s)
	}
}

// FILE + ReportDir starts the export loop; file grows across ≥2 ticks.
func TestEnableMetricsFileExporterWritesPeriodically(t *testing.T) {
	dir := t.TempDir()
	clstr := newTestCluster()
	p := DefaultMetricsPolicy()
	p.Interval = 40 * time.Millisecond
	p.Exporter = MetricsExporterFile
	p.ReportDir = dir
	clstr.EnableMetrics(p)
	defer clstr.DisableMetrics()

	var firstPath string
	var firstSize int64
	deadline := time.Now().Add(2 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) >= 1 {
			firstPath = filepath.Join(dir, entries[0].Name())
			body, err := os.ReadFile(firstPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), " header(4)") {
				t.Fatalf("expected v3 header in periodic file, got:\n%s", body)
			}
			firstSize = int64(len(body))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected a metrics file within 2s of EnableMetrics(FILE)")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A later tick must append (byte size grows) or rotate a new file.
	deadline = time.Now().Add(2 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 1 {
			return
		}
		info, err := os.Stat(firstPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > firstSize {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected file growth across ≥2 export ticks; size stayed %d", firstSize)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// NONE must not install the file writer even if ReportDir is set.
func TestEnableMetricsNoneDoesNotCreateMetricsFile(t *testing.T) {
	dir := t.TempDir()
	clstr := newTestCluster()
	p := DefaultMetricsPolicy()
	p.Interval = 40 * time.Millisecond
	p.Exporter = MetricsExporterNone
	p.ReportDir = dir
	clstr.EnableMetrics(p)
	defer clstr.DisableMetrics()

	time.Sleep(250 * time.Millisecond)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("NONE must not write metrics files, got %d entries", len(entries))
	}
}

// Unwritable report path must not panic; Export records an error.
func TestFileMetricsExporterUnwritableDir(t *testing.T) {
	base := t.TempDir()
	blocker := filepath.Join(base, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	e := NewFileMetricsExporter(blocker)
	e.Export(&MetricsSnapshot{
		Timestamp:      time.Now(),
		ClusterName:    "c1",
		ClientType:     "go",
		ClientVersion:  "test",
		AppID:          "not-set",
		LatencyColumns: 7,
		LatencyShift:   1,
	})
	if e.LastExportError() == nil {
		t.Fatal("expected LastExportError when ReportDir is not a writable directory")
	}
}

// Policy labels appear in the file cluster line.
func TestFileMetricsExporterWritesLabels(t *testing.T) {
	dir := t.TempDir()
	e := NewFileMetricsExporter(dir)
	e.Export(&MetricsSnapshot{
		Timestamp:      time.Now(),
		ClusterName:    "c1",
		ClientType:     "go",
		ClientVersion:  "test",
		AppID:          "not-set",
		Labels:         map[string]string{"region": "us-west", "env": "ci"},
		LatencyColumns: 7,
		LatencyShift:   1,
	})
	e.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if !strings.Contains(s, "region") || !strings.Contains(s, "us-west") {
		t.Fatalf("expected region label in file, got:\n%s", s)
	}
	if !strings.Contains(s, "env") || !strings.Contains(s, "ci") {
		t.Fatalf("expected env label in file, got:\n%s", s)
	}
}

func TestFileMetricsExporterAppendsUsageFeatureOnClusterLine(t *testing.T) {
	dir := t.TempDir()
	e := NewFileMetricsExporter(dir)
	snap := &MetricsSnapshot{
		Timestamp:           time.Now(),
		ClusterName:         "c1",
		ClientType:          "go",
		ClientVersion:       "test",
		AppID:               "not-set",
		UsageMetricsEnabled: true,
		Usage: &UsageMetricsSnapshot{
			Blocking: 3, Deferred: 2, Background: 1,
			ShapePoint: 4, ShapeBatch: 5, ShapeQuery: 6,
			FilterAEL: 7, FilterExp: 8,
		},
		LatencyColumns: 7,
		LatencyShift:   1,
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
	s := string(body)
	if !strings.Contains(s, "feature[blocking,deferred,background,shape_point,shape_batch,shape_query,filter_ael,filter_exp]") {
		t.Fatalf("header should document feature segment, got:\n%s", s)
	}
	// Usage values sit in the cluster[...] feature[] slot (before node[]).
	if !strings.Contains(s, ",[3,2,1,4,5,6,7,8],[") {
		t.Fatalf("expected usage feature values in cluster line, got:\n%s", s)
	}
}

func newTestCluster() *Cluster {
	clstr := &Cluster{stats: map[string]*nodeStats{}}
	cp := &atomic.Pointer[ClientPolicy]{}
	cp.Store(&ClientPolicy{})
	clstr.clientPolicy = cp
	clstr.reshapeClusterLatencies(DefaultMetricsPolicy())
	return clstr
}

func TestNodesDepartedRetention(t *testing.T) {
	clstr := newTestCluster()
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "gone", NodePort: 3000})

	// On-demand includes departed but does not drain.
	snap := clstr.buildMetricsSnapshot(false)
	if len(snap.NodesDeparted) != 1 || snap.NodesDeparted[0].NodeName != "gone" {
		t.Fatalf("on-demand should include nodes_departed")
	}
	if len(clstr.departed) != 1 {
		t.Fatalf("on-demand must not drain nodes_departed")
	}

	// Flip-flop replaces the single entry.
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "gone", NodePort: 3001})
	out, overflow := clstr.peekDeparted()
	if len(out) != 1 || out[0].NodePort != 3001 || overflow {
		t.Fatalf("flip-flop should keep one updated entry, got %+v overflow=%v", out, overflow)
	}

	clstr.clearDeparted()
	if len(clstr.departed) != 0 || clstr.departedOverflow {
		t.Fatalf("clearDeparted should empty the set")
	}
}

func TestNodesDepartedSoftCap(t *testing.T) {
	clstr := newTestCluster()
	for i := 0; i < maxNodesDeparted; i++ {
		clstr.storeDeparted(NodeMetricsSnapshot{NodeName: fmt.Sprintf("n%d", i)})
	}
	clstr.storeDeparted(NodeMetricsSnapshot{NodeName: "overflow"})

	_, overflow := clstr.peekDeparted()
	if !overflow {
		t.Fatalf("expected possibly-more signal at soft-cap")
	}
	if len(clstr.departed) != maxNodesDeparted {
		t.Fatalf("set must not grow past soft-cap, got %d", len(clstr.departed))
	}
	snap := clstr.buildMetricsSnapshot(false)
	if !snap.NodesDepartedPossiblyMore || len(snap.NodesDeparted) != maxNodesDeparted {
		t.Fatalf("snapshot should carry overflow and capped set")
	}
}

func TestValidationAndNodeConnectionFailures(t *testing.T) {
	clstr := &Cluster{}
	nd := &Node{cluster: clstr}

	// Behind operational: no increments while off.
	clstr.recordValidationNewConnectionFailure(newError(types.NETWORK_ERROR, "dial"))
	nd.recordNewConnectionFailure(newError(types.NETWORK_ERROR, "dial"))
	if clstr.validationOpenFailure.Get() != 0 || nd.stats.ConnectionOpenFailure.Get() != 0 {
		t.Fatalf("failures must not record when operational is off")
	}

	clstr.operationalEnabled.Store(true)

	// Pre-Node validation home.
	clstr.recordValidationNewConnectionFailure(newError(types.NETWORK_ERROR, "dial"))
	clstr.recordValidationNewConnectionFailure(newTLSHandshakeNetworkError(errors.New("handshake")))
	clstr.recordValidationAuthFailure()
	// dial=1 open; tls=+1 tls +1 open rollup; auth=+1 auth +1 open rollup → open=3, tls=1, auth=1
	if clstr.validationOpenFailure.Get() != 3 {
		t.Fatalf("validation open (with rollup): got %d", clstr.validationOpenFailure.Get())
	}
	if clstr.validationTlsHandshakeFailure.Get() != 1 {
		t.Fatalf("validation tls: got %d", clstr.validationTlsHandshakeFailure.Get())
	}
	if clstr.validationAuthFailure.Get() != 1 {
		t.Fatalf("validation auth: got %d", clstr.validationAuthFailure.Get())
	}

	// Existing-Node home (one home only — validation counters unchanged).
	nd.recordNewConnectionFailure(newError(types.NETWORK_ERROR, "dial"))
	nd.recordNewConnectionFailure(newTLSHandshakeNetworkError(errors.New("handshake")))
	nd.recordConnectionAuthFailure()
	if nd.stats.ConnectionOpenFailure.Get() != 3 {
		t.Fatalf("node open (with rollup): got %d", nd.stats.ConnectionOpenFailure.Get())
	}
	if nd.stats.ConnectionTlsHandshakeFailure.Get() != 1 || nd.stats.ConnectionAuthFailure.Get() != 1 {
		t.Fatalf("node tls/auth: %d/%d", nd.stats.ConnectionTlsHandshakeFailure.Get(), nd.stats.ConnectionAuthFailure.Get())
	}
	if clstr.validationOpenFailure.Get() != 3 {
		t.Fatalf("node failures must not touch validation counters")
	}
}

func TestIsConnectionAuthFailure(t *testing.T) {
	if !isConnectionAuthFailure(newError(types.INVALID_CREDENTIAL, "bad")) {
		t.Fatal("INVALID_CREDENTIAL should be auth")
	}
	if !isConnectionAuthFailure(newError(types.NOT_AUTHENTICATED)) {
		t.Fatal("NOT_AUTHENTICATED should be auth")
	}
	if isConnectionAuthFailure(newError(types.NETWORK_ERROR, "dial")) {
		t.Fatal("NETWORK_ERROR during login must not count as auth")
	}
	if isConnectionAuthFailure(newError(types.TIMEOUT, "login timeout")) {
		t.Fatal("TIMEOUT during login must not count as auth")
	}
}

func TestNormalizeZeroValueMetricsPolicy(t *testing.T) {
	p := normalizeMetricsPolicy(nil)
	if !p.OperationalEnabled || p.LatencyColumns != 7 {
		t.Fatalf("nil should map to DefaultMetricsPolicy, got %+v", p)
	}
	p = normalizeMetricsPolicy(&MetricsPolicy{})
	if !p.OperationalEnabled || p.LatencyColumns != 7 || p.UsageEnabled {
		t.Fatalf("zero-value should map to DefaultMetricsPolicy, got %+v", p)
	}
	// Explicit operational-off with configured columns must not be rewritten.
	custom := &MetricsPolicy{LatencyColumns: 7, LatencyShift: 1, LatencyBase: 2, OperationalEnabled: false}
	p = normalizeMetricsPolicy(custom)
	if p.OperationalEnabled || p.LatencyColumns != 7 {
		t.Fatalf("configured policy must be preserved, got %+v", p)
	}
}

func TestRecoverQueueSizeTracksInFlightSalvage(t *testing.T) {
	clstr := &Cluster{}
	if clstr.recoverQueueSize() != 0 {
		t.Fatal("expected 0")
	}
	clstr.beginConnectionRecover()
	clstr.beginConnectionRecover()
	if clstr.recoverQueueSize() != 2 {
		t.Fatalf("got %d", clstr.recoverQueueSize())
	}
	clstr.endConnectionRecover()
	if clstr.recoverQueueSize() != 1 {
		t.Fatalf("got %d", clstr.recoverQueueSize())
	}
	clstr.endConnectionRecover()
	clstr.endConnectionRecover() // underflow guard
	if clstr.recoverQueueSize() != 0 {
		t.Fatalf("got %d", clstr.recoverQueueSize())
	}
	snap := clstr.buildMetricsSnapshot(false)
	if snap.RecoverQueueSize != 0 {
		t.Fatalf("snapshot recover_queue_size=%d", snap.RecoverQueueSize)
	}
}

func TestResetMetricsSinceEnable(t *testing.T) {
	clstr := &Cluster{}
	nd := &Node{cluster: clstr}
	nd.stats.ConnectionsSuccessful.Set(9)
	nd.stats.ConnectionsClosed.Set(4)
	nd.stats.ConnectionsIdleDropped.Set(7)
	nd.stats.ConnectionsClosedError.Set(3)
	nd.stats.ConnectionsPoolOverflow.Set(2)
	nd.stats.ConnectionsClosedNodeRemoved.Set(5)
	nd.stats.ConnectionsPoolExhausted.Set(1)
	clstr.commandCount.Set(100)
	clstr.usageBlocking.Set(50)
	clstr.nodes.Set([]*Node{nd})

	clstr.resetMetricsSinceEnable()
	if nd.stats.ConnectionsSuccessful.Get() != 0 || nd.stats.ConnectionsClosed.Get() != 0 {
		t.Fatalf("opened/closed should reset at enable, got %d/%d",
			nd.stats.ConnectionsSuccessful.Get(), nd.stats.ConnectionsClosed.Get())
	}
	if nd.stats.ConnectionsIdleDropped.Get() != 0 || nd.stats.ConnectionsClosedError.Get() != 0 ||
		nd.stats.ConnectionsPoolOverflow.Get() != 0 || nd.stats.ConnectionsClosedNodeRemoved.Get() != 0 ||
		nd.stats.ConnectionsPoolExhausted.Get() != 0 {
		t.Fatalf("close-reason/pool counters should reset at enable")
	}
	if clstr.commandCount.Get() != 0 || clstr.usageBlocking.Get() != 0 {
		t.Fatalf("cluster enable-scoped counters should reset")
	}
}

func TestBatchRecordFilterExpression(t *testing.T) {
	exp := ExpEq(ExpIntBin("a"), ExpIntVal(1))
	br := NewBatchRead(&BatchReadPolicy{FilterExpression: exp}, nil, nil)
	if batchRecordFilterExpression(br) != exp {
		t.Fatal("expected BatchRead policy filter")
	}
	bw := NewBatchWrite(&BatchWritePolicy{FilterExpression: exp}, nil)
	if batchRecordFilterExpression(bw) != exp {
		t.Fatal("expected BatchWrite policy filter")
	}
	if firstBatchRecordFilter([]BatchRecordIfc{NewBatchRead(nil, nil, nil), br}) != exp {
		t.Fatal("expected first non-nil per-record filter")
	}
}

func TestMaybeBatchPartialFailure(t *testing.T) {
	clstr := &Cluster{}
	clstr.operationalEnabled.Store(true)
	maybeBatchPartialFailure(clstr, types.OK)
	maybeBatchPartialFailure(clstr, types.KEY_NOT_FOUND_ERROR)
	maybeBatchPartialFailure(clstr, types.FILTERED_OUT)
	maybeBatchPartialFailure(clstr, types.NO_RESPONSE)
	if clstr.batchPartialFailure.Get() != 0 {
		t.Fatalf("benign codes must not count, got %d", clstr.batchPartialFailure.Get())
	}
	maybeBatchPartialFailure(clstr, types.TIMEOUT)
	maybeBatchPartialFailure(clstr, types.SERVER_ERROR)
	if clstr.batchPartialFailure.Get() != 2 {
		t.Fatalf("expected 2 partial failures, got %d", clstr.batchPartialFailure.Get())
	}
}

// DisableMetrics stops periodic Export.
func TestDisableMetricsStopsPeriodicExport(t *testing.T) {
	clstr := newTestCluster()
	rec := &recordingExporter{}
	p := DefaultMetricsPolicy()
	p.Interval = 40 * time.Millisecond
	p.Exporter = MetricsExporterCustom
	p.AddExporter(rec)
	clstr.EnableMetrics(p)

	deadline := time.Now().Add(2 * time.Second)
	for {
		rec.mu.Lock()
		n := rec.count
		rec.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected at least one Export before disable")
		}
		time.Sleep(20 * time.Millisecond)
	}

	clstr.DisableMetrics()
	rec.mu.Lock()
	afterDisable := rec.count
	rec.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	rec.mu.Lock()
	later := rec.count
	rec.mu.Unlock()
	if later != afterDisable {
		t.Fatalf("Export must stop after DisableMetrics: %d → %d", afterDisable, later)
	}
	if clstr.MetricsEnabled() || clstr.decideOperationalSample() {
		t.Fatal("collection and sampler must be off after DisableMetrics")
	}
}

// Latency sum must be present when count > 0.
func TestClusterLatencySumPresentWhenCountPositive(t *testing.T) {
	clstr := newTestCluster()
	clstr.metricsEnabled.Store(true)
	clstr.operationalEnabled.Store(true)
	clstr.metricsPolicy.Set(DefaultMetricsPolicy())
	clstr.addClusterLatency(LatencyRead, 5*time.Millisecond)

	snap := clstr.buildMetricsSnapshot(false)
	var found bool
	for _, lat := range snap.Latencies {
		if lat.Type != LatencyRead {
			continue
		}
		found = true
		if lat.Count < 1 {
			t.Fatalf("expected read count ≥ 1, got %d", lat.Count)
		}
		if lat.Sum <= 0 {
			t.Fatalf("sum must be present when count > 0, got sum=%v count=%d", lat.Sum, lat.Count)
		}
	}
	if !found {
		t.Fatal("expected LatencyRead in cluster latencies")
	}
}

// Usage feature.api.* / shape are not sampler-gated.
func TestRecordUsageIncrementsFeatureAPI(t *testing.T) {
	clstr := newTestCluster()
	clstr.metricsEnabled.Store(true)
	clstr.usageEnabled.Store(true)
	clstr.recordUsage(ttGet, false, nil)
	clstr.recordUsage(ttBatchRead, false, nil)

	snap := clstr.buildMetricsSnapshot(false)
	if snap.Usage == nil {
		t.Fatal("expected usage snapshot when usage enabled")
	}
	if snap.Usage.Blocking != 2 || snap.Usage.ShapePoint != 1 || snap.Usage.ShapeBatch != 1 {
		t.Fatalf("usage counters: blocking=%d point=%d batch=%d",
			snap.Usage.Blocking, snap.Usage.ShapePoint, snap.Usage.ShapeBatch)
	}
}

// Errors per failed attempt; latency/bytes on success only; command.count once.
func TestErrorLatencyAndCommandCountSemantics(t *testing.T) {
	clstr := newTestCluster()
	clstr.metricsEnabled.Store(true)
	clstr.operationalEnabled.Store(true)
	clstr.metricsPolicy.Set(DefaultMetricsPolicy())

	ns := newNodeStats(DefaultMetricsPolicy())
	// Two failed attempts (e.g. timeout then retry), then success path records latency/bytes once.
	ns.addSharedError("test", types.TIMEOUT, true)
	ns.addSharedError("test", types.TIMEOUT, true)
	op := ns.getOrCreateNsOp("test")
	if op.ClientTimeouts.Get() != 2 {
		t.Fatalf("errors must increment per failed attempt, got client_timeouts=%d", op.ClientTimeouts.Get())
	}
	if op.Latencies[LatencyRead].Clone().Count != 0 || op.BytesIn.Get() != 0 {
		t.Fatal("latency/bytes must stay 0 until success")
	}

	ns.addSharedLatency("test", LatencyRead, 3)
	ns.addSharedBytes("test", 10, 4)
	c := op.Latencies[LatencyRead].Clone()
	if c.Count != 1 || c.Sum <= 0 || op.BytesIn.Get() != 10 || op.BytesOut.Get() != 4 {
		t.Fatalf("success records one latency sample and bytes, got count=%d sum=%v in=%d out=%d",
			c.Count, c.Sum, op.BytesIn.Get(), op.BytesOut.Get())
	}

	// One sampled call → command.count once; one retry → command.retries once.
	clstr.recordCommandCount()
	clstr.recordCommandRetry()
	if clstr.commandCount.Get() != 1 || clstr.commandRetries.Get() != 1 {
		t.Fatalf("command.count=%d retries=%d", clstr.commandCount.Get(), clstr.commandRetries.Get())
	}
}

// Export must not reset cumulative counters.
func TestExportDoesNotResetCommandCount(t *testing.T) {
	clstr := newTestCluster()
	rec := &recordingExporter{}
	p := DefaultMetricsPolicy()
	p.Interval = time.Hour // avoid timer noise; we call fanOutExport directly
	p.Exporter = MetricsExporterCustom
	p.AddExporter(rec)
	clstr.EnableMetrics(p)
	defer clstr.DisableMetrics()

	clstr.commandCount.Set(42)
	snap := clstr.buildMetricsSnapshot(true)
	if !clstr.fanOutExport(snap) {
		t.Fatal("expected successful export")
	}
	if clstr.commandCount.Get() != 42 {
		t.Fatalf("Export must not reset command.count, got %d", clstr.commandCount.Get())
	}
	snap2 := clstr.buildMetricsSnapshot(false)
	if snap2.CommandCount != 42 {
		t.Fatalf("on-demand snapshot must still see 42, got %d", snap2.CommandCount)
	}
}

// File rotate: soft size limit opens a new metrics-*.log (bypass SetReportSizeLimit floor for the test).
// maxSize must exceed one header line — otherwise openNewFile→writeLine recurses forever
// (production SetReportSizeLimit floors at 1MB, so this only matters in tests).
func TestFileMetricsExporterRotatesAtSizeLimit(t *testing.T) {
	dir := t.TempDir()
	e := NewFileMetricsExporter(dir)
	headerLen := len(formatMetricsFileHeader(7, 1, LatencyMilliseconds)) + 1
	e.maxSize = int64(headerLen + 80) // room for header + one short cluster line, then rotate
	snap := &MetricsSnapshot{
		Timestamp:      time.Now(),
		ClusterName:    "c1",
		ClientType:     "go",
		ClientVersion:  "test",
		AppID:          "not-set",
		LatencyColumns: 7,
		LatencyShift:   1,
	}
	for i := 0; i < 10; i++ {
		snap.Timestamp = time.Now()
		snap.CommandCount = int64(i)
		e.Export(snap)
		if e.LastExportError() != nil {
			t.Fatalf("export %d: %v", i, e.LastExportError())
		}
	}
	e.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected rotate to create ≥2 files, got %d", len(entries))
	}
}

// Disable closes the client-owned file writer; re-enable reopens on the next Export.
func TestDisableMetricsClosesFileWriterReopensOnEnable(t *testing.T) {
	dir := t.TempDir()
	clstr := newTestCluster()
	p := DefaultMetricsPolicy()
	p.Interval = 40 * time.Millisecond
	p.Exporter = MetricsExporterFile
	p.ReportDir = dir
	clstr.EnableMetrics(p)

	deadline := time.Now().Add(2 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected initial metrics file")
		}
		time.Sleep(20 * time.Millisecond)
	}
	fe := clstr.fileExporter
	if fe == nil || !fe.open {
		t.Fatal("expected open file exporter while enabled")
	}

	clstr.DisableMetrics()
	if fe.open || fe.file != nil {
		t.Fatal("DisableMetrics must close the file writer")
	}

	entriesAfterDisable, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	nAfterDisable := len(entriesAfterDisable)

	clstr.EnableMetrics(p)
	defer clstr.DisableMetrics()
	deadline = time.Now().Add(2 * time.Second)
	for {
		if clstr.fileExporter != nil && clstr.fileExporter.open {
			break
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		// New file or reopen that appends both count as reopen.
		if len(entries) > nAfterDisable || (clstr.fileExporter != nil && clstr.fileExporter.open) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected file writer to reopen after re-enable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if clstr.fileExporter == nil || !clstr.fileExporter.open {
		t.Fatal("file exporter must be open after re-enable Export")
	}
}

// Conn latency is operational but not sampler-gated.
func TestConnLatencyNotSamplerGated(t *testing.T) {
	clstr := newTestCluster()
	p := DefaultMetricsPolicy()
	p.SetSamplerNever()
	clstr.EnableMetrics(p)
	defer clstr.DisableMetrics()

	if clstr.decideOperationalSample() {
		t.Fatal("sampler never must not sample commands")
	}
	if !clstr.operationalEnabled.Load() {
		t.Fatal("operational must remain on")
	}

	nd := &Node{cluster: clstr}
	nd.stats = *newNodeStats(p)
	nd.stats.addConnLatency(p.elapsedUnits(3 * time.Millisecond))
	snap := nd.stats.snapshotConnLatency()
	if snap.Count != 1 || snap.Sum <= 0 {
		t.Fatalf("conn latency must record while sampler is never: count=%d sum=%v", snap.Count, snap.Sum)
	}
}

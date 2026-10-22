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
	if !strings.Contains(s, " header(3)") {
		t.Fatalf("expected header(3) snake_case v3 schema, got:\n%s", s)
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
		t.Fatalf("expected CM-7 splits and snake_case cluster header, got:\n%s", s)
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

func TestResetOpenedClosedSinceEnable(t *testing.T) {
	clstr := &Cluster{}
	nd := &Node{cluster: clstr}
	nd.stats.ConnectionsSuccessful.Set(9)
	nd.stats.ConnectionsClosed.Set(4)
	clstr.nodes.Set([]*Node{nd})

	clstr.resetOpenedClosedSinceEnable()
	if nd.stats.ConnectionsSuccessful.Get() != 0 || nd.stats.ConnectionsClosed.Get() != 0 {
		t.Fatalf("opened/closed should reset at enable, got %d/%d",
			nd.stats.ConnectionsSuccessful.Get(), nd.stats.ConnectionsClosed.Get())
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

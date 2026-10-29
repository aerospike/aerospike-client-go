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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const metricsFileMinSize = 1_000_000

// FileMetricsExporter is the client-owned metrics log writer installed when
// MetricsPolicy.Exporter is FILE and ReportDir is set (YAML metrics.exporter: FILE).
//
// This client never shipped a learn-metrics file before, so the format is v3
// snake_case end-to-end (same naming as dynamic config / Rust snapshot JSON) and
// includes cluster.latency.*, usage feature.*, CM-7 timeout/error splits, and
// CM-8 validation counters. Legacy Java/C# camelCase positional grammar and
// UX-6 major-release deferrals do not apply here.
//
// Close/flush on disable; reopen lazily on the next Export after re-enable.
type FileMetricsExporter struct {
	dir string

	mu      sync.Mutex
	file    *os.File
	size    int64
	maxSize int64
	columns int
	shift   int
	unit    LatencyUnit
	open    bool
	lastErr error
}

// NewFileMetricsExporter creates a file exporter for dir. An empty dir is invalid;
// the cluster will not install a file exporter when ReportDir is empty.
func NewFileMetricsExporter(dir string) *FileMetricsExporter {
	return &FileMetricsExporter{dir: dir}
}

func (e *FileMetricsExporter) Export(snapshot *MetricsSnapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastErr = nil
	if snapshot == nil {
		return
	}

	if err := e.ensureOpen(snapshot); err != nil {
		e.lastErr = err
		return
	}

	line := e.formatClusterLine(snapshot)
	if err := e.writeLine(line); err != nil {
		e.lastErr = err
		return
	}

	for i := range snapshot.NodesDeparted {
		nline := e.formatNodeLine(&snapshot.NodesDeparted[i], snapshot)
		if err := e.writeLine(nline); err != nil {
			e.lastErr = err
			return
		}
	}
}

// LastExportError returns the error from the most recent Export, if any.
// Used by the metrics thread to decide whether nodes_departed may be drained.
func (e *FileMetricsExporter) LastExportError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastErr
}

// Close flushes and closes the current file. Called when metrics are disabled
// or the cluster is closed. The next Export after re-enable reopens a new file.
func (e *FileMetricsExporter) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeLocked()
}

func (e *FileMetricsExporter) ensureOpen(snapshot *MetricsSnapshot) error {
	if snapshot != nil {
		shapeChanged := e.open && (e.columns != snapshot.LatencyColumns ||
			e.shift != snapshot.LatencyShift ||
			e.unit != snapshot.LatencyUnit)
		e.columns = snapshot.LatencyColumns
		e.shift = snapshot.LatencyShift
		e.unit = snapshot.LatencyUnit
		if shapeChanged {
			// New header must match bucket layout / unit; rotate the file.
			return e.openNewFile()
		}
	}
	if e.open && e.file != nil {
		return nil
	}
	if e.dir == "" {
		return fmt.Errorf("report_dir is empty")
	}
	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		return err
	}
	return e.openNewFile()
}

func (e *FileMetricsExporter) openNewFile() error {
	e.closeLocked()

	stamp := time.Now().Format("20060102150405")
	path := filepath.Join(e.dir, "metrics-"+stamp+".log")
	// Avoid appending to a same-second file after a reshape rotate.
	for seq := 1; ; seq++ {
		_, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		path = filepath.Join(e.dir, fmt.Sprintf("metrics-%s-%d.log", stamp, seq))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	e.file = f
	e.size = 0
	e.open = true

	header := formatMetricsFileHeader(e.columns, e.shift, e.unit)
	return e.writeLine(header)
}

func (e *FileMetricsExporter) closeLocked() {
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	e.open = false
	e.size = 0
}

func (e *FileMetricsExporter) writeLine(s string) error {
	if e.file == nil {
		return fmt.Errorf("file not open")
	}
	n, err := e.file.WriteString(s + "\n")
	if err != nil {
		e.closeLocked()
		return err
	}
	if err := e.file.Sync(); err != nil {
		logger.Logger.Debug("metrics file exporter sync: %s", err.Error())
	}
	e.size += int64(n)
	if e.maxSize > 0 && e.size >= e.maxSize {
		return e.openNewFile()
	}
	return nil
}

func (e *FileMetricsExporter) SetReportSizeLimit(limit int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if limit != 0 && limit < metricsFileMinSize {
		limit = metricsFileMinSize
	}
	e.maxSize = limit
}

func formatMetricsFileHeader(columns, shift int, unit LatencyUnit) string {
	var b strings.Builder
	b.WriteString(time.Now().Format("2006-01-02 15:04:05.000"))
	// header(3): first Go metrics-file schema (snake_case v3; no prior shippers).
	b.WriteString(" header(3)")
	b.WriteString(" cluster[name,client_type,client_version,app_id,label[],cpu,mem,recover_queue_size,invalid_node_count,command_count,command_retries,delay_queue_timeout_count,batch_partial_failure,validation_open_failure,validation_tls_handshake_failure,validation_auth_failure,latency[],feature[],node[]]")
	b.WriteString(" feature[blocking,deferred,background,shape_point,shape_batch,shape_query,filter_ael,filter_exp]")
	b.WriteString(" label[name,value]")
	b.WriteString(" node[name,address,port,conn,connection_open_failure,connection_tls_handshake_failure,connection_auth_failure,conn_latency,namespace[]]")
	b.WriteString(" conn[in_use,in_pool,opened,closed]")
	b.WriteString(" namespace[name,errors,client_timeouts,server_timeouts,key_busy,record_too_big,device_overload,bytes_in,bytes_out,latency[]]")
	fmt.Fprintf(&b, " latency(%d,%d,%s)[type[l1,l2,l3...]]", columns, shift, unit.String())
	return b.String()
}

func (e *FileMetricsExporter) formatClusterLine(s *MetricsSnapshot) string {
	var b strings.Builder
	b.WriteString(s.Timestamp.Format("2006-01-02 15:04:05.000"))
	b.WriteString(" cluster[")
	b.WriteString(escapeMetricsToken(s.ClusterName))
	b.WriteByte(',')
	b.WriteString(escapeMetricsToken(s.ClientType))
	b.WriteByte(',')
	b.WriteString(escapeMetricsToken(s.ClientVersion))
	b.WriteByte(',')
	b.WriteString(escapeMetricsToken(s.AppID))
	b.WriteByte(',')
	writeLabels(&b, s.Labels)
	fmt.Fprintf(&b, ",%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,",
		int(s.CpuPercent),
		s.MemoryBytes,
		s.RecoverQueueSize,
		s.InvalidNodeCount,
		s.CommandCount,
		s.CommandRetries,
		s.DelayQueueTimeoutCount,
		s.BatchPartialFailure,
		s.ValidationOpenFailure,
		s.ValidationTlsHandshakeFailure,
		s.ValidationAuthFailure,
	)
	writeClusterLatencies(&b, s)
	b.WriteByte(',')
	writeFeatureSegment(&b, s)
	b.WriteByte(',')
	b.WriteByte('[')
	for i := range s.Nodes {
		if i > 0 {
			b.WriteByte(',')
		}
		writeNode(&b, &s.Nodes[i], s)
	}
	b.WriteString("]]")
	return b.String()
}

func (e *FileMetricsExporter) formatNodeLine(n *NodeMetricsSnapshot, s *MetricsSnapshot) string {
	var b strings.Builder
	b.WriteString(s.Timestamp.Format("2006-01-02 15:04:05.000"))
	b.WriteString(" node")
	writeNode(&b, n, s)
	return b.String()
}

func writeFeatureSegment(b *strings.Builder, s *MetricsSnapshot) {
	if !s.UsageMetricsEnabled || s.Usage == nil {
		b.WriteString("[]")
		return
	}
	u := s.Usage
	fmt.Fprintf(b, "[%d,%d,%d,%d,%d,%d,%d,%d]",
		u.Blocking, u.Deferred, u.Background,
		u.ShapePoint, u.ShapeBatch, u.ShapeQuery,
		u.FilterAEL, u.FilterExp)
}

func writeClusterLatencies(b *strings.Builder, s *MetricsSnapshot) {
	b.WriteByte('[')
	first := true
	for _, lat := range s.Latencies {
		if lat.Type == LatencyConn {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		writeLatencyBuckets(b, lat, s.LatencyColumns)
	}
	b.WriteByte(']')
}

func writeLabels(b *strings.Builder, labels map[string]string) {
	if len(labels) == 0 {
		b.WriteString("[]")
		return
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteByte('[')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(b, "[%s,%s]", escapeMetricsToken(k), escapeMetricsToken(labels[k]))
	}
	b.WriteByte(']')
}

func writeNode(b *strings.Builder, n *NodeMetricsSnapshot, s *MetricsSnapshot) {
	fmt.Fprintf(b, "[%s,%s,%d,",
		escapeMetricsToken(n.NodeName),
		escapeMetricsToken(n.NodeAddress),
		n.NodePort)
	writeConn(b, n.Connections)
	fmt.Fprintf(b, ",%d,%d,%d,",
		n.ConnectionOpenFailure,
		n.ConnectionTlsHandshakeFailure,
		n.ConnectionAuthFailure)
	writeBucketCounts(b, n.ConnLatency.BucketCounts, s.LatencyColumns)
	b.WriteString(",[")
	first := true
	for i := range n.Namespaces {
		if n.Namespaces[i].Namespace == "" || n.Namespaces[i].Namespace == "_" {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		writeNamespace(b, &n.Namespaces[i], s)
	}
	b.WriteString("]]") // close namespace[] and node
}

func writeConn(b *strings.Builder, c ConnectionMetricsSnapshot) {
	fmt.Fprintf(b, "%d,%d,%d,%d", c.InUse, c.InPool, c.Opened, c.Closed)
}

func writeNamespace(b *strings.Builder, ns *NamespaceMetricsSnapshot, s *MetricsSnapshot) {
	fmt.Fprintf(b, "[%s,%d,%d,%d,%d,%d,%d,%d,%d,[",
		escapeMetricsToken(ns.Namespace),
		ns.Errors,
		ns.ClientTimeouts,
		ns.ServerTimeouts,
		ns.KeyBusy,
		ns.RecordTooBig,
		ns.DeviceOverload,
		ns.BytesIn,
		ns.BytesOut)
	first := true
	for _, lat := range ns.Latencies {
		if lat.Type == LatencyConn {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		writeLatencyBuckets(b, lat, s.LatencyColumns)
	}
	b.WriteString("]]") // close latency[] and namespace
}

func writeLatencyBuckets(b *strings.Builder, lat LatencySnapshot, columns int) {
	b.WriteString(lat.Type.String())
	writeBucketCounts(b, lat.BucketCounts, columns)
}

func writeBucketCounts(b *strings.Builder, counts []uint64, columns int) {
	b.WriteByte('[')
	if len(counts) == 0 {
		counts = make([]uint64, columns)
	}
	for j, c := range counts {
		if j > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(b, "%d", c)
	}
	b.WriteByte(']')
}

// escapeMetricsToken keeps positional segment tokens free of ',' / '[' / ']'.
func escapeMetricsToken(s string) string {
	if s == "" {
		return s
	}
	if !strings.ContainsAny(s, ",[]") {
		return s
	}
	r := strings.NewReplacer(",", "_", "[", "_", "]", "_")
	return r.Replace(s)
}

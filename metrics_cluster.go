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
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// decideOperationalSample is the command-path sampler gate (one decision per
// user API call / sub-batch, drawn before the retry loop). Connection-path
// CM-8 failures use operationalEnabled alone — they are not sampler-gated.
//
// Spec: draw = next_random_uint64; metrics_on when (draw % range) < threshold.
func (clstr *Cluster) decideOperationalSample() bool {
	if !clstr.operationalEnabled.Load() {
		return false
	}
	policy := clstr.metricsPolicy.Get()
	if policy == nil || policy.SamplerThreshold <= 0 {
		return false
	}
	r := policy.samplerRange()
	// Full-rate sample (default range=1, threshold=1): skip the RNG.
	if policy.SamplerThreshold >= r {
		return true
	}
	return int(rand.Uint64()%uint64(r)) < policy.SamplerThreshold
}

func (clstr *Cluster) recordUsage(ct commandType, background bool, filter *Expression) {
	// Caller already checked metricsEnabled and skipped non data-plane commands.
	// Usage is off by default. P1: feature.api.*; P2/CM-27: shape + filter.
	if !clstr.usageEnabled.Load() {
		return
	}
	if background {
		clstr.usageBackground.IncrementAndGet()
		// Background jobs are not point/batch/query shape at the public API.
	} else {
		switch ct {
		case ttQuery, ttScan:
			clstr.usageDeferred.IncrementAndGet()
			clstr.usageShapeQuery.IncrementAndGet()
		case ttBatchRead, ttBatchWrite:
			clstr.usageBlocking.IncrementAndGet()
			clstr.usageShapeBatch.IncrementAndGet()
		default:
			clstr.usageBlocking.IncrementAndGet()
			clstr.usageShapePoint.IncrementAndGet()
		}
	}
	if filter != nil {
		if filter.isAELFilter() {
			clstr.usageFilterAEL.IncrementAndGet()
		} else {
			clstr.usageFilterExp.IncrementAndGet()
		}
	}
}

// addClusterLatency records cluster.latency.{read,write,batch,query} on call success.
// LatencyConn is node-scoped (node.latency.conn); do not record it here.
func (clstr *Cluster) addClusterLatency(lt LatencyType, d time.Duration) {
	if clstr == nil || lt == LatencyConn || lt < 0 || lt >= latencyTypeMax {
		return
	}
	policy := clstr.metricsPolicy.Get()
	if policy == nil {
		return
	}
	clstr.clusterLatencies[lt].Add(policy.elapsedUnits(d))
}

func (clstr *Cluster) reshapeClusterLatencies(policy *MetricsPolicy) {
	if policy == nil {
		policy = DefaultMetricsPolicy()
	}
	cols, shift := policy.LatencyColumns, policy.LatencyShift
	if cols < 1 {
		cols = defaultLatencyColumns
	}
	if shift < 1 {
		shift = defaultLatencyShift
	}
	unitChanged := clstr.latencyUnit != policy.LatencyUnit
	clstr.latencyUnit = policy.LatencyUnit
	for i := LatencyRead; i < latencyTypeMax; i++ {
		if unitChanged {
			// Reshape no-ops when columns/shift match; unit change still needs a reset.
			clstr.clusterLatencies[i].Reset()
		}
		clstr.clusterLatencies[i].Reshape(cols, shift)
	}
}

func (clstr *Cluster) snapshotClusterLatencies() []LatencySnapshot {
	out := make([]LatencySnapshot, latencyTypeMax)
	for i := range clstr.clusterLatencies {
		c := clstr.clusterLatencies[i].Clone()
		out[i] = LatencySnapshot{
			Type:         LatencyType(i),
			BucketCounts: append([]uint64(nil), c.Buckets...),
			Min:          c.Min,
			Max:          c.Max,
			Sum:          c.Sum,
			Count:        c.Count,
		}
	}
	return out
}

func (clstr *Cluster) recordBatchPartialFailure() {
	if clstr == nil || !clstr.operationalEnabled.Load() {
		return
	}
	clstr.batchPartialFailure.IncrementAndGet()
}

func (clstr *Cluster) recordCommandCount() {
	clstr.commandCount.IncrementAndGet()
}

func (clstr *Cluster) recordCommandRetry() {
	clstr.commandRetries.IncrementAndGet()
}

// recordValidationNewConnectionFailure records a pre-Node seed/NodeValidator
// open or TLS failure. open.failure rolls up TLS. Not sampler-gated.
func (clstr *Cluster) recordValidationNewConnectionFailure(err Error) {
	if clstr == nil || !clstr.operationalEnabled.Load() {
		return
	}
	if isTLSHandshakeFailure(err) {
		clstr.validationTlsHandshakeFailure.IncrementAndGet()
		clstr.validationOpenFailure.IncrementAndGet() // rollup
		return
	}
	clstr.validationOpenFailure.IncrementAndGet()
}

// recordValidationAuthFailure records a pre-Node auth failure; open.failure rolls up.
func (clstr *Cluster) recordValidationAuthFailure() {
	if clstr == nil || !clstr.operationalEnabled.Load() {
		return
	}
	clstr.validationAuthFailure.IncrementAndGet()
	clstr.validationOpenFailure.IncrementAndGet() // rollup
}

// isConnectionAuthFailure reports whether err is an authentication result (CM-8
// connection.auth.failure / validation.auth.failure). Network and TLS failures
// during login are open/tls failures, not auth.
func isConnectionAuthFailure(err Error) bool {
	if err == nil {
		return false
	}
	return err.Matches(
		types.INVALID_USER,
		types.INVALID_PASSWORD,
		types.EXPIRED_PASSWORD,
		types.FORBIDDEN_PASSWORD,
		types.INVALID_CREDENTIAL,
		types.EXPIRED_SESSION,
		types.NOT_AUTHENTICATED,
		types.SECURITY_NOT_ENABLED,
	)
}

// resetOpenedClosedSinceEnable zeros Tier 0 opened/closed counters so snapshots
// report cumulative values since the current EnableMetrics (collection spec).
func (clstr *Cluster) resetOpenedClosedSinceEnable() {
	if clstr == nil {
		return
	}
	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()
	for _, stat := range clstr.stats {
		if stat != nil {
			stat.ConnectionsSuccessful.Set(0)
			stat.ConnectionsClosed.Set(0)
		}
	}
	for _, node := range clstr.GetNodes() {
		if node != nil {
			node.stats.ConnectionsSuccessful.Set(0)
			node.stats.ConnectionsClosed.Set(0)
		}
	}
}

func (clstr *Cluster) appID() string {
	if clstr.clientPolicy != nil {
		if cp := clstr.clientPolicy.Load(); cp != nil && cp.ApplicationId != "" {
			return cp.ApplicationId
		}
	}
	if clstr.user != "" {
		return clstr.user
	}
	return "not-set"
}

func (clstr *Cluster) staticLabels() map[string]string {
	out := map[string]string{}
	mp := clstr.metricsPolicy.Get()
	if mp != nil && mp.Labels != nil {
		for _, m := range *mp.Labels {
			maps.Copy(out, m)
		}
	}
	return out
}

func (clstr *Cluster) installExporters(policy *MetricsPolicy) {
	clstr.metricsMu.Lock()
	defer clstr.metricsMu.Unlock()

	token := policy.exporterToken()

	// Merge previously registered app exporters with this policy's list so
	// FILE/NONE toggles cannot drop AddExporter targets (re-enable rules).
	existingApp := make([]*registeredExporter, 0, len(clstr.exporters)+len(clstr.appExporters))
	for _, e := range clstr.appExporters {
		if e != nil && !e.clientOwnedFile {
			existingApp = append(existingApp, e)
		}
	}
	for _, e := range clstr.exporters {
		if e != nil && !e.clientOwnedFile {
			existingApp = append(existingApp, e)
		}
	}

	// Dedupe by pointer identity. Avoid interface == / map keys: value types
	// containing maps/slices are unhashable and panic on comparison.
	seenPtr := map[uintptr]struct{}{}
	noteSeen := func(e MetricsExporter) bool {
		p := exporterPointer(e)
		if p == 0 {
			return false
		}
		if _, ok := seenPtr[p]; ok {
			return true
		}
		seenPtr[p] = struct{}{}
		return false
	}

	app := make([]*registeredExporter, 0, len(existingApp)+len(policy.exporters))
	for _, e := range existingApp {
		if e == nil || noteSeen(e.exporter) {
			continue
		}
		app = append(app, e)
	}
	for _, e := range policy.exporters {
		if e == nil || noteSeen(e) {
			continue
		}
		app = append(app, &registeredExporter{exporter: e})
	}
	clstr.appExporters = app

	wantFile := policy.fileExporterRequested()
	sameFile := wantFile && clstr.fileExporter != nil && clstr.fileExporter.dir == policy.fileExportDir()
	if clstr.fileExporter != nil && !sameFile {
		clstr.fileExporter.Close()
		clstr.fileExporter = nil
	}

	var kept []*registeredExporter
	switch token {
	case MetricsExporterFile:
		// FILE calls only the built-in writer (PRD metrics.exporter: FILE).
	case MetricsExporterCustom, MetricsExporterNone:
		// NONE keeps exporters registered; periodicExportEnabled gates the loop.
		kept = app
	default:
		kept = app
	}

	if wantFile {
		if clstr.fileExporter == nil {
			fe := NewFileMetricsExporter(policy.fileExportDir())
			fe.SetReportSizeLimit(policy.ReportSizeLimit)
			clstr.fileExporter = fe
		} else {
			clstr.fileExporter.SetReportSizeLimit(policy.ReportSizeLimit)
		}
		kept = append(kept, &registeredExporter{exporter: clstr.fileExporter, clientOwnedFile: true})
	}

	clstr.exporters = kept
}

func (clstr *Cluster) startMetricsExport(interval time.Duration) {
	if interval <= 0 {
		interval = defaultMetricsExportInterval
	}

	// Idempotent when already running at the same interval — dynconfig polling
	// must not restart the ticker (that would starve Export).
	clstr.metricsMu.Lock()
	if clstr.exportRunning.Load() && clstr.exportInterval == interval {
		clstr.metricsMu.Unlock()
		return
	}
	clstr.metricsMu.Unlock()

	// Serialize start/stop; never hold metricsMu across Wait — the export loop
	// may need metricsMu inside fanOutExport.
	clstr.stopMetricsExport()
	// If a prior Disable ran from inside Export, stop skipped Wait — drain here
	// (Wait on zero is a no-op; never call this from exportInCallback).
	if !clstr.exportInCallback.Load() {
		clstr.exportWG.Wait()
	}

	clstr.metricsMu.Lock()
	defer clstr.metricsMu.Unlock()
	// Another Enable may have won the race; stop again under the lock without Wait.
	if clstr.exportRunning.CompareAndSwap(true, false) {
		if clstr.exportStop != nil {
			close(clstr.exportStop)
			clstr.exportStop = nil
		}
		clstr.metricsMu.Unlock()
		if !clstr.exportInCallback.Load() {
			clstr.exportWG.Wait()
		}
		clstr.metricsMu.Lock()
	}
	stop := make(chan struct{})
	clstr.exportStop = stop
	clstr.exportInterval = interval
	clstr.exportRunning.Store(true)
	clstr.exportWG.Add(1)
	go clstr.metricsExportLoop(interval, stop)
}

func (clstr *Cluster) stopMetricsExport() {
	clstr.metricsMu.Lock()
	if !clstr.exportRunning.CompareAndSwap(true, false) {
		clstr.metricsMu.Unlock()
		return
	}
	stop := clstr.exportStop
	clstr.exportStop = nil
	clstr.exportInterval = 0
	inCallback := clstr.exportInCallback.Load()
	clstr.metricsMu.Unlock()
	if stop != nil {
		close(stop)
	}
	// Export → DisableMetrics would deadlock if we Wait on our own goroutine.
	if inCallback {
		return
	}
	clstr.exportWG.Wait()
}

func (clstr *Cluster) metricsExportLoop(interval time.Duration, stop <-chan struct{}) {
	defer clstr.exportWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !clstr.metricsEnabled.Load() {
				continue
			}
			// Build outside metricsMu so EnableMetrics reshape (statsLock) cannot
			// deadlock with export. fanOutExport releases metricsMu across Export
			// so DisableMetrics from inside an exporter cannot deadlock.
			snap := clstr.buildMetricsSnapshotForExport()
			clstr.exportInCallback.Store(true)
			ok := clstr.fanOutExport(snap)
			clstr.exportInCallback.Store(false)
			if ok {
				// Clear the whole set after a fully successful export fan-out
				// not by snapshot identity.
				clstr.clearDeparted()
			}
		}
	}
}

// fanOutExport returns true only when every active exporter succeeded
// (CLIENT-5654: nodes_departed cleared only after a fully successful export).
// Export runs without metricsMu so DisableMetrics from inside an exporter
// cannot deadlock; skip/fail counters are updated under the lock around calls.
func (clstr *Cluster) fanOutExport(snap *MetricsSnapshot) bool {
	type pending struct {
		reg *registeredExporter
		exp MetricsExporter
	}

	clstr.metricsMu.Lock()
	if len(clstr.exporters) == 0 {
		clstr.metricsMu.Unlock()
		return false
	}

	toCall := make([]pending, 0, len(clstr.exporters))
	for _, e := range clstr.exporters {
		if e == nil || e.exporter == nil {
			continue
		}
		if e.skipRemaining > 0 {
			e.skipRemaining--
			if e.skipRemaining == 0 {
				// Fresh chance after the suspend window (Decision 10).
				e.consecutiveFails = 0
			}
			// Suspended exporters are skipped for this round; do not fail the
			// overall success gate (they already failed previously).
			continue
		}
		toCall = append(toCall, pending{reg: e, exp: e.exporter})
	}
	clstr.metricsMu.Unlock()

	if len(toCall) == 0 {
		return false
	}

	type outcome struct {
		reg *registeredExporter
		ok  bool
	}
	results := make([]outcome, 0, len(toCall))
	for _, p := range toCall {
		ok := true
		func() {
			defer func() {
				if r := recover(); r != nil {
					ok = false
					logger.Logger.Warn("metrics exporter panic: %v", r)
				}
			}()
			p.exp.Export(snap)
			if fe, okFE := p.exp.(*FileMetricsExporter); okFE {
				if err := fe.LastExportError(); err != nil {
					ok = false
					logger.Logger.Warn("metrics file exporter: %s", err.Error())
				}
			}
		}()
		results = append(results, outcome{reg: p.reg, ok: ok})
	}

	clstr.metricsMu.Lock()
	defer clstr.metricsMu.Unlock()
	allOK := true
	for _, r := range results {
		if r.ok {
			r.reg.consecutiveFails = 0
			continue
		}
		allOK = false
		r.reg.consecutiveFails++
		if r.reg.consecutiveFails >= metricsExporterFailureThreshold {
			r.reg.skipRemaining = metricsExporterFailureThreshold
		}
	}
	return allOK
}

const maxNodesDeparted = 256

func departedNodeKey(snap *NodeMetricsSnapshot) string {
	if snap.NodeName != "" {
		return snap.NodeName
	}
	return fmt.Sprintf("%s:%d", snap.NodeAddress, snap.NodePort)
}

func (clstr *Cluster) captureDepartedNode(node *Node) {
	if node == nil || !clstr.metricsEnabled.Load() {
		return
	}
	clstr.aggregateNodeStats([]*Node{node})
	snap := clstr.nodeSnapshotFromLive(node)
	clstr.storeDeparted(snap)
}

// storeDeparted upserts a final per-node snapshot into the departed set.
func (clstr *Cluster) storeDeparted(snap NodeMetricsSnapshot) {
	key := departedNodeKey(&snap)
	clstr.departedMu.Lock()
	defer clstr.departedMu.Unlock()
	if clstr.departed == nil {
		clstr.departed = make(map[string]NodeMetricsSnapshot)
	}
	// Flip-flop: keep one entry per node identity; replace with latest final.
	if _, exists := clstr.departed[key]; exists {
		clstr.departed[key] = snap
		return
	}
	if len(clstr.departed) >= maxNodesDeparted {
		clstr.departedOverflow = true
		logger.Logger.Warn("metrics nodes_departed soft-cap %d reached; possibly more departed nodes not retained", maxNodesDeparted)
		return
	}
	clstr.departed[key] = snap
}

// peekDeparted returns finals for nodes still absent from the live cluster
// (metrics.md §5.6 / PRD: depart+rejoin before snapshot → not in nodes_departed).
func (clstr *Cluster) peekDeparted() ([]NodeMetricsSnapshot, bool) {
	live := map[string]struct{}{}
	for _, n := range clstr.GetNodes() {
		if n == nil {
			continue
		}
		live[n.name] = struct{}{}
	}
	clstr.departedMu.Lock()
	defer clstr.departedMu.Unlock()
	out := make([]NodeMetricsSnapshot, 0, len(clstr.departed))
	for key, s := range clstr.departed {
		if _, ok := live[key]; ok {
			continue
		}
		if s.NodeName != "" {
			if _, ok := live[s.NodeName]; ok {
				continue
			}
		}
		out = append(out, s)
	}
	return out, clstr.departedOverflow
}

// clearDepartedForLiveNode drops a rejoined node from the departed set so it
// never appears in nodes_departed after it is back in the live list.
func (clstr *Cluster) clearDepartedForLiveNode(node *Node) {
	if node == nil {
		return
	}
	clstr.departedMu.Lock()
	defer clstr.departedMu.Unlock()
	if clstr.departed == nil {
		return
	}
	delete(clstr.departed, node.name)
	if host := node.host; host != nil {
		delete(clstr.departed, fmt.Sprintf("%s:%d", host.Name, host.Port))
	}
	if len(clstr.departed) == 0 {
		clstr.departed = nil
		clstr.departedOverflow = false
	}
}

func (clstr *Cluster) clearDeparted() {
	clstr.departedMu.Lock()
	defer clstr.departedMu.Unlock()
	clstr.departed = nil
	clstr.departedOverflow = false
}

// sampleHeapObjectsBytes reads live heap object bytes without a full STW MemStats.
func sampleHeapObjectsBytes() int64 {
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		return int64(samples[0].Value.Uint64())
	}
	return 0
}

// exporterPointer returns a stable identity for pointer-ish exporters, or 0 for
// value types (which are not deduped — callers may register distinct values).
func exporterPointer(e MetricsExporter) uintptr {
	if e == nil {
		return 0
	}
	v := reflect.ValueOf(e)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func:
		return v.Pointer()
	default:
		return 0
	}
}

func (clstr *Cluster) MetricsSnapshot() *MetricsSnapshot {
	return clstr.buildMetricsSnapshot(false)
}

func (clstr *Cluster) buildMetricsSnapshotForExport() *MetricsSnapshot {
	return clstr.buildMetricsSnapshot(true)
}

// sampleCPUPercent returns approximate process CPU utilization as a percent of
// one machine (0–100×NumCPU scale normalized by NumCPU). First sample in a
// window returns 0; subsequent samples use the delta of process CPU time
// (user + gc), not /cpu/classes/total which includes idle.
// onDemand selects the on-demand window so periodic export is not skewed.
func (clstr *Cluster) sampleCPUPercent(forExport bool) float64 {
	samples := []metrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
	}
	metrics.Read(samples)
	var cur float64
	for i := range samples {
		if samples[i].Value.Kind() == metrics.KindFloat64 {
			cur += samples[i].Value.Float64()
		}
	}
	now := time.Now()

	clstr.cpuSampleMu.Lock()
	defer clstr.cpuSampleMu.Unlock()

	var prev float64
	var prevAt time.Time
	if forExport {
		prev, prevAt = clstr.lastCPUSecondsExport, clstr.lastCPUSampleAtExport
		clstr.lastCPUSecondsExport = cur
		clstr.lastCPUSampleAtExport = now
	} else {
		prev, prevAt = clstr.lastCPUSecondsOnDemand, clstr.lastCPUSampleAtOnDemand
		clstr.lastCPUSecondsOnDemand = cur
		clstr.lastCPUSampleAtOnDemand = now
	}
	if prevAt.IsZero() {
		return 0
	}
	elapsed := now.Sub(prevAt).Seconds()
	if elapsed <= 0 {
		return 0
	}
	ncpu := float64(runtime.NumCPU())
	if ncpu < 1 {
		ncpu = 1
	}
	pct := (cur - prev) / elapsed * 100 / ncpu
	if pct < 0 {
		return 0
	}
	return pct
}

func (clstr *Cluster) buildMetricsSnapshot(forExport bool) *MetricsSnapshot {
	policy := clstr.metricsPolicy.Get()
	if policy == nil {
		policy = DefaultMetricsPolicy()
	}

	nodes := clstr.GetNodes()
	clstr.aggregateNodeStats(nodes)

	memBytes := sampleHeapObjectsBytes()
	cpuPct := clstr.sampleCPUPercent(forExport)

	var open int64
	nodeSnaps := make([]NodeMetricsSnapshot, 0, len(nodes))
	var agg NodeMetricsSnapshot
	first := true

	clstr.statsLock.Lock()
	for _, node := range nodes {
		ns := clstr.nodeSnapshotLocked(node)
		open += int64(ns.Connections.InUse + ns.Connections.InPool)
		nodeSnaps = append(nodeSnaps, ns)
		if first {
			// Deep-copy so aggregate merges do not alias Nodes[0] slice backs.
			agg = cloneNodeMetricsSnapshot(ns)
			agg.NodeName = ""
			agg.NodeAddress = ""
			agg.NodePort = 0
			first = false
		} else {
			aggregateNodeSnapshots(&agg, &ns)
		}
	}
	clstr.statsLock.Unlock()

	// On-demand and periodic both include nodes_departed; only periodic Export drains.
	departed, overflow := clstr.peekDeparted()

	var clusterName string
	if clstr.clientPolicy != nil {
		if cp := clstr.clientPolicy.Load(); cp != nil {
			clusterName = cp.ClusterName
		}
	}

	snap := &MetricsSnapshot{
		Timestamp:                     time.Now(),
		MetricsEnabled:                clstr.metricsEnabled.Load(),
		OperationalMetricsEnabled:     clstr.operationalEnabled.Load(),
		UsageMetricsEnabled:           clstr.usageEnabled.Load(),
		ClusterName:                   clusterName,
		ClientType:                    "go",
		ClientVersion:                 clstr.clientModuleVersion,
		AppID:                         clstr.appID(),
		Labels:                        clstr.staticLabels(),
		TotalNodes:                    len(nodes),
		RecoverQueueSize:              0,
		InvalidNodeCount:              clstr.invalidNodeCount.Get(),
		DelayQueueTimeoutCount:        0,
		OpenConnections:               open,
		CommandCount:                  int64(clstr.commandCount.Get()),
		CommandRetries:                int64(clstr.commandRetries.Get()),
		RetryCount:                    int64(clstr.commandRetries.Get()),
		ExceededMaxRetries:            int64(clstr.maxRetriesExceededCount.Get()),
		ExceededTotalTimeout:          int64(clstr.totalTimeoutExceededCount.Get()),
		ValidationOpenFailure:         int64(clstr.validationOpenFailure.Get()),
		ValidationTlsHandshakeFailure: int64(clstr.validationTlsHandshakeFailure.Get()),
		ValidationAuthFailure:         int64(clstr.validationAuthFailure.Get()),
		CpuPercent:                    cpuPct,
		MemoryBytes:                   memBytes,
		Nodes:                         nodeSnaps,
		NodesDeparted:                 departed,
		NodesDepartedPossiblyMore:     overflow,
		BatchPartialFailure:           int64(clstr.batchPartialFailure.Get()),
		Latencies:                     clstr.snapshotClusterLatencies(),
		LatencyUnit:                   policy.LatencyUnit,
		LatencyColumns:                policy.LatencyColumns,
		LatencyShift:                  policy.LatencyShift,
	}
	if !first {
		snap.ClusterAggregated = &agg
	}
	if snap.UsageMetricsEnabled {
		snap.Usage = &UsageMetricsSnapshot{
			Blocking:   int64(clstr.usageBlocking.Get()),
			Deferred:   int64(clstr.usageDeferred.Get()),
			Background: int64(clstr.usageBackground.Get()),
			ShapePoint: int64(clstr.usageShapePoint.Get()),
			ShapeBatch: int64(clstr.usageShapeBatch.Get()),
			ShapeQuery: int64(clstr.usageShapeQuery.Get()),
			FilterAEL:  int64(clstr.usageFilterAEL.Get()),
			FilterExp:  int64(clstr.usageFilterExp.Get()),
		}
	}
	return snap
}

func (clstr *Cluster) nodeSnapshotFromLive(node *Node) NodeMetricsSnapshot {
	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()
	return clstr.nodeSnapshotLocked(node)
}

func (clstr *Cluster) nodeSnapshotLocked(node *Node) NodeMetricsSnapshot {
	h := node.host.String()
	st, ok := clstr.stats[h]
	if !ok {
		copied := node.stats.clone()
		st = &copied
	}
	inPool := node.connections.LenAll()
	open := node.connectionCount.Get()
	inUse := open - inPool
	if inUse < 0 {
		inUse = 0
	}
	return NodeMetricsSnapshot{
		NodeName:    node.GetName(),
		NodeAddress: node.host.Name,
		NodePort:    node.host.Port,
		Connections: ConnectionMetricsSnapshot{
			InUse:  inUse,
			InPool: inPool,
			Opened: st.ConnectionsSuccessful.Get(),
			Closed: st.ConnectionsClosed.Get(),
		},
		ConnectionOpenFailure:         int64(st.ConnectionOpenFailure.Get()),
		ConnectionTlsHandshakeFailure: int64(st.ConnectionTlsHandshakeFailure.Get()),
		ConnectionAuthFailure:         int64(st.ConnectionAuthFailure.Get()),
		CircuitBreakerHits:            int64(st.CircuitBreakerHits.Get()),
		ConnectionPoolEmptyCount:      int64(st.ConnectionsPoolEmpty.Get()),
		TendsTotal:                    int64(st.TendsTotal.Get()),
		PartitionMapUpdates:           int64(st.PartitionMapUpdates.Get()),
		GetMetrics:                    snapshotSyncHist(&st.GetMetrics),
		GetHeaderMetrics:              snapshotSyncHist(&st.GetHeaderMetrics),
		ExistsMetrics:                 snapshotSyncHist(&st.ExistsMetrics),
		PutMetrics:                    snapshotSyncHist(&st.PutMetrics),
		DeleteMetrics:                 snapshotSyncHist(&st.DeleteMetrics),
		OperateMetrics:                snapshotSyncHist(&st.OperateMetrics),
		QueryMetrics:                  snapshotSyncHist(&st.QueryMetrics),
		ScanMetrics:                   snapshotSyncHist(&st.ScanMetrics),
		UDFMetrics:                    snapshotSyncHist(&st.UDFMetrics),
		BatchReadMetrics:              snapshotSyncHist(&st.BatchReadMetrics),
		BatchWriteMetrics:             snapshotSyncHist(&st.BatchWriteMetrics),
		ConnLatency:                   st.snapshotConnLatency(),
		Namespaces:                    st.snapshotNamespaces(),
	}
}

// cloneNodeMetricsSnapshot deep-copies slice-backed fields so aggregates can
// mutate without aliasing the per-node snapshot.
func cloneNodeMetricsSnapshot(src NodeMetricsSnapshot) NodeMetricsSnapshot {
	dst := src
	dst.GetMetrics = cloneHistogramSnapshot(src.GetMetrics)
	dst.GetHeaderMetrics = cloneHistogramSnapshot(src.GetHeaderMetrics)
	dst.ExistsMetrics = cloneHistogramSnapshot(src.ExistsMetrics)
	dst.PutMetrics = cloneHistogramSnapshot(src.PutMetrics)
	dst.DeleteMetrics = cloneHistogramSnapshot(src.DeleteMetrics)
	dst.OperateMetrics = cloneHistogramSnapshot(src.OperateMetrics)
	dst.QueryMetrics = cloneHistogramSnapshot(src.QueryMetrics)
	dst.ScanMetrics = cloneHistogramSnapshot(src.ScanMetrics)
	dst.UDFMetrics = cloneHistogramSnapshot(src.UDFMetrics)
	dst.BatchReadMetrics = cloneHistogramSnapshot(src.BatchReadMetrics)
	dst.BatchWriteMetrics = cloneHistogramSnapshot(src.BatchWriteMetrics)
	dst.ConnLatency = cloneLatencySnapshot(src.ConnLatency)
	if len(src.Namespaces) > 0 {
		dst.Namespaces = make([]NamespaceMetricsSnapshot, len(src.Namespaces))
		for i := range src.Namespaces {
			dst.Namespaces[i] = cloneNamespaceMetricsSnapshot(src.Namespaces[i])
		}
	}
	return dst
}

func cloneNamespaceMetricsSnapshot(src NamespaceMetricsSnapshot) NamespaceMetricsSnapshot {
	dst := src
	if len(src.Latencies) > 0 {
		dst.Latencies = make([]LatencySnapshot, len(src.Latencies))
		for i := range src.Latencies {
			dst.Latencies[i] = cloneLatencySnapshot(src.Latencies[i])
		}
	}
	if len(src.Commands) > 0 {
		dst.Commands = make([]CommandMetricsSnapshot, len(src.Commands))
		for i := range src.Commands {
			dst.Commands[i] = cloneCommandMetricsSnapshot(src.Commands[i])
		}
	}
	return dst
}

func cloneCommandMetricsSnapshot(src CommandMetricsSnapshot) CommandMetricsSnapshot {
	return CommandMetricsSnapshot{
		Command:       src.Command,
		ConnectionAq:  cloneHistogramSnapshot(src.ConnectionAq),
		Latency:       cloneHistogramSnapshot(src.Latency),
		Parsing:       cloneHistogramSnapshot(src.Parsing),
		BytesSent:     cloneHistogramSnapshot(src.BytesSent),
		BytesReceived: cloneHistogramSnapshot(src.BytesReceived),
	}
}

func cloneHistogramSnapshot(src HistogramSnapshot) HistogramSnapshot {
	dst := src
	if len(src.BucketCounts) > 0 {
		dst.BucketCounts = append([]uint64(nil), src.BucketCounts...)
	}
	return dst
}

func cloneLatencySnapshot(src LatencySnapshot) LatencySnapshot {
	dst := src
	if len(src.BucketCounts) > 0 {
		dst.BucketCounts = append([]uint64(nil), src.BucketCounts...)
	}
	return dst
}

func aggregateNodeSnapshots(dst, src *NodeMetricsSnapshot) {
	dst.Connections.InUse += src.Connections.InUse
	dst.Connections.InPool += src.Connections.InPool
	dst.Connections.Opened += src.Connections.Opened
	dst.Connections.Closed += src.Connections.Closed
	dst.ConnectionOpenFailure += src.ConnectionOpenFailure
	dst.ConnectionTlsHandshakeFailure += src.ConnectionTlsHandshakeFailure
	dst.ConnectionAuthFailure += src.ConnectionAuthFailure
	dst.CircuitBreakerHits += src.CircuitBreakerHits
	dst.ConnectionPoolEmptyCount += src.ConnectionPoolEmptyCount
	dst.TendsTotal += src.TendsTotal
	dst.PartitionMapUpdates += src.PartitionMapUpdates
	mergeHistogram(&dst.GetMetrics, &src.GetMetrics)
	mergeHistogram(&dst.GetHeaderMetrics, &src.GetHeaderMetrics)
	mergeHistogram(&dst.ExistsMetrics, &src.ExistsMetrics)
	mergeHistogram(&dst.PutMetrics, &src.PutMetrics)
	mergeHistogram(&dst.DeleteMetrics, &src.DeleteMetrics)
	mergeHistogram(&dst.OperateMetrics, &src.OperateMetrics)
	mergeHistogram(&dst.QueryMetrics, &src.QueryMetrics)
	mergeHistogram(&dst.ScanMetrics, &src.ScanMetrics)
	mergeHistogram(&dst.UDFMetrics, &src.UDFMetrics)
	mergeHistogram(&dst.BatchReadMetrics, &src.BatchReadMetrics)
	mergeHistogram(&dst.BatchWriteMetrics, &src.BatchWriteMetrics)
	mergeLatency(&dst.ConnLatency, &src.ConnLatency)
	byNS := map[string]int{}
	for i := range dst.Namespaces {
		byNS[dst.Namespaces[i].Namespace] = i
	}
	for i := range src.Namespaces {
		s := src.Namespaces[i]
		if idx, ok := byNS[s.Namespace]; ok {
			d := &dst.Namespaces[idx]
			d.Errors += s.Errors
			d.ClientTimeouts += s.ClientTimeouts
			d.ServerTimeouts += s.ServerTimeouts
			d.KeyBusy += s.KeyBusy
			d.RecordTooBig += s.RecordTooBig
			d.DeviceOverload += s.DeviceOverload
			d.BytesIn += s.BytesIn
			d.BytesOut += s.BytesOut
			for j := range s.Latencies {
				if j < len(d.Latencies) {
					mergeLatency(&d.Latencies[j], &s.Latencies[j])
				} else {
					d.Latencies = append(d.Latencies, cloneLatencySnapshot(s.Latencies[j]))
				}
			}
			mergeCommands(&d.Commands, s.Commands)
		} else {
			dst.Namespaces = append(dst.Namespaces, cloneNamespaceMetricsSnapshot(s))
			byNS[s.Namespace] = len(dst.Namespaces) - 1
		}
	}
}

func mergeCommands(dst *[]CommandMetricsSnapshot, src []CommandMetricsSnapshot) {
	if len(src) == 0 {
		return
	}
	byCmd := map[string]int{}
	for i := range *dst {
		byCmd[(*dst)[i].Command] = i
	}
	for i := range src {
		s := src[i]
		if idx, ok := byCmd[s.Command]; ok {
			d := &(*dst)[idx]
			mergeHistogram(&d.ConnectionAq, &s.ConnectionAq)
			mergeHistogram(&d.Latency, &s.Latency)
			mergeHistogram(&d.Parsing, &s.Parsing)
			mergeHistogram(&d.BytesSent, &s.BytesSent)
			mergeHistogram(&d.BytesReceived, &s.BytesReceived)
			continue
		}
		*dst = append(*dst, cloneCommandMetricsSnapshot(s))
		byCmd[s.Command] = len(*dst) - 1
	}
}

func mergeHistogram(d, s *HistogramSnapshot) {
	if s.Count == 0 {
		return
	}
	prevCount := d.Count
	d.Count += s.Count
	d.Sum += s.Sum
	if s.Max > d.Max {
		d.Max = s.Max
	}
	// Gate on Count, not Min==0: a real minimum of 0 is common in ms units.
	if prevCount == 0 || s.Min < d.Min {
		d.Min = s.Min
	}
	if len(d.BucketCounts) < len(s.BucketCounts) {
		tmp := make([]uint64, len(s.BucketCounts))
		copy(tmp, d.BucketCounts)
		d.BucketCounts = tmp
	}
	for i := range s.BucketCounts {
		if i < len(d.BucketCounts) {
			d.BucketCounts[i] += s.BucketCounts[i]
		}
	}
}

func mergeLatency(d, s *LatencySnapshot) {
	if s.Count == 0 {
		return
	}
	prevCount := d.Count
	d.Count += s.Count
	d.Sum += s.Sum
	if s.Max > d.Max {
		d.Max = s.Max
	}
	// Gate on Count, not Min==0: a real minimum of 0 is common in ms units.
	if prevCount == 0 || s.Min < d.Min {
		d.Min = s.Min
	}
	if len(d.BucketCounts) < len(s.BucketCounts) {
		tmp := make([]uint64, len(s.BucketCounts))
		copy(tmp, d.BucketCounts)
		d.BucketCounts = tmp
	}
	for i := range s.BucketCounts {
		if i < len(d.BucketCounts) {
			d.BucketCounts[i] += s.BucketCounts[i]
		}
	}
}

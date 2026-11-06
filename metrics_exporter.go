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

// MetricsExporter receives periodic snapshots. Implementations must copy if they
// retain data after Export returns. The snapshot is valid only for the duration
// of the call. Custom/network exporters are bounded by a 10s watchdog: on
// timeout the call counts as a failed Export, but the Export goroutine is not
// cancelled — do not mutate shared state after return, and treat the snapshot
// as read-only even if Export is still running past the deadline.
//
// Init and cleanup belong to the exporter. DisableMetrics stops periodic Export
// but does not unregister exporters (YAML/API re-enable resumes the same list).
type MetricsExporter interface {
	Export(snapshot *MetricsSnapshot)
}

// metricsExporterFailureThreshold is consecutive Export failures before suspend
// (cross-client default N = 3).
const metricsExporterFailureThreshold = 3

// metricsExporterSuspendIntervals is how many export_interval ticks to wait
// between probes while suspended (retry every 10th interval).
const metricsExporterSuspendIntervals = 10

// metricsExportWatchdog is the bound for non-file Export calls.
// Built-in file writer is unbounded.
const metricsExportWatchdog = 10 * time.Second

type registeredExporter struct {
	exporter         MetricsExporter
	consecutiveFails int
	// skipRemaining counts down ticks until the next probe while suspended.
	// Set to metricsExporterSuspendIntervals on suspend; each tick decrements;
	// when it reaches 0 the exporter is probed on that tick.
	skipRemaining int
	clientOwnedFile bool
	// drainedDeparted tracks nodes_departed keys this exporter has already
	// successfully Export'd (per-exporter drain).
	drainedDeparted map[string]struct{}
}

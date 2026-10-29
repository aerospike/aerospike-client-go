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

// MetricsExporter receives periodic snapshots. Implementations must copy if they
// retain data after Export returns. The snapshot is valid only for the duration
// of the call.
//
// Init and cleanup belong to the exporter. DisableMetrics stops periodic Export
// but does not unregister exporters (YAML/API re-enable resumes the same list).
type MetricsExporter interface {
	Export(snapshot *MetricsSnapshot)
}

// metricsExporterFailureThreshold is consecutive Export failures before suspend
// (exporter Decision 10; cross-client default N = 3).
const metricsExporterFailureThreshold = 3

type registeredExporter struct {
	exporter         MetricsExporter
	consecutiveFails int
	skipRemaining    int
	clientOwnedFile  bool
}

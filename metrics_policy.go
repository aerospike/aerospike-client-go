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
	"strconv"
	"strings"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types/histogram"
)

const (
	defaultMetricsExportInterval = 30 * time.Second
	defaultSamplerRange          = 1
	defaultSamplerThreshold      = 1
	defaultLatencyColumns        = 7
	defaultLatencyShift          = 1
	defaultLatencyBase           = 2

	// MetricsExporterNone collects without periodic Export calls (PRD metrics.exporter: NONE).
	MetricsExporterNone = "NONE"
	// MetricsExporterFile selects the client-owned metrics file writer (FILE).
	MetricsExporterFile = "FILE"
	// MetricsExporterCustom calls Export on exporters registered via AddExporter (CUSTOM).
	MetricsExporterCustom = "CUSTOM"
)

// MetricsPolicy specifies client metrics collection and periodic export configuration.
type MetricsPolicy struct {
	// HistogramType specifies if Go-only detailed (language-optional) histograms
	// should be [histogram.Linear] or [histogram.Logarithmic]. Shared MUST latency
	// classes (conn/read/write/batch/query) use LatencyShift range buckets instead
	// of LatencyBase (collection Decision 5).
	//
	// Default: [histogram.Logarithmic]
	HistogramType histogram.Type

	// LatencyColumns defines the number of elapsed time range buckets in latency histograms.
	//
	// Default: 7 (shared learn-metrics / asadm layout)
	LatencyColumns int //= 7;

	// LatencyBase is used by Go-only detailed (language-optional) logarithmic/linear
	// histograms. Shared latency classes use LatencyShift.
	//
	// Depending on the type of histogram:
	//
	// For logarithmic histograms, the buckets are: <base^1 <base^2 <base^3 ... >=base^(columns-1)
	//
	//  // LatencyColumns=5 latencyBase=8
	//  <8µs <64µs <512µs <4096µs >=4096
	//
	//  // LatencyColumns=7 LatencyBase=4
	//  <4µs <16µs <64µs <256µs <1024µs <4096 >=4096µs
	//
	// For linear histograms, the buckets are: <base <base*2 <base*3 ... >=base*(column-1)
	//
	//  // LatencyColumns=5 latencyBase=15
	//  <15µs <30µs <45µs <60µs >=60µs
	//
	//  // LatencyColumns=7 LatencyBase=5
	//  <5µs <10µs <15µs <20µs <25µs <30µs >=30µs
	//
	// Default: 2
	LatencyBase int //= 2;

	// LatencyShift is the power-of-two multiplier between shared latency buckets
	// after the first (<=1 unit). Same as Java/C# latencyShift / YAML latency_shift.
	//
	// Example (LatencyColumns=7, LatencyShift=1, milliseconds):
	//  <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
	//
	// Default: 1
	LatencyShift int //= 1;

	// LatencyUnit is milliseconds (default) or microseconds for shared latency classes.
	LatencyUnit LatencyUnit

	// OperationalEnabled turns on command-path operational metrics (histograms,
	// namespace errors/bytes, command count/retries). Default true for EnableMetrics
	// so legacy enable stays "master + operational". YAML v3 metrics.enabled without
	// extended.operational defaults to false; legacy metrics.enable alone keeps
	// operational on for v1.0.0 back-compat.
	OperationalEnabled bool

	// UsageEnabled turns on client-wide feature.api.* counters. Default false.
	UsageEnabled bool

	// SamplerRange and SamplerThreshold implement
	// (draw % range) < threshold for operational command-path metrics.
	// Defaults range=1, threshold=1 (every command once operational is on).
	// threshold == 0 never samples command-path operational metrics.
	SamplerRange     int
	SamplerThreshold int

	// Interval is the periodic export period (v3 export_interval duration). Default 30s.
	Interval time.Duration

	// ReportDir is the metrics log directory used when Exporter is FILE.
	// Empty ReportDir installs no file writer; collection can still run.
	// ReportDir alone does not enable collection. File field names are
	// snake_case (v3); this client never shipped a prior learn-metrics file.
	ReportDir string

	// ReportSizeLimit is a soft byte limit for the metrics log. 0 is unbounded.
	// Values below 1_000_000 are raised to 1_000_000 when set.
	ReportSizeLimit int64

	// Exporter is the YAML metrics.exporter token: NONE | FILE | CUSTOM.
	// NONE: collect but do not call Export. FILE: built-in MetricsWriter when
	// ReportDir is set. CUSTOM: Export to AddExporter list only.
	// Custom exporters are never selected by class name from YAML.
	Exporter string

	// exporters is the application-registered list (copied on EnableMetrics).
	exporters []MetricsExporter

	// User provided labels which will appended to the metrics on export. This
	// information is used downstream by metrics aggregators to group/identify metrics
	// collected by the client.
	Labels *Labels
}

// AddExporter registers a custom exporter. The snapshot is pushed on Interval.
func (mp *MetricsPolicy) AddExporter(e MetricsExporter) {
	if mp == nil || e == nil {
		return
	}
	mp.exporters = append(mp.exporters, e)
}

// SetSamplerProbability sets range=1_000_000 and threshold=round(p*1_000_000)
// for cross-client parity. NaN/Inf and values outside [0,1] are clamped.
func (mp *MetricsPolicy) SetSamplerProbability(p float64) {
	if mp == nil {
		return
	}
	mp.SamplerRange = 1_000_000
	if math.IsNaN(p) || math.IsInf(p, 0) || p <= 0 {
		mp.SamplerThreshold = 0
		return
	}
	if p >= 1 {
		mp.SamplerThreshold = 1_000_000
		return
	}
	mp.SamplerThreshold = int(math.Round(p * 1_000_000))
	if mp.SamplerThreshold < 0 {
		mp.SamplerThreshold = 0
	}
}

// SetSamplerNever disables command-path operational sampling (threshold=0).
// Connection-path CM-8 failures remain gated only by operationalEnabled.
func (mp *MetricsPolicy) SetSamplerNever() {
	if mp == nil {
		return
	}
	mp.SamplerThreshold = 0
}

// UseMillisecondsLatency sets shared latency unit to milliseconds with 7 columns
// (cross-client / learn-metrics default).
func (mp *MetricsPolicy) UseMillisecondsLatency() {
	if mp == nil {
		return
	}
	mp.LatencyUnit = LatencyMilliseconds
	mp.LatencyColumns = defaultLatencyColumns
	mp.LatencyShift = defaultLatencyShift
}

// UseMicrosecondsLatency sets shared latency unit to microseconds with 24 columns
// (Go fine-detail preset; still uses shared conn/read/write/batch/query classes).
func (mp *MetricsPolicy) UseMicrosecondsLatency() {
	if mp == nil {
		return
	}
	mp.LatencyUnit = LatencyMicroseconds
	mp.LatencyColumns = 24
	mp.LatencyShift = defaultLatencyShift
}

// DefaultMetricsPolicy creates a MetricsPolicy with predefined set of default parameters.
// EnableMetrics maps this to master + operational collection and starts the export timer.
// A non-empty ReportDir installs the snake_case metrics file writer; empty ReportDir does not.
func DefaultMetricsPolicy() *MetricsPolicy {
	return &MetricsPolicy{
		HistogramType:      histogram.Logarithmic,
		LatencyColumns:     defaultLatencyColumns,
		LatencyBase:        defaultLatencyBase,
		LatencyShift:       defaultLatencyShift,
		LatencyUnit:        LatencyMilliseconds,
		OperationalEnabled: true,
		UsageEnabled:       false,
		SamplerRange:       defaultSamplerRange,
		SamplerThreshold:   defaultSamplerThreshold,
		Interval:           defaultMetricsExportInterval,
		Exporter:           MetricsExporterFile,
		Labels:             NewLabels(),
	}
}

// DefaultMetricsPolicyWithLabels creates a new MetricsPolicy with the provided labels.
// The labels are used to identify the metrics collected by the client.
func DefaultMetricsPolicyWithLabels(pairs ...map[string]string) *MetricsPolicy {
	labels := NewLabels(pairs...)
	mp := *DefaultMetricsPolicy()

	mp.Labels = labels

	return &mp
}

// copy creates a new MetricsPolicy instance and copies the values from the source policy.
func (mp *MetricsPolicy) copy() *MetricsPolicy {
	if mp == nil {
		return nil
	}

	response := *mp
	if len(mp.exporters) > 0 {
		response.exporters = append([]MetricsExporter(nil), mp.exporters...)
	}
	return &response
}

func (mp *MetricsPolicy) elapsedUnits(d time.Duration) uint64 {
	if mp != nil && mp.LatencyUnit == LatencyMicroseconds {
		us := d.Microseconds()
		if us < 0 {
			return 0
		}
		return uint64(us)
	}
	ms := d.Milliseconds()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

func (mp *MetricsPolicy) samplerRange() int {
	if mp == nil || mp.SamplerRange < 1 {
		return 1
	}
	return mp.SamplerRange
}

// parseExportInterval accepts a duration with unit (e.g. "30s", "500ms") or a
// bare integer number of seconds for migration from int-valued YAML.
func parseExportInterval(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d, true
	}
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return time.Duration(n) * time.Second, true
	}
	return 0, false
}

func coerceExportInterval(v any) (time.Duration, bool) {
	switch t := v.(type) {
	case string:
		return parseExportInterval(t)
	case int:
		if t > 0 {
			return time.Duration(t) * time.Second, true
		}
	case int64:
		if t > 0 {
			return time.Duration(t) * time.Second, true
		}
	case float64:
		if t > 0 {
			return time.Duration(t) * time.Second, true
		}
	}
	return 0, false
}

// normalizeExporterToken maps policy/YAML exporter values to NONE|FILE|CUSTOM.
// Empty defaults to FILE (legacy EnableMetrics + report_dir convenience).
// learn_metrics_file / none are accepted as aliases during migration.
func normalizeExporterToken(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case MetricsExporterNone:
		return MetricsExporterNone
	case MetricsExporterCustom:
		return MetricsExporterCustom
	case MetricsExporterFile, "", "LEARN_METRICS_FILE":
		return MetricsExporterFile
	default:
		// Unknown tokens do not install the file writer; treat as CUSTOM.
		return MetricsExporterCustom
	}
}

func (mp *MetricsPolicy) exporterToken() string {
	if mp == nil {
		return MetricsExporterFile
	}
	return normalizeExporterToken(mp.Exporter)
}

// periodicExportEnabled is false only for NONE (collection may still run).
func (mp *MetricsPolicy) periodicExportEnabled() bool {
	return mp != nil && mp.exporterToken() != MetricsExporterNone
}

// fileExporterRequested is true when Exporter is FILE and report_dir is set.
// report_dir alone never enables collection; enable still comes from
// EnableMetrics / metrics.enabled.
func (mp *MetricsPolicy) fileExporterRequested() bool {
	return mp != nil && mp.ReportDir != "" && mp.exporterToken() == MetricsExporterFile
}

func (mp *MetricsPolicy) fileExportDir() string {
	if mp == nil || mp.ReportDir == "" {
		return ""
	}
	return mp.ReportDir
}

// metricsSyncCallBack is called when dynamic configuration changes.
// Enable/disable/re-apply only when the effective policy actually changes so
// config polling does not restart the export ticker or recreate the file writer.
func metricsSyncCallBack(config *dynconfig.Config, client *Client) {
	if client == nil || config == nil || config.Dynamic == nil || config.Dynamic.Metrics == nil {
		return
	}
	policy := client.dynDefaultMetricsPolicy.Load()
	wantEnabled := metricsYAMLEnabled(config.Dynamic.Metrics)

	if !client.MetricsEnabled() {
		if wantEnabled {
			client.cluster.EnableMetrics(policy)
		}
		return
	}
	if !wantEnabled {
		client.cluster.DisableMetrics()
		return
	}
	if metricsPolicyEqual(client.cluster.metricsPolicy.Get(), policy) {
		return
	}
	client.cluster.EnableMetrics(policy)
}

// metricsPolicyEqual compares collection/export knobs that EnableMetrics applies.
// Labels and exporter instance identity are ignored: dynconfig does not rebuild
// AddExporter lists, and label-only churn must not restart the export loop.
func metricsPolicyEqual(a, b *MetricsPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.OperationalEnabled == b.OperationalEnabled &&
		a.UsageEnabled == b.UsageEnabled &&
		a.LatencyColumns == b.LatencyColumns &&
		a.LatencyShift == b.LatencyShift &&
		a.LatencyUnit == b.LatencyUnit &&
		a.LatencyBase == b.LatencyBase &&
		a.HistogramType == b.HistogramType &&
		a.SamplerRange == b.SamplerRange &&
		a.SamplerThreshold == b.SamplerThreshold &&
		a.Interval == b.Interval &&
		a.ReportDir == b.ReportDir &&
		a.ReportSizeLimit == b.ReportSizeLimit &&
		a.exporterToken() == b.exporterToken()
}

// metricsHistogramChanged reports whether shared/detailed histogram layout must reset.
func metricsHistogramChanged(prev, next *MetricsPolicy) bool {
	if next == nil {
		return false
	}
	if prev == nil {
		return true
	}
	return prev.LatencyColumns != next.LatencyColumns ||
		prev.LatencyShift != next.LatencyShift ||
		prev.LatencyUnit != next.LatencyUnit ||
		prev.LatencyBase != next.LatencyBase ||
		prev.HistogramType != next.HistogramType
}

// isZeroValueMetricsPolicy reports a bare &MetricsPolicy{} (or equivalent) with
// no histogram/sampler/export knobs set. EnableMetrics maps that to
// DefaultMetricsPolicy so legacy "enable with empty policy" stays master+operational.
func isZeroValueMetricsPolicy(mp *MetricsPolicy) bool {
	if mp == nil {
		return true
	}
	// LatencyUnit iota 0 is LatencyMilliseconds (a valid default) — do not treat
	// it as unset. LatencyColumns == 0 is the primary "never configured" signal.
	return mp.LatencyColumns == 0 &&
		mp.LatencyBase == 0 &&
		mp.LatencyShift == 0 &&
		mp.HistogramType == 0 &&
		!mp.OperationalEnabled &&
		!mp.UsageEnabled &&
		mp.SamplerRange == 0 &&
		mp.SamplerThreshold == 0 &&
		mp.Interval == 0 &&
		mp.ReportDir == "" &&
		mp.ReportSizeLimit == 0 &&
		mp.Exporter == "" &&
		len(mp.exporters) == 0
}

// normalizeMetricsPolicy returns a policy ready for EnableMetrics: nil or
// zero-value → DefaultMetricsPolicy (legacy enable); otherwise a copy.
func normalizeMetricsPolicy(policy *MetricsPolicy) *MetricsPolicy {
	if policy == nil || isZeroValueMetricsPolicy(policy) {
		return DefaultMetricsPolicy()
	}
	return policy.copy()
}

// metricsYAMLEnabled prefers v3 metrics.enabled over legacy metrics.enable.
func metricsYAMLEnabled(m *dynconfig.Metrics) bool {
	if m == nil {
		return false
	}
	if m.Enabled != nil {
		return *m.Enabled
	}
	if m.Enable != nil {
		return *m.Enable
	}
	return false
}

// patchDynamic implements the configuration logic without locking.
func (mp *MetricsPolicy) patchDynamic(dynConfig *DynConfig) *MetricsPolicy {
	if dynConfig == nil {
		return mp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if config != nil && config.Dynamic != nil && config.Dynamic.Metrics != nil {
		if mp != nil {
			// Copy the existing policy to preserve custom settings.
			return mp.copy().mapDynamic(dynConfig)
		}
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultMetricsPolicy.Load()
	}
	return mp
}

func (mp *MetricsPolicy) mapDynamic(dynConfig *DynConfig) *MetricsPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return mp
	}

	m := currentConfig.Dynamic.Metrics
	if m == nil {
		return mp
	}

	if m.LatencyColumns != nil {
		configValue := *m.LatencyColumns
		mp.LatencyColumns = configValue
		if dynConfig.logUpdate.Load() {
			logger.Logger.Info("LatencyColumns set to %d", configValue)
		}
	}
	if m.LatencyBase != nil {
		configValue := *m.LatencyBase
		mp.LatencyBase = configValue
		if dynConfig.logUpdate.Load() {
			logger.Logger.Info("LatencyBase set to %d", configValue)
		}
	}
	if m.LatencyShift != nil {
		configValue := *m.LatencyShift
		mp.LatencyShift = configValue
		if dynConfig.logUpdate.Load() {
			logger.Logger.Info("LatencyShift set to %d", configValue)
		}
	}
	if m.LatencyUnit != nil {
		mp.LatencyUnit = parseLatencyUnit(*m.LatencyUnit)
		if dynConfig.logUpdate.Load() {
			logger.Logger.Info("LatencyUnit set to %s", mp.LatencyUnit.String())
		}
	}
	if m.Labels != nil {
		configValue := *m.Labels
		mp.Labels = NewLabels(configValue)
		if dynConfig.logUpdate.Load() {
			var labelPairs []string
			for key, value := range configValue {
				labelPairs = append(labelPairs, key+":"+value)
			}
			logger.Logger.Info("Labels set to %s", strings.Join(labelPairs, ", "))
		}
	}
	if m.ExportInterval != nil {
		if d, ok := coerceExportInterval(m.ExportInterval); ok {
			mp.Interval = d
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ExportInterval set to %s", mp.Interval)
			}
		} else if dynConfig.logUpdate.Load() {
			logger.Logger.Warn("ExportInterval value %#v is invalid; keeping %s", m.ExportInterval, mp.Interval)
		}
	}
	if m.ReportDir != nil {
		mp.ReportDir = *m.ReportDir
	}
	if m.ReportSizeLimit != nil {
		mp.ReportSizeLimit = int64(*m.ReportSizeLimit)
	}
	if m.Exporter != nil {
		mp.Exporter = normalizeExporterToken(*m.Exporter)
	}

	// YAML metrics.enabled does not enable operational/usage children.
	// Legacy metrics.enable alone keeps master+operational (v1.0.0 back-compat).
	if m.Extended != nil {
		if m.Extended.Operational != nil {
			op := m.Extended.Operational
			if op.Enabled != nil {
				mp.OperationalEnabled = *op.Enabled
			}
			if op.LatencyUnit != nil {
				mp.LatencyUnit = parseLatencyUnit(*op.LatencyUnit)
			}
			if op.LatencyColumns != nil {
				mp.LatencyColumns = *op.LatencyColumns
			}
			if op.LatencyShift != nil {
				mp.LatencyShift = *op.LatencyShift
			}
			if op.Sampler != nil {
				if op.Sampler.Range != nil {
					mp.SamplerRange = *op.Sampler.Range
				}
				if op.Sampler.Threshold != nil {
					mp.SamplerThreshold = *op.Sampler.Threshold
				}
			}
		} else if m.Enabled != nil && *m.Enabled {
			// extended present but operational omitted under v3 enabled → off.
			mp.OperationalEnabled = false
		}
		if m.Extended.Usage != nil && m.Extended.Usage.Enabled != nil {
			mp.UsageEnabled = *m.Extended.Usage.Enabled
		}
	} else if m.Enabled != nil && *m.Enabled {
		// v3 metrics.enabled without extended does not imply operational.
		mp.OperationalEnabled = false
	} else if m.Enable != nil && *m.Enable {
		// Legacy metrics.enable: master + operational.
		mp.OperationalEnabled = true
	}

	return mp
}

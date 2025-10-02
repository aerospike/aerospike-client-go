// Copyright 2014-2022 Aerospike, Inc.
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
	"strings"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types/histogram"
)

// MetricsPolicy specifies client periodic metrics configuration.
type MetricsPolicy struct {
	// Histogram type specifies if the histogram should be [histogram.Linear] or [histogram.Logarithmic].
	//
	// Default: [histogram.Logarithmic]
	HistogramType histogram.Type

	// LatencyColumns defines the number of elapsed time range buckets in latency histograms.
	//
	// Default: 24
	LatencyColumns int //= 24;

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

	// User provided labels which will appended to the metrics on export. This
	// information is used downstream by metrics aggregetators to group/identify metrics
	// collected by the client.
	Labels *Labels
}

// NewMetricsPolicy creates a new MetricsPolicy with predefined set of default parameters.
func DefaultMetricsPolicy() *MetricsPolicy {
	return &MetricsPolicy{
		HistogramType:  histogram.Logarithmic,
		LatencyColumns: 24,
		LatencyBase:    2,
		Labels:         NewLabels(),
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

// copy creates a new BasePolicy instance and copies the values from the source policy.
func (mp *MetricsPolicy) copy() *MetricsPolicy {
	if mp == nil {
		return nil
	}

	response := *mp
	return &response
}

// metricsSyncCallBack is a callback function that is called when the dynamic configuration changes.
// Changes will only be made if the there is a discrepancy between the current configuration and the new configuration.
func metricsSyncCallBack(config *dynconfig.Config, client *Client) {
	// Metrics are not enabled but configuration is set to enable metrics
	if client != nil && !client.MetricsEnabled() && config.Dynamic.Metrics.Enable != nil && *config.Dynamic.Metrics.Enable {
		client.cluster.EnableMetrics(client.dynDefaultMetricsPolicy.Load())
	} else if client != nil && client.MetricsEnabled() && !*config.Dynamic.Metrics.Enable {
		// Metrics are enabled but configuration is set to disable metrics
		client.cluster.DisableMetrics()
	}
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
		} else {
			// Passed in policy is nil, fetch mapped default policy from cache.
			return dynConfig.client.dynDefaultMetricsPolicy.Load()
		}
	} else {
		return mp
	}
}

func (mp *MetricsPolicy) mapDynamic(dynConfig *DynConfig) *MetricsPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return mp
	}

	if currentConfig.Dynamic.Metrics != nil {
		if currentConfig.Dynamic.Metrics.LatencyColumns != nil {
			configValue := *currentConfig.Dynamic.Metrics.LatencyColumns
			mp.LatencyColumns = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("LatencyColumns set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.Metrics.LatencyBase != nil {
			configValue := *currentConfig.Dynamic.Metrics.LatencyBase
			mp.LatencyBase = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("LatencyBase set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.Metrics.Labels != nil {
			configValue := *currentConfig.Dynamic.Metrics.Labels
			mp.Labels = NewLabels(configValue)
			if dynConfig.logUpdate.Load() {
				var labelPairs []string
				for key, value := range configValue {
					labelPairs = append(labelPairs, key+":"+value)
				}
				logger.Logger.Info("Labels set to %s", strings.Join(labelPairs, ", "))
			}
		}
	}

	return mp
}

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

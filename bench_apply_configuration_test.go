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
	"testing"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
)

func BenchmarkApplyConfigToClientPolicy(b *testing.B) {
	// Create a default client policy.
	cp := NewClientPolicy()

	// Create a dummy dynamic configuration. Adjust the configuration fields as needed.
	// In this example we pass nil for mappedPolicies and a minimal dynconfig.Config.
	cfg := &dynconfig.Config{}
	dynCfg := NewDynConfigForTest(cfg)

	// Ensure the function runs once before benchmarking.
	_ = cp.patchDynamic(dynCfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cp.patchDynamic(dynCfg)
	}
}

func BenchmarkApplyConfigToClientPolicyWithDynamicAndStaticConfig(b *testing.B) {
	cp := NewClientPolicy()

	cfg := &dynconfig.Config{
		Static: &dynconfig.StaticConfig{
			Client: &dynconfig.Client{
				ConfigInterval:        func() *int { i := int(1000 * time.Millisecond); return &i }(),
				ConnectionQueueSize:   func() *int { i := 100; return &i }(),
				MinConnectionsPerNode: func() *int { i := 10; return &i }(),
			},
		},
		Dynamic: &dynconfig.DynamicConfig{
			Client: &dynconfig.Client{
				Timeout:             func() *int { i := int(1000 * time.Millisecond); return &i }(),
				ErrorRateWindow:     func() *int { i := 5; return &i }(),
				MaxErrorRate:        func() *int { i := 10; return &i }(),
				LoginTimeout:        func() *int { i := 1000; return &i }(),
				RackAware:           func() *bool { b := true; return &b }(),
				RackIds:             func() *[]int { i := []int{1, 2, 3}; return &i }(),
				TendInterval:        func() *int { i := 1000; return &i }(),
				ServicesType: func() *string { s := "alternate"; return &s }(),
			},
		},
	}

	dynCfg := NewDynConfigForTest(cfg)

	_ = cp.patchDynamic(dynCfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cp.patchDynamic(dynCfg)
	}
}

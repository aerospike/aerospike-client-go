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
	"sync/atomic"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ApplyConfigToMetricsPolicy", func() {

	gg.Context("when applying full configuration", func() {
		gg.It("updates the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Metrics: &dynconfig.Metrics{
							Enable:         func() *bool { r := true; return &r }(),
							LatencyBase:    func() *int { r := 3; return &r }(),
							LatencyColumns: func() *int { r := 3; return &r }(),
						},
					},
				},
			}

			// Create an initial TxnVerifyPolicy.
			policy := DefaultMetricsPolicy()

			// Check defaults.
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.LatencyBase).To(gm.Equal(2))
			gm.Expect(policy.LatencyColumns).To(gm.Equal(24))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.LatencyBase).To(gm.Equal(3))
			gm.Expect(updatedPolicy.LatencyColumns).To(gm.Equal(3))
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("updates only the specified fields and leaves others unchanged", func() {
			// Create the full configuration.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Metrics: &dynconfig.Metrics{
							Enable:         func() *bool { r := true; return &r }(),
							LatencyColumns: func() *int { r := 3; return &r }(),
						},
					},
				},
			}

			// Create an initial TxnVerifyPolicy.
			policy := DefaultMetricsPolicy()

			// Check defaults.
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.LatencyBase).To(gm.Equal(2))
			gm.Expect(policy.LatencyColumns).To(gm.Equal(24))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.LatencyColumns).To(gm.Equal(3))
		})
	})
})

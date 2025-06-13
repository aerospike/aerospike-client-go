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
	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ApplyConfigToMetricsPolicy", func() {

	Context("when applying full configuration", func() {
		It("updates the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
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
			Expect(policy).NotTo(BeNil())
			Expect(int(policy.LatencyBase)).To(Equal(int(2)))
			Expect(int(policy.LatencyColumns)).To(Equal(int(24)))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(int(updatedPolicy.LatencyBase)).To(Equal(int(3)))
			Expect(int(updatedPolicy.LatencyColumns)).To(Equal(int(3)))
		})
	})

	Context("when applying configuration with select fields", func() {
		It("updates only the specified fields and leaves others unchanged", func() {
			// Create the full configuration.
			config := &DynConfig{
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
			Expect(policy).NotTo(BeNil())
			Expect(int(policy.LatencyBase)).To(Equal(int(2)))
			Expect(int(policy.LatencyColumns)).To(Equal(int(24)))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(int(updatedPolicy.LatencyColumns)).To(Equal(int(3)))
		})
	})
})

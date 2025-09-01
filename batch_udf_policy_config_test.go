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
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ApplyConfigToBatchUDFPolicy", func() {

	gg.Context("when applying full configuration to batch udf policy", func() {
		gg.It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchUdf: &dynconfig.BatchUdf{
							DurableDelete: func() *bool {
								r := true
								return &r
							}(),
							SendKey: func() *bool {
								r := true
								return &r
							}(),
						},
					},
				},
			}

			// Create an initial BatchReadPolicy.
			policy := NewBatchUDFPolicy()

			// Verify defaults.
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.DurableDelete).To(gm.BeFalse())
			gm.Expect(policy.SendKey).To(gm.BeFalse())

			// Apply configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.DurableDelete).To(gm.BeTrue())
			gm.Expect(updatedPolicy.SendKey).To(gm.BeTrue())
		})
	})

	gg.Context("when applying batch read config to a write policy", func() {
		gg.It("should update the write policy values based on the batch udf dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchUdf: &dynconfig.BatchUdf{
							DurableDelete: func() *bool {
								r := true
								return &r
							}(),
							SendKey: func() *bool {
								r := true
								return &r
							}(),
						},
					},
				},
			}

			// Create an initial BatchPolicy (used for write operations).
			batchPolicy := NewBatchPolicy()

			// Verify defaults.
			gm.Expect(batchPolicy).NotTo(gm.BeNil())
			gm.Expect(batchPolicy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(batchPolicy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(batchPolicy.ReadTouchTTLPercent).To(gm.Equal(int32(0)))

			batchUdfPolicy := NewBatchUDFPolicy()
			updatedWritePolicy := batchUdfPolicy.toWritePolicy(batchPolicy, config)

			// Validate applied configuration.
			gm.Expect(updatedWritePolicy).NotTo(gm.BeNil())
			gm.Expect(updatedWritePolicy.DurableDelete).To(gm.BeTrue())
			gm.Expect(updatedWritePolicy.SendKey).To(gm.BeTrue())
		})
	})
})

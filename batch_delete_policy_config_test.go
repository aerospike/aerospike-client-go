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

var _ = Describe("ApplyConfigToBatchDeletePolicy", func() {

	Context("when applying full configuration to batch delete policy", func() {
		It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchDelete: &dynconfig.BatchDelete{
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
			policy := NewBatchDeletePolicy()

			// Verify defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(policy.SendKey).To(BeFalse())

			// Apply configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
		})
	})

	Context("when applying batch read config to a write policy", func() {
		It("should update the write policy values based on the batch delete dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchDelete: &dynconfig.BatchDelete{
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
			Expect(batchPolicy).NotTo(BeNil())
			Expect(batchPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(batchPolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(batchPolicy.ReadTouchTTLPercent).To(Equal(int32(0)))

			batchDeletePolicy := NewBatchDeletePolicy()
			updatedWritePolicy := batchDeletePolicy.toWritePolicy(batchPolicy, config)

			// Validate applied configuration.
			Expect(updatedWritePolicy).NotTo(BeNil())
			Expect(updatedWritePolicy.DurableDelete).To(BeTrue())
			Expect(updatedWritePolicy.SendKey).To(BeTrue())
		})
	})
})

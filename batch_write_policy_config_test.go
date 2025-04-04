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
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	pc "github.com/aerospike/aerospike-client-go/v8/internal/cache"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ApplyConfigToBatchWritePolicy", func() {

	Context("when applying full configuration to batch write policy", func() {
		It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchWrite: &dynconfig.BatchWrite{
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER
								return &r
							}(),
							SleepBetweenRetries: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 1)
								return &d
							}(),
							SocketTimeout: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 3)
								return &d
							}(),
							TotalTimeout: func() *dynconfig.Duration {
								r := dynconfig.Duration(time.Second * 15)
								return &r
							}(),
							MaxRetries: func() *int {
								r := 5
								return &r
							}(),
							DurableDelete: func() *bool {
								r := false
								return &r
							}(),
							SendKey: func() *bool {
								r := false
								return &r
							}(),
							MaxConcurrentThread: func() *int {
								r := 5
								return &r
							}(),
							AllowInline: func() *bool {
								r := true
								return &r
							}(),
							RespondAllKeys: func() *bool {
								r := true
								return &r
							}(),
						},
					},
				},
			}

			// Create an initial BatchWritePolicy.
			policy := NewBatchWritePolicy()

			// Check defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.RecordExistsAction).To(Equal(UPDATE))
			Expect(policy.GenerationPolicy).To(Equal(NONE))
			Expect(policy.CommitLevel).To(Equal(COMMIT_ALL))
			Expect(policy.Generation).To(Equal(uint32(0)))
			Expect(policy.Expiration).To(Equal(uint32(0)))
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(policy.OnLockingOnly).To(BeFalse())
			Expect(policy.SendKey).To(BeFalse())

			// Apply the configuration.
			updatedPolicy := applyConfigToBatchWritePolicy(policy, config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.DurableDelete).To(BeFalse())
			Expect(updatedPolicy.SendKey).To(BeFalse())
		})
	})

	Context("when applying batch write config to a write policy", func() {
		It("should update the write policy values based on the batch write dynamic config", func() {
			dynamicDefaultPolicy := make(map[pc.PolicyT]any, 0)
			dynamicDefaultPolicy[pc.BATCH_PARENT_WRITE_POLICY] = NewWritePolicy(0, 0)

			// Create the full configuration.
			config := NewDynConfigForTest(pc.NewPolicyCacheWithData(nil, dynamicDefaultPolicy), &dynconfig.Config{})

			// Check defaults for BatchPolicy (used for write operations).
			batchPolicy := NewBatchPolicy()
			Expect(batchPolicy).NotTo(BeNil())
			Expect(batchPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(batchPolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(int(batchPolicy.TotalTimeout.Milliseconds())).To(Equal(1000))
			Expect(int(batchPolicy.SocketTimeout.Seconds())).To(Equal(30))
			Expect(batchPolicy.MaxRetries).To(Equal(2))
			Expect(int(batchPolicy.SleepBetweenRetries.Milliseconds())).To(Equal(1))
			Expect(batchPolicy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(batchPolicy.SendKey).To(BeFalse())
			Expect(batchPolicy.UseCompression).To(BeFalse())

			// Create an initial BatchWritePolicy.
			writePolicy := NewBatchWritePolicy()

			// Apply configuration to convert to a write policy.
			updatedWritePolicy := writePolicy.toWritePolicyWithConfig(batchPolicy, config)
			Expect(updatedWritePolicy).NotTo(BeNil())
			Expect(updatedWritePolicy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(int(updatedWritePolicy.TotalTimeout.Seconds())).To(Equal(1))
			Expect(int(updatedWritePolicy.SocketTimeout.Seconds())).To(Equal(30))
			Expect(updatedWritePolicy.MaxRetries).To(Equal(0))
			Expect(int(updatedWritePolicy.SleepBetweenRetries.Milliseconds())).To(BeNumerically(">", 0))
			Expect(updatedWritePolicy.SendKey).To(BeFalse())
		})
	})
})

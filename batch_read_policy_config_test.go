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

var _ = Describe("ApplyConfigToBatchReadPolicy", func() {

	Context("when applying full configuration to batch read policy", func() {
		It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchRead: &dynconfig.BatchRead{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ONE
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.ALLOWUNAVAILABLE
								return &r
							}(),
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
							MaxRetries:          func() *int { r := 5; return &r }(),
							MaxConcurrentThread: func() *int { r := 5; return &r }(),
							AllowInline:         func() *bool { r := true; return &r }(),
							RespondAllKeys:      func() *bool { r := true; return &r }(),
						},
					},
				},
			}

			// Create an initial BatchReadPolicy.
			policy := NewBatchReadPolicy()

			// Verify defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.ReadTouchTTLPercent).To(Equal(int32(0)))

			// Apply configuration.
			updatedPolicy := applyConfigToBatchReadPolicy(policy, config)

			// Validate applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCAllowUnavailable))
		})
	})

	Context("when applying batch read config to a write policy", func() {
		It("should update the write policy values based on the batch read dynamic config", func() {
			dynamicDefaultPolicy := make(map[pc.PolicyT]any, 0)
			dynamicDefaultPolicy[pc.BATCH_PARENT_WRITE_POLICY] = NewWritePolicy(0, 0)
			// Create the full configuration.
			config := NewDynConfigForTest(pc.NewPolicyCacheWithData(nil, dynamicDefaultPolicy), &dynconfig.Config{})

			// Create an initial BatchPolicy (used for write operations).
			batchPolicy := NewBatchPolicy()

			// Verify defaults.
			Expect(batchPolicy).NotTo(BeNil())
			Expect(batchPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(batchPolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(batchPolicy.ReadTouchTTLPercent).To(Equal(int32(0)))

			batchReadPolicy := NewBatchReadPolicy()
			updatedWritePolicy := batchReadPolicy.ToWritePolicyWithConfig(batchPolicy, config)

			// Validate applied configuration.
			Expect(updatedWritePolicy).NotTo(BeNil())
			Expect(updatedWritePolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(updatedWritePolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(updatedWritePolicy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(int(updatedWritePolicy.TotalTimeout.Seconds())).To(Equal(1))
			Expect(int(updatedWritePolicy.SocketTimeout.Seconds())).To(Equal(30))
			Expect(updatedWritePolicy.MaxRetries).To(Equal(0))
			Expect(int(updatedWritePolicy.SleepBetweenRetries.Milliseconds())).To(BeNumerically(">", 0))
			Expect(updatedWritePolicy.SendKey).To(BeFalse())
		})
	})
})

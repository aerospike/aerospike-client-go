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
								r := dynconfig.ALLOW_UNAVAILABLE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER
								return &r
							}(),
							SleepBetweenRetries: func() *int {
								d := 1
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 15
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
			// Create the full configuration.

			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						BatchRead: &dynconfig.BatchRead{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.ALLOW_UNAVAILABLE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER
								return &r
							}(),
							SleepBetweenRetries: func() *int {
								d := 1
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 15
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

			config.client = &Client{dynConfig: config}
			config.updateCachedPolicies()

			// Create an initial BatchPolicy (used for write operations).
			batchPolicy := NewBatchPolicy()

			// Verify defaults.
			Expect(batchPolicy).NotTo(BeNil())
			Expect(batchPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(batchPolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(batchPolicy.ReadTouchTTLPercent).To(Equal(int32(0)))

			// Apply configuration to BatchPolicy.
			batchPolicy = config.client.dynDefaultBatchPolicy.Load()

			// Validate the loaded policy.
			Expect(batchPolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(batchPolicy.ReadModeSC).To(Equal(ReadModeSCAllowUnavailable))
			Expect(batchPolicy.TotalTimeout).To(Equal(15 * time.Millisecond))
			Expect(batchPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(batchPolicy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(batchPolicy.MaxRetries).To(Equal(5))
			Expect(batchPolicy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(batchPolicy.SendKey).To(BeFalse())
			Expect(batchPolicy.UseCompression).To(BeFalse())
			Expect(batchPolicy.AllowInline).To(BeTrue())

			// Apply the dynamic configuration to the BatchPolicy.
			batchReadPolicy := config.client.dynDefaultBatchReadPolicy.Load()
			updatedWritePolicy := batchReadPolicy.ToWritePolicy(batchPolicy, config)

			// Validate applied configuration.
			Expect(updatedWritePolicy).NotTo(BeNil())
			Expect(updatedWritePolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(updatedWritePolicy.ReadModeSC).To(Equal(ReadModeSCAllowUnavailable))
			Expect(updatedWritePolicy.ReplicaPolicy).To(Equal(MASTER))
			Expect(updatedWritePolicy.TotalTimeout).To(Equal(15 * time.Millisecond))
			Expect(updatedWritePolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedWritePolicy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(updatedWritePolicy.MaxRetries).To(Equal(5))
			Expect(updatedWritePolicy.SendKey).To(BeFalse())
		})
	})
})

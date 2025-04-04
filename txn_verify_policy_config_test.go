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

var _ = Describe("ApplyConfigToTxnVerifyPolicy", func() {

	Context("when applying full configuration", func() {
		It("updates the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						TxnVerify: &dynconfig.TxnVerify{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.LINEARIZE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER
								return &r
							}(),
							SleepBetweenRetries: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Millisecond * 1)
								return &d
							}(),
							SocketTimeout: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 3)
								return &d
							}(),
							TotalTimeout: func() *dynconfig.Duration {
								r := dynconfig.Duration(time.Second * 20)
								return &r
							}(),
							MaxRetries:     func() *int { r := 5; return &r }(),
							AllowInline:    func() *bool { r := true; return &r }(),
							AllowInlineSSD: func() *bool { r := true; return &r }(),
							RespondAllKeys: func() *bool { r := true; return &r }(),
						},
					},
				},
			}

			// Create an initial TxnVerifyPolicy.
			policy := NewTxnVerifyPolicy()

			// Check defaults.
			Expect(policy).NotTo(BeNil())
			Expect(int(policy.TotalTimeout.Milliseconds())).To(Equal(10_000))
			Expect(int(policy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(int(policy.SleepBetweenRetries.Milliseconds())).To(Equal(1_000))
			Expect(policy.SendKey).To(BeFalse())

			// Apply the configuration.
			updatedPolicy := applyConfigToTxnVerifyPolicy(policy, config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCLinearize))
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(MASTER))
			Expect(int(updatedPolicy.SleepBetweenRetries.Milliseconds())).To(Equal(1))
			Expect(int(updatedPolicy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(int(updatedPolicy.TotalTimeout.Seconds())).To(Equal(20))
			Expect(updatedPolicy.MaxRetries).To(Equal(5))
			Expect(updatedPolicy.AllowInline).To(BeTrue())
			Expect(updatedPolicy.RespondAllKeys).To(BeTrue())
		})
	})

	Context("when applying configuration with select fields", func() {
		It("updates only the specified fields and leaves others unchanged", func() {
			// Create a configuration with select fields (omitting some values like MaxRetries).
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						TxnVerify: &dynconfig.TxnVerify{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.LINEARIZE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER
								return &r
							}(),
							SleepBetweenRetries: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Millisecond * 1)
								return &d
							}(),
							SocketTimeout: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 3)
								return &d
							}(),
							TotalTimeout: func() *dynconfig.Duration {
								r := dynconfig.Duration(time.Second * 20)
								return &r
							}(),
							// Intentionally leave out MaxRetries and AllowInline.
							AllowInlineSSD: func() *bool {
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

			// Create an initial TxnVerifyPolicy.
			policy := NewTxnVerifyPolicy()

			// Check defaults.
			Expect(policy).NotTo(BeNil())
			Expect(int(policy.TotalTimeout.Milliseconds())).To(Equal(10_000))
			Expect(int(policy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(int(policy.SleepBetweenRetries.Milliseconds())).To(Equal(1_000))
			Expect(policy.SendKey).To(BeFalse())

			// Apply the configuration.
			updatedPolicy := applyConfigToTxnVerifyPolicy(policy, config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(int(updatedPolicy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(updatedPolicy.MaxRetries).To(Equal(5)) // unchanged
			Expect(int(updatedPolicy.SleepBetweenRetries.Milliseconds())).To(Equal(1))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(MASTER))
		})
	})
})

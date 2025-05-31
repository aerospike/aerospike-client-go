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

var _ = Describe("ApplyConfigToTxnRollPolicy", func() {
	Context("when applying full configuration", func() {
		It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						TxnRoll: &dynconfig.TxnRoll{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.LINEARIZE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER_PROLES
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
							MaxRetries: func() *int {
								r := 5
								return &r
							}(),
							AllowInline: func() *bool {
								r := true
								return &r
							}(),
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

			// Create an initial TxnRollPolicy.
			policy := NewTxnRollPolicy()

			// Validate defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.ReplicaPolicy).To(Equal(MASTER))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Second))
			Expect(policy.SocketTimeout).To(Equal(3 * time.Second))
			Expect(policy.TotalTimeout).To(Equal(10 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.AllowInline).To(BeTrue())
			Expect(policy.RespondAllKeys).To(BeTrue())

			updatedPolicy := applyConfigToTxnRollPolicy(policy, config)

			// Validate applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCLinearize))
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(MASTER_PROLES))
			Expect(updatedPolicy.TotalTimeout).To(Equal(15 * time.Second))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Second))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(1 * time.Second))
			Expect(updatedPolicy.MaxRetries).To(Equal(5))
			Expect(updatedPolicy.SendKey).To(BeFalse())
		})
	})

	Context("when applying configuration with select fields", func() {
		It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a configuration with all fields as in full config.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						TxnRoll: &dynconfig.TxnRoll{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.ALLOWUNAVAILABLE
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.MASTER_PROLES
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
							MaxRetries: func() *int {
								r := 5
								return &r
							}(),
							AllowInline: func() *bool {
								r := true
								return &r
							}(),
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

			// Create an initial TxnRollPolicy.
			policy := NewTxnRollPolicy()

			// Validate defaults.
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.ReplicaPolicy).To(Equal(MASTER))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Second))
			Expect(policy.SocketTimeout).To(Equal(3 * time.Second))
			Expect(policy.TotalTimeout).To(Equal(10 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.AllowInline).To(BeTrue())
			Expect(policy.RespondAllKeys).To(BeTrue())

			// Apply configuration.
			updatedPolicy := applyConfigToTxnRollPolicy(policy, config)

			// Validate applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCAllowUnavailable))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Second))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(1 * time.Second))
			Expect(updatedPolicy.TotalTimeout).To(Equal(10 * time.Second))
			Expect(updatedPolicy.MaxRetries).To(Equal(5))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(MASTER_PROLES))
		})
	})
})

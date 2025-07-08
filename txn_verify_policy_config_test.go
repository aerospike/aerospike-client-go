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
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ApplyConfigToTxnVerifyPolicy", func() {

	gg.Context("when applying full configuration", func() {
		gg.It("updates the policy values based on the dynamic config", func() {
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
							SleepBetweenRetries: func() *int {
								d := 1
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 20
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(10 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(3 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Second))
			gm.Expect(policy.SendKey).To(gm.BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.ReadModeAP).To(gm.Equal(ReadModeAPAll))
			gm.Expect(updatedPolicy.ReadModeSC).To(gm.Equal(ReadModeSCLinearize))
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(MASTER))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(20 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(updatedPolicy.AllowInline).To(gm.BeTrue())
			gm.Expect(updatedPolicy.RespondAllKeys).To(gm.BeTrue())
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("updates only the specified fields and leaves others unchanged", func() {
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
							SleepBetweenRetries: func() *int {
								d := 1_000
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3_000
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 20_000
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(10 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(3 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Second))
			gm.Expect(policy.SendKey).To(gm.BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3_000 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(5)) // unchanged
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(20_000 * time.Millisecond))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(MASTER))
		})
	})
})

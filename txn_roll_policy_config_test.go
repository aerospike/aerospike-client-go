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
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ApplyConfigToTxnRollPolicy", func() {
	gg.Context("when applying full configuration", func() {
		gg.It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(policy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(MASTER))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(3 * time.Second))
			gm.Expect(policy.TotalTimeout).To(gm.Equal(10 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.AllowInline).To(gm.BeTrue())
			gm.Expect(policy.RespondAllKeys).To(gm.BeTrue())

			updatedPolicy := policy.patchDynamic(config)

			// Validate applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.ReadModeAP).To(gm.Equal(ReadModeAPAll))
			gm.Expect(updatedPolicy.ReadModeSC).To(gm.Equal(ReadModeSCLinearize))
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(MASTER_PROLES))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(15 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a configuration with all fields as in full config.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						TxnRoll: &dynconfig.TxnRoll{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.ALLOW_UNAVAILABLE
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
			gm.Expect(policy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(policy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(MASTER))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(3 * time.Second))
			gm.Expect(policy.TotalTimeout).To(gm.Equal(10 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.AllowInline).To(gm.BeTrue())
			gm.Expect(policy.RespondAllKeys).To(gm.BeTrue())

			// Apply configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.ReadModeSC).To(gm.Equal(ReadModeSCAllowUnavailable))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(10 * time.Second))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(MASTER_PROLES))
		})
	})
})

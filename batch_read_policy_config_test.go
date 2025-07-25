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

var _ = gg.Describe("ApplyConfigToBatchReadPolicy", func() {

	gg.Context("when applying full configuration to batch read policy", func() {
		gg.It("should update the policy values based on the dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(policy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(policy.ReadTouchTTLPercent).To(gm.Equal(int32(0)))

			// Apply configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(updatedPolicy.ReadModeSC).To(gm.Equal(ReadModeSCAllowUnavailable))
		})
	})

	gg.Context("when applying batch read config to a write policy", func() {
		gg.It("should update the write policy values based on the batch read dynamic config", func() {
			// Create the full configuration.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
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
			gm.Expect(batchPolicy).NotTo(gm.BeNil())
			gm.Expect(batchPolicy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(batchPolicy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(batchPolicy.ReadTouchTTLPercent).To(gm.Equal(int32(0)))

			// Apply configuration to BatchPolicy.
			batchPolicy = config.client.dynDefaultBatchPolicy.Load()

			// Validate the loaded policy.
			gm.Expect(batchPolicy.ReadModeAP).To(gm.Equal(ReadModeAPAll))
			gm.Expect(batchPolicy.ReadModeSC).To(gm.Equal(ReadModeSCAllowUnavailable))
			gm.Expect(batchPolicy.TotalTimeout).To(gm.Equal(15 * time.Millisecond))
			gm.Expect(batchPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(batchPolicy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(batchPolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(batchPolicy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
			gm.Expect(batchPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(batchPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(batchPolicy.AllowInline).To(gm.BeTrue())

			// Apply the dynamic configuration to the BatchPolicy.
			batchReadPolicy := config.client.dynDefaultBatchReadPolicy.Load()
			updatedWritePolicy := batchReadPolicy.toWritePolicy(batchPolicy, config)

			// Validate applied configuration.
			gm.Expect(updatedWritePolicy).NotTo(gm.BeNil())
			gm.Expect(updatedWritePolicy.ReadModeAP).To(gm.Equal(ReadModeAPAll))
			gm.Expect(updatedWritePolicy.ReadModeSC).To(gm.Equal(ReadModeSCAllowUnavailable))
			gm.Expect(updatedWritePolicy.ReplicaPolicy).To(gm.Equal(MASTER))
			gm.Expect(updatedWritePolicy.TotalTimeout).To(gm.Equal(15 * time.Millisecond))
			gm.Expect(updatedWritePolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedWritePolicy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(updatedWritePolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(updatedWritePolicy.SendKey).To(gm.BeFalse())
		})
	})
})

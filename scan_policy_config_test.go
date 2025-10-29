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

var _ = gg.Describe("ApplyConfigToScanPolicy", func() {

	gg.Context("when applying full configuration", func() {
		gg.It("should update all policy values based on the dynamic config", func() {
			// Create the full configuration for scan policies.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Scan: &dynconfig.Scan{
							Replica: func() *dynconfig.Replica {
								r := dynconfig.PREFER_RACK
								return &r
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 5000
								return &r
							}(),
							MaxRetries:         func() *int { r := 3; return &r }(),
							MaxConcurrentNodes: func() *int { r := 5; return &r }(),
						},
					},
				},
			}

			// Create an initial ScanPolicy.
			policy := NewScanPolicy()

			// Validate default values.
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(0 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SleepMultiplier).To(gm.Equal(1.0))
			gm.Expect(policy.IncludeBinData).To(gm.BeTrue())
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.UseCompression).To(gm.BeFalse())
			gm.Expect(policy.MaxConcurrentNodes).To(gm.Equal(0))
			gm.Expect(policy.RecordQueueSize).To(gm.Equal(50))
			gm.Expect(policy.RecordsPerSecond).To(gm.Equal(0))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(5000 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepMultiplier).To(gm.Equal(1.0))
			gm.Expect(updatedPolicy.IncludeBinData).To(gm.BeTrue())
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(updatedPolicy.MaxConcurrentNodes).To(gm.Equal(5))
			gm.Expect(updatedPolicy.RecordQueueSize).To(gm.Equal(50))
			gm.Expect(updatedPolicy.RecordsPerSecond).To(gm.Equal(0))
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a configuration with only a subset of scan fields.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Scan: &dynconfig.Scan{
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							TotalTimeout: func() *int {
								r := 5000
								return &r
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							MaxRetries:         func() *int { r := 3; return &r }(),
							MaxConcurrentNodes: func() *int { r := 5; return &r }(),
						},
					},
				},
			}

			// Create an initial ScanPolicy.
			policy := NewScanPolicy()

			// Validate default values.
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(0 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SleepMultiplier).To(gm.Equal(1.0))
			gm.Expect(policy.IncludeBinData).To(gm.BeTrue())
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.UseCompression).To(gm.BeFalse())
			gm.Expect(policy.MaxConcurrentNodes).To(gm.Equal(0))
			gm.Expect(policy.RecordQueueSize).To(gm.Equal(50))
			gm.Expect(policy.RecordsPerSecond).To(gm.Equal(0))

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(5000 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			// Even if only select fields are configured, SendKey gets overridden.
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
		})
	})
})

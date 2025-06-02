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

var _ = Describe("ApplyConfigToScanPolicy", func() {

	Context("when applying full configuration", func() {
		It("should update all policy values based on the dynamic config", func() {
			// Create the full configuration for scan policies.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Scan: &dynconfig.Scan{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ONE
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.SESSION
								return &r
							}(),
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
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.TotalTimeout).To(Equal(0 * time.Second))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SleepMultiplier).To(Equal(1.0))
			Expect(policy.IncludeBinData).To(BeTrue())
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.UseCompression).To(BeFalse())
			Expect(policy.MaxConcurrentNodes).To(Equal(0))
			Expect(policy.RecordQueueSize).To(Equal(50))
			Expect(policy.RecordsPerSecond).To(Equal(0))

			// Apply the configuration.
			updatedPolicy := applyConfigToScanPolicy(policy, config)
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(updatedPolicy.TotalTimeout).To(Equal(5000 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.SleepMultiplier).To(Equal(1.0))
			Expect(updatedPolicy.IncludeBinData).To(BeTrue())
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.UseCompression).To(BeFalse())
			Expect(updatedPolicy.MaxConcurrentNodes).To(Equal(5))
			Expect(updatedPolicy.RecordQueueSize).To(Equal(50))
			Expect(updatedPolicy.RecordsPerSecond).To(Equal(0))
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})

	Context("when applying configuration with select fields", func() {
		It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a configuration with only a subset of scan fields.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Scan: &dynconfig.Scan{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								r := dynconfig.ALL
								return &r
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								r := dynconfig.ALLOW_UNAVAILABLE
								return &r
							}(),
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
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.TotalTimeout).To(Equal(0 * time.Second))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SleepMultiplier).To(Equal(1.0))
			Expect(policy.IncludeBinData).To(BeTrue())
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.UseCompression).To(BeFalse())
			Expect(policy.MaxConcurrentNodes).To(Equal(0))
			Expect(policy.RecordQueueSize).To(Equal(50))
			Expect(policy.RecordsPerSecond).To(Equal(0))

			// Apply the configuration.
			updatedPolicy := applyConfigToScanPolicy(policy, config)
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.TotalTimeout).To(Equal(5000 * time.Millisecond))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			// Even if only select fields are configured, SendKey gets overridden.
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(SEQUENCE))
		})
	})
})

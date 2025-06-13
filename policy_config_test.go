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

var _ = Describe("ApplyConfigToBasePolicy", func() {

	Context("when applying full configuration", func() {
		It("should update all policy values based on the dynamic config", func() {
			// Create a dummy configuration
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Read: &dynconfig.Read{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								d := dynconfig.ALL
								return &d
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								d := dynconfig.LINEARIZE
								return &d
							}(),
							TotalTimeout: func() *int {
								d := 5
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							MaxRetries: func() *int {
								d := 3
								return &d
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							Replica: func() *dynconfig.Replica {
								d := dynconfig.PREFER_RACK
								return &d
							}(),
						},
					},
				},
			}

			// Create an initial base policy.
			policy := NewPolicy()

			// Verify defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.MaxRetries).To(Equal(2))
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(policy.UseCompression).To(BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCLinearize))
			Expect(updatedPolicy.TotalTimeout).To(Equal(5 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.UseCompression).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})

	Context("when applying configuration with select fields", func() {
		It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a dummy configuration with only a subset of fields.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Read: &dynconfig.Read{
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							Replica: func() *dynconfig.Replica {
								d := dynconfig.PREFER_RACK
								return &d
							}(),
						},
					},
				},
			}

			// Create an initial base policy.
			policy := NewPolicy()

			// Verify defaults.
			Expect(mapReadModeAPToReadModeAP(dynconfig.ONE)).To(Equal(ReadModeAPOne))
			Expect(mapReadModeSCToReadModeSC(dynconfig.LINEARIZE)).To(Equal(ReadModeSCLinearize))
			Expect(policy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.MaxRetries).To(Equal(2))
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(policy.UseCompression).To(BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate that the selected fields were updated.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			// MaxRetries should remain at default since it wasn't set in the config.
			Expect(updatedPolicy.MaxRetries).To(Equal(2))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.UseCompression).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})
})

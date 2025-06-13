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

var _ = Describe("WritePolicy Config", func() {
	var (
		config *DynConfig
		policy *WritePolicy
	)

	Context("when applying complete write configuration", func() {
		BeforeEach(func() {
			// Create the full config.
			config = &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Write: &dynconfig.Write{
							TotalTimeout: func() *int {
								r := 5000
								return &r
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							MaxRetries: func() *int {
								r := 3
								return &r
							}(),
							DurableDelete: func() *bool {
								r := true
								return &r
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							SendKey: func() *bool {
								r := true
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.PREFER_RACK
								return &r
							}(),
						},
					},
				},
			}

			// Create an initial WritePolicy.
			policy = NewWritePolicy(0, 0)
		})

		It("should update all fields from the configuration", func() {
			// Check default values of initial policy.
			Expect(policy).ToNot(BeNil())
			Expect(policy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(0))
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SendKey).To(BeFalse())

			updatedPolicy := policy.patchDynamic(config)

			// Validate the updated policy.
			Expect(updatedPolicy).ToNot(BeNil())
			Expect(updatedPolicy.TotalTimeout).To(Equal(5000 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.SendKey).To(BeTrue())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})

	Context("when applying configuration with select fields", func() {
		BeforeEach(func() {
			config = &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Write: &dynconfig.Write{
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							MaxRetries: func() *int {
								r := 3
								return &r
							}(),
							DurableDelete: func() *bool {
								r := true
								return &r
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							SendKey: func() *bool {
								r := false
								return &r
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.PREFER_RACK
								return &r
							}(),
						},
					},
				},
			}

			policy = NewWritePolicy(0, 0)
		})

		It("should update only select fields while leaving defaults intact", func() {
			// Check default values of initial policy.
			Expect(policy).ToNot(BeNil())
			Expect(policy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(0))
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SendKey).To(BeFalse())

			updatedPolicy := policy.patchDynamic(config)

			// Validate the updated policy.
			Expect(updatedPolicy).ToNot(BeNil())
			// TotalTimeout remains unchanged
			Expect(updatedPolicy.TotalTimeout).To(Equal(1_000 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})
})

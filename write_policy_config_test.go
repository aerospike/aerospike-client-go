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
							TotalTimeout: func() *dynconfig.Duration {
								r := dynconfig.Duration(5000 * time.Millisecond)
								return &r
							}(),
							SocketTimeout: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 3)
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
							SleepBetweenRetries: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 2)
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
			Expect(int(policy.TotalTimeout.Milliseconds())).To(Equal(1000))
			Expect(int(policy.SocketTimeout.Seconds())).To(Equal(30))
			Expect(policy.MaxRetries).To(Equal(0))
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(int(policy.SleepBetweenRetries.Milliseconds())).To(Equal(1))
			Expect(policy.SendKey).To(BeFalse())

			updatedPolicy := applyConfigToWritePolicy(policy, config)

			// Validate the updated policy.
			Expect(updatedPolicy).ToNot(BeNil())
			Expect(int(updatedPolicy.TotalTimeout.Milliseconds())).To(Equal(5000))
			Expect(int(updatedPolicy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
			Expect(int(updatedPolicy.SleepBetweenRetries.Milliseconds())).To(Equal(2000))
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
							SocketTimeout: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 3)
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
							SleepBetweenRetries: func() *dynconfig.Duration {
								d := dynconfig.Duration(time.Second * 2)
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
			Expect(int(policy.TotalTimeout.Milliseconds())).To(Equal(1000))
			Expect(int(policy.SocketTimeout.Seconds())).To(Equal(30))
			Expect(policy.MaxRetries).To(Equal(0))
			Expect(policy.DurableDelete).To(BeFalse())
			Expect(int(policy.SleepBetweenRetries.Milliseconds())).To(Equal(1))
			Expect(policy.SendKey).To(BeFalse())

			updatedPolicy := applyConfigToWritePolicy(policy, config)

			// Validate the updated policy.
			Expect(updatedPolicy).ToNot(BeNil())
			// TotalTimeout remains unchanged
			Expect(int(updatedPolicy.SocketTimeout.Seconds())).To(Equal(3))
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.DurableDelete).To(BeTrue())
			Expect(int(updatedPolicy.SleepBetweenRetries.Milliseconds())).To(Equal(2000))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
		})
	})
})

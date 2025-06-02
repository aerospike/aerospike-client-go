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

var _ = Describe("ApplyConfigToQueryPolicy", func() {

	Context("when applying full configuration", func() {
		It("should update all policy values based on the dynamic config", func() {
			// Create a dummy configuration in dynconfig.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Query: &dynconfig.Query{
							ReadModeAp: func() *dynconfig.ReadModeAp {
								d := dynconfig.ALL
								return &d
							}(),
							ReadModeSc: func() *dynconfig.ReadModeSc {
								d := dynconfig.LINEARIZE
								return &d
							}(),
							TotalTimeout: func() *int {
								d := 3000
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
							IncludeBinData: func() *bool {
								d := false
								return &d
							}(),
							RecordQueueSize: func() *int {
								d := 50
								return &d
							}(),
							ExpectedDuration: func() *dynconfig.QueryDuration {
								d := dynconfig.SHORT
								return &d
							}(),
						},
					},
				},
			}

			// Create an initial QueryPolicy.
			policy := NewQueryPolicy()

			// Check defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.TotalTimeout).To(Equal(0 * time.Millisecond))
			// SocketTimeout is in seconds.
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(policy.UseCompression).To(BeFalse())
			// ExpectedDuration check for default value.
			// (Assuming default ExpectedDuration conversion to int yields LONG.)
			Expect(int(policy.ExpectedDuration)).To(Equal(LONG))

			// Apply the configuration.
			updatedPolicy := applyConfigToQueryPolicy(policy, config)

			// Validate the applied configuration.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.ReadModeAP).To(Equal(ReadModeAPAll))
			Expect(updatedPolicy.ReadModeSC).To(Equal(ReadModeSCLinearize))
			Expect(updatedPolicy.TotalTimeout).To(Equal(3000 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			// Note: Some tests change MaxRetries; full config changes it to 3.
			Expect(updatedPolicy.MaxRetries).To(Equal(3))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.UseCompression).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
			Expect(updatedPolicy.IncludeBinData).To(BeFalse())
			Expect(int(updatedPolicy.ExpectedDuration)).To(Equal(SHORT))
		})
	})

	Context("when applying configuration with select fields", func() {
		It("should update only the specified configuration fields and leave the remainder unchanged", func() {
			// Create a dummy configuration in dynconfig with only a subset of fields.
			config := &DynConfig{
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Query: &dynconfig.Query{
							TotalTimeout: func() *int {
								d := 5
								return &d
							}(),
							SocketTimeout: func() *int {
								d := 3
								return &d
							}(),
							SleepBetweenRetries: func() *int {
								d := 2
								return &d
							}(),
							Replica: func() *dynconfig.Replica {
								r := dynconfig.PREFER_RACK
								return &r
							}(),
						},
					},
				},
			}

			// Create an initial QueryPolicy.
			policy := NewQueryPolicy()

			// Check defaults.
			Expect(policy).NotTo(BeNil())
			Expect(policy.ReadModeAP).To(Equal(ReadModeAPOne))
			Expect(policy.ReadModeSC).To(Equal(ReadModeSCSession))
			Expect(policy.TotalTimeout).To(Equal(0 * time.Second))
			Expect(policy.SocketTimeout).To(Equal(30 * time.Second))
			Expect(policy.MaxRetries).To(Equal(5))
			Expect(policy.SleepBetweenRetries).To(Equal(1 * time.Millisecond))
			Expect(policy.SendKey).To(BeFalse())
			Expect(policy.ReplicaPolicy).To(Equal(SEQUENCE))
			Expect(policy.UseCompression).To(BeFalse())
			Expect(int(policy.ExpectedDuration)).To(Equal(LONG))

			// Apply the configuration.
			updatedPolicy := applyConfigToQueryPolicy(policy, config)

			// Validate that the specified fields were updated.
			Expect(updatedPolicy).NotTo(BeNil())
			Expect(updatedPolicy.TotalTimeout).To(Equal(5 * time.Millisecond))
			Expect(updatedPolicy.SocketTimeout).To(Equal(3 * time.Millisecond))
			// MaxRetries should remain unchanged (default = 5) since it was not set.
			Expect(updatedPolicy.MaxRetries).To(Equal(5))
			Expect(updatedPolicy.SleepBetweenRetries).To(Equal(2 * time.Millisecond))
			Expect(updatedPolicy.SendKey).To(BeFalse())
			Expect(updatedPolicy.UseCompression).To(BeFalse())
			Expect(updatedPolicy.ReplicaPolicy).To(Equal(PREFER_RACK))
			Expect(policy.UseCompression).To(BeFalse())
			Expect(int(policy.ExpectedDuration)).To(Equal(LONG))
		})
	})
})

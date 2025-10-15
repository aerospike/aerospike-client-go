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

var _ = gg.Describe("ApplyConfigToQueryPolicy", func() {

	gg.Context("when applying full configuration", func() {
		gg.It("should update all policy values based on the dynamic config", func() {
			// Create a dummy configuration in dynconfig.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
				config: &dynconfig.Config{
					Dynamic: &dynconfig.DynamicConfig{
						Query: &dynconfig.Query{
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(0 * time.Millisecond))
			// SocketTimeout is in seconds.
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
			gm.Expect(policy.UseCompression).To(gm.BeFalse())
			// ExpectedDuration check for default value.
			// (Assuming default ExpectedDuration conversion to int yields LONG.)
			gm.Expect(int(policy.ExpectedDuration)).To(gm.Equal(LONG))

			// Apply the configuration.
			updatedPolicy := policy.pathDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(3000 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			// Note: Some tests change MaxRetries; full config changes it to 3.
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
			gm.Expect(updatedPolicy.IncludeBinData).To(gm.BeFalse())
			gm.Expect(int(updatedPolicy.ExpectedDuration)).To(gm.Equal(SHORT))
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("should update only the specified configuration fields and leave the remainder unchanged", func() {
			// Create a dummy configuration in dynconfig with only a subset of fields.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(0 * time.Second))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(5))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
			gm.Expect(policy.UseCompression).To(gm.BeFalse())
			gm.Expect(int(policy.ExpectedDuration)).To(gm.Equal(LONG))

			// Apply the configuration.
			updatedPolicy := policy.pathDynamic(config)

			// Validate that the specified fields were updated.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(5 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			// MaxRetries should remain unchanged (default = 5) since it was not set.
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(5))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
			gm.Expect(policy.UseCompression).To(gm.BeFalse())
			gm.Expect(int(policy.ExpectedDuration)).To(gm.Equal(LONG))
		})
	})
})

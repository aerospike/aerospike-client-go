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

var _ = gg.Describe("ApplyConfigToBasePolicy", func() {

	gg.Context("when applying full configuration", func() {
		gg.It("should update all policy values based on the dynamic config", func() {
			// Create a dummy configuration
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
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
			gm.Expect(policy).NotTo(gm.BeNil())
			gm.Expect(policy.ReadModeAP).To(gm.Equal(ReadModeAPOne))
			gm.Expect(policy.ReadModeSC).To(gm.Equal(ReadModeSCSession))
			gm.Expect(policy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.MaxRetries).To(gm.Equal(2))
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
			gm.Expect(policy.UseCompression).To(gm.BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate the applied configuration.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.ReadModeAP).To(gm.Equal(ReadModeAPAll))
			gm.Expect(updatedPolicy.ReadModeSC).To(gm.Equal(ReadModeSCLinearize))
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(5 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.It("should update only the specified configuration fields and leave the rest unchanged", func() {
			// Create a dummy configuration with only a subset of fields.
			config := &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
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
			gm.Expect(mapReadModeAPToReadModeAP(dynconfig.ONE)).To(gm.Equal(ReadModeAPOne))
			gm.Expect(mapReadModeSCToReadModeSC(dynconfig.LINEARIZE)).To(gm.Equal(ReadModeSCLinearize))
			gm.Expect(policy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.MaxRetries).To(gm.Equal(2))
			gm.Expect(policy.SendKey).To(gm.BeFalse())
			gm.Expect(policy.ReplicaPolicy).To(gm.Equal(SEQUENCE))
			gm.Expect(policy.UseCompression).To(gm.BeFalse())

			// Apply the configuration.
			updatedPolicy := policy.patchDynamic(config)

			// Validate that the selected fields were updated.
			gm.Expect(updatedPolicy).NotTo(gm.BeNil())
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			// MaxRetries should remain at default since it wasn't set in the config.
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(2))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.UseCompression).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
		})
	})
})

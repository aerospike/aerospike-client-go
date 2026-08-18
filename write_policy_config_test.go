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

var _ = gg.Describe("WritePolicy Config", func() {
	var (
		config *DynConfig
		policy *WritePolicy
	)

	gg.Context("when applying complete write configuration", func() {
		gg.BeforeEach(func() {
			// Create the full config.
			config = &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
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
							SleepMultiplier: func() *float64 {
								d := 1.5
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

		gg.It("should update all fields from the configuration", func() {
			// Check default values of initial policy.
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(0))
			gm.Expect(policy.DurableDelete).To(gm.BeFalse())
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SleepMultiplier).To(gm.Equal(1.0))
			gm.Expect(policy.SendKey).To(gm.BeFalse())

			updatedPolicy := policy.patchDynamic(config)

			// Validate the updated policy.
			gm.Expect(updatedPolicy).ToNot(gm.BeNil())
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(5000 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.DurableDelete).To(gm.BeTrue())
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.SleepMultiplier).To(gm.Equal(1.5))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeTrue())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
		})
	})

	gg.Context("when applying configuration with select fields", func() {
		gg.BeforeEach(func() {
			config = &DynConfig{
				configInitialized: func() *atomic.Bool { v := &atomic.Bool{}; v.Store(true); return v }(),
				logUpdate:         func() *atomic.Bool { v := &atomic.Bool{}; v.Store(false); return v }(),
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

		gg.It("should update only select fields while leaving defaults intact", func() {
			// Check default values of initial policy.
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(policy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(policy.SocketTimeout).To(gm.Equal(30 * time.Second))
			gm.Expect(policy.MaxRetries).To(gm.Equal(0))
			gm.Expect(policy.DurableDelete).To(gm.BeFalse())
			gm.Expect(policy.SleepBetweenRetries).To(gm.Equal(1 * time.Millisecond))
			gm.Expect(policy.SendKey).To(gm.BeFalse())

			updatedPolicy := policy.patchDynamic(config)

			// Validate the updated policy.
			gm.Expect(updatedPolicy).ToNot(gm.BeNil())
			// TotalTimeout remains unchanged
			gm.Expect(updatedPolicy.TotalTimeout).To(gm.Equal(1_000 * time.Millisecond))
			gm.Expect(updatedPolicy.SocketTimeout).To(gm.Equal(3 * time.Millisecond))
			gm.Expect(updatedPolicy.MaxRetries).To(gm.Equal(3))
			gm.Expect(updatedPolicy.DurableDelete).To(gm.BeTrue())
			gm.Expect(updatedPolicy.SleepBetweenRetries).To(gm.Equal(2 * time.Millisecond))
			gm.Expect(updatedPolicy.SendKey).To(gm.BeFalse())
			gm.Expect(updatedPolicy.ReplicaPolicy).To(gm.Equal(PREFER_RACK))
		})
	})
})

// WritePolicy.Xdr makes a write operate in XDR mode: the INFO1_XDR bit must be
// set in the wire header so the server treats the write as coming from XDR (or
// a connector emulating one). Mirrors the Java client's WritePolicy.xdr.
var _ = gg.Describe("WritePolicy XDR bit encoding", func() {

	// encodeWriteReadAttr encodes a single-bin write and returns the readAttr
	// byte from the message header.
	encodeWriteReadAttr := func(policy *WritePolicy) byte {
		key, err := NewKey("test", "xdr_wire", 1)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cmd := &baseCommand{}
		cmd.dataBuffer = make([]byte, 2048)
		gm.Expect(cmd.setWrite(policy, _WRITE, key, []*Bin{NewBin("a", 1)}, nil)).ToNot(gm.HaveOccurred())
		return cmd.dataBuffer[9]
	}

	gg.It("must not set the bit by default", func() {
		gm.Expect(encodeWriteReadAttr(NewWritePolicy(0, 0)) & byte(_INFO1_XDR)).To(gm.BeZero())
	})

	gg.It("must set the bit when Xdr is enabled, and only that bit", func() {
		policy := NewWritePolicy(0, 0)
		policy.Xdr = true

		plain := encodeWriteReadAttr(NewWritePolicy(0, 0))
		xdr := encodeWriteReadAttr(policy)

		gm.Expect(xdr & byte(_INFO1_XDR)).ToNot(gm.BeZero())
		gm.Expect(xdr^byte(_INFO1_XDR)).To(gm.Equal(plain),
			"enabling Xdr must change nothing besides the XDR bit")
	})
})

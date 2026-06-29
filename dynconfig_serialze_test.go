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

package aerospike_test

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"

	as "github.com/aerospike/aerospike-client-go/v8"
	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	registry "github.com/aerospike/aerospike-client-go/v8/config/registry"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("YAML Unmarshal for Enum Types", func() {
	gg.DescribeTable("ReadModeAp",
		func(input string, expected dynconfig.ReadModeAp, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeAp `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("ONE", "ONE", dynconfig.ONE, false),
		gg.Entry("ALL", "ALL", dynconfig.ALL, false),
		gg.Entry("lowercase", "one", dynconfig.ONE, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("ReadModeSc",
		func(input string, expected dynconfig.ReadModeSc, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeSc `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("SESSION", "SESSION", dynconfig.SESSION, false),
		gg.Entry("LINEARIZE", "LINEARIZE", dynconfig.LINEARIZE, false),
		gg.Entry("ALLOW_REPLICA", "ALLOW_REPLICA", dynconfig.ALLOW_REPLICA, false),
		gg.Entry("ALLOW_UNAVAILABLE", "ALLOW_UNAVAILABLE", dynconfig.ALLOW_UNAVAILABLE, false),
		gg.Entry("lowercase", "session", dynconfig.SESSION, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("Replica",
		func(input string, expected dynconfig.Replica, expectErr bool) {
			var wrapper struct {
				Val dynconfig.Replica `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("MASTER", "MASTER", dynconfig.MASTER, false),
		gg.Entry("MASTER_PROLES", "MASTER_PROLES", dynconfig.MASTER_PROLES, false),
		gg.Entry("SEQUENCE", "SEQUENCE", dynconfig.SEQUENCE, false),
		gg.Entry("PREFER_RACK", "PREFER_RACK", dynconfig.PREFER_RACK, false),
		gg.Entry("lowercase", "master", dynconfig.MASTER, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("QueryDuration",
		func(input string, expected dynconfig.QueryDuration, expectErr bool) {
			var wrapper struct {
				Val dynconfig.QueryDuration `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("LONG", "LONG", dynconfig.LONG, false),
		gg.Entry("SHORT", "SHORT", dynconfig.SHORT, false),
		gg.Entry("LONG_RELAX_AP", "LONG_RELAX_AP", dynconfig.LONG_RELAX_AP, false),
		gg.Entry("lowercase", "long", dynconfig.LONG, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)
})

// In-memory dynamic-config provider (same shape as fakeConfigProvider in client_test.go).
type sendKeyDynProvider struct{ cfg *dynconfig.Config }

func (p *sendKeyDynProvider) LoadConfig(dsn string) *dynconfig.Config { return p.cfg }

// Register the fixture schemes ONCE at tree-construction time (registry.Register panics on a
// duplicate scheme, so this must not run inside a BeforeEach/It). Each scheme carries send_key
// for write, UDF and delete so dynamic config applies to every batch write type.
var _ = func() bool {
	mk := func(sendKey bool) *sendKeyDynProvider {
		sk, version := sendKey, "1.0.0"
		return &sendKeyDynProvider{cfg: &dynconfig.Config{
			Version: &version,
			Dynamic: &dynconfig.DynamicConfig{
				BatchWrite:  &dynconfig.BatchWrite{SendKey: &sk},
				BatchUdf:    &dynconfig.BatchUdf{SendKey: &sk},
				BatchDelete: &dynconfig.BatchDelete{SendKey: &sk},
			},
		}}
	}
	registry.Register("sendkeytrue://", mk(true))
	registry.Register("sendkeyfalse://", mk(false))
	return true
}()

var _ = gg.Describe("CLIENT-4898 dynamic config sendKey override", func() {
	ns := *namespace
	set := "ck4898_dyn"

	// Self-contained (no shared helpers): dynamic config only enables sendKey, never disables an
	// API-set value, and never mutates the caller's policy.
	gg.It("dynamic config can only enable sendKey, never disable it, and never mutates the caller policy", func() {
		// Fresh dynconfig-enabled client per scheme (config is wired at client construction).
		withClient := func(url string) (*as.Client, func()) {
			orig := as.AEROSPIKE_CLIENT_CONFIG_URL
			as.AEROSPIKE_CLIENT_CONFIG_URL = url
			c, err := as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return c, func() { c.Close(); as.AEROSPIKE_CLIENT_CONFIG_URL = orig }
		}

		// Did the server store the user key for this digest? A scan is the only correct probe —
		// the client otherwise supplies the key itself.
		stored := func(key *as.Key) bool {
			rs, err := client.ScanAll(as.NewScanPolicy(), ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer rs.Close()
			for res := range rs.Results() {
				if res.Record != nil && res.Record.Key != nil &&
					string(res.Record.Key.Digest()) == string(key.Digest()) {
					return res.Record.Key.Value() != nil
				}
			}
			return false
		}

		// Run a 3-record batch write with the given per-record sendKey on a dynconfig client.
		run := func(url string, apiSendKey bool, prefix string) []*as.Key {
			c, cleanup := withClient(url)
			defer cleanup()
			wp := as.NewBatchWritePolicy()
			wp.SendKey = apiSendKey
			var recs []as.BatchRecordIfc
			var keys []*as.Key
			for i := 0; i < 3; i++ {
				k, _ := as.NewKey(ns, set, prefix+strconv.Itoa(i))
				keys = append(keys, k)
				recs = append(recs, as.NewBatchWrite(wp, k, as.PutOp(as.NewBin("v", i))))
			}
			gm.Expect(c.BatchOperate(as.NewBatchPolicy(), recs)).ToNot(gm.HaveOccurred())
			return keys
		}

		// API sendKey=true + dynamic send_key=false → must STAY stored (dynamic cannot disable).
		for _, k := range run("sendkeyfalse://dummy", true, "dyn-sticky-") {
			gm.Expect(stored(k)).To(gm.BeTrue(), "dynamic send_key=false must not disable an API-set sendKey=true")
		}

		// API sendKey=false + dynamic send_key=true → must become stored (dynamic enables).
		for _, k := range run("sendkeytrue://dummy", false, "dyn-enable-") {
			gm.Expect(stored(k)).To(gm.BeTrue(), "dynamic send_key=true must enable sendKey")
		}

		// The caller's policy object must never be mutated by dynamic config.
		c, cleanup := withClient("sendkeytrue://dummy")
		defer cleanup()
		wp := as.NewBatchWritePolicy()
		wp.SendKey = false
		k, _ := as.NewKey(ns, set, "dyn-nomutate")
		recs := []as.BatchRecordIfc{as.NewBatchWrite(wp, k, as.PutOp(as.NewBin("v", 1)))}
		gm.Expect(c.BatchOperate(as.NewBatchPolicy(), recs)).ToNot(gm.HaveOccurred())
		gm.Expect(wp.SendKey).To(gm.BeFalse(), "dynamic config must apply to a copy, not the caller's policy")
		gm.Expect(stored(k)).To(gm.BeTrue())
	})
})

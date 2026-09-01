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

const (
	sendKeyTrueConfigScheme  = "sendkeytrue://"
	sendKeyFalseConfigScheme = "sendkeyfalse://"
	testDynConfigDSN         = "dummy" // URL path segment; ignored by in-memory test providers
)

// sendKeyConfigProvider is an in-memory ConfigProvider (same shape as fakeConfigProvider in client_test.go).
type sendKeyConfigProvider struct {
	config *dynconfig.Config
}

func (provider *sendKeyConfigProvider) LoadConfig(dsn string) *dynconfig.Config {
	return provider.config
}

func newSendKeyConfigProvider(sendKey bool) *sendKeyConfigProvider {
	sendKeyValue := sendKey
	configVersion := "1.0.0"
	return &sendKeyConfigProvider{
		config: &dynconfig.Config{
			Version: &configVersion,
			Dynamic: &dynconfig.DynamicConfig{
				BatchWrite:  &dynconfig.BatchWrite{SendKey: &sendKeyValue},
				BatchUdf:    &dynconfig.BatchUdf{SendKey: &sendKeyValue},
				BatchDelete: &dynconfig.BatchDelete{SendKey: &sendKeyValue},
			},
		},
	}
}

// Register fixture schemes ONCE at package init (registry.Register panics on duplicate scheme).
var _ = func() bool {
	registry.Register(sendKeyTrueConfigScheme, newSendKeyConfigProvider(true))
	registry.Register(sendKeyFalseConfigScheme, newSendKeyConfigProvider(false))
	return true
}()

var _ = gg.Describe("CLIENT-4898 dynamic config sendKey override", func() {
	namespaceName := *namespace
	setName := "ck4898_dyn"

	// Dynamic config only enables sendKey, never disables an API-set value,
	// and never mutates the caller's policy.
	gg.It("dynamic config can only enable sendKey, never disable it, and never mutates the caller policy", func() {
		expectBatchOperateOK := func(err error) {
			gm.Expect(err == nil).To(gm.BeTrue(), "batch write failed: %v", err)
		}

		// Dynconfig is wired at client construction, so each scheme needs its own client.
		newDynConfigClient := func(configURL string) (*as.Client, func()) {
			originalConfigURL := as.AEROSPIKE_CLIENT_CONFIG_URL
			as.AEROSPIKE_CLIENT_CONFIG_URL = configURL
			dynClient, err := as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			_, err = dynClient.WarmUp(0)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return dynClient, func() {
				dynClient.Close()
				as.AEROSPIKE_CLIENT_CONFIG_URL = originalConfigURL
			}
		}

		// Did the server store the user key for this digest? A scan is the only correct probe —
		// the client otherwise supplies the key itself.
		isUserKeyStoredOnServer := func(key *as.Key) bool {
			scanResults, err := client.ScanAll(as.NewScanPolicy(), namespaceName, setName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer scanResults.Close()
			for result := range scanResults.Results() {
				if result.Record != nil && result.Record.Key != nil &&
					string(result.Record.Key.Digest()) == string(key.Digest()) {
					return result.Record.Key.Value() != nil
				}
			}
			return false
		}

		// Run a 3-record batch write with the given per-record sendKey on a dynconfig client.
		runDynConfigBatchWrite := func(dynClient *as.Client, apiSendKey bool, keyPrefix string) []*as.Key {
			batchWritePolicy := as.NewBatchWritePolicy()
			batchWritePolicy.SendKey = apiSendKey
			var batchRecords []as.BatchRecordIfc
			var keys []*as.Key
			for i := 0; i < 3; i++ {
				key, _ := as.NewKey(namespaceName, setName, keyPrefix+strconv.Itoa(i))
				keys = append(keys, key)
				batchRecords = append(batchRecords, as.NewBatchWrite(batchWritePolicy, key, as.PutOp(as.NewBin("v", i))))
			}
			expectBatchOperateOK(dynClient.BatchOperate(newSuiteBatchPolicy(), batchRecords))
			return keys
		}

		sendKeyFalseConfigURL := sendKeyFalseConfigScheme + testDynConfigDSN
		sendKeyTrueConfigURL := sendKeyTrueConfigScheme + testDynConfigDSN

		sendKeyFalseClient, cleanupSendKeyFalseClient := newDynConfigClient(sendKeyFalseConfigURL)
		defer cleanupSendKeyFalseClient()
		sendKeyTrueClient, cleanupSendKeyTrueClient := newDynConfigClient(sendKeyTrueConfigURL)
		defer cleanupSendKeyTrueClient()

		// API sendKey=true + dynamic send_key=false → must STAY stored (dynamic cannot disable).
		for _, key := range runDynConfigBatchWrite(sendKeyFalseClient, true, "dyn-sticky-") {
			gm.Expect(isUserKeyStoredOnServer(key)).To(gm.BeTrue(), "dynamic send_key=false must not disable an API-set sendKey=true")
		}

		// API sendKey=false + dynamic send_key=true → must become stored (dynamic enables).
		for _, key := range runDynConfigBatchWrite(sendKeyTrueClient, false, "dyn-enable-") {
			gm.Expect(isUserKeyStoredOnServer(key)).To(gm.BeTrue(), "dynamic send_key=true must enable sendKey")
		}

		// The caller's policy object must never be mutated by dynamic config.
		batchWritePolicy := as.NewBatchWritePolicy()
		batchWritePolicy.SendKey = false
		key, _ := as.NewKey(namespaceName, setName, "dyn-nomutate")
		batchRecords := []as.BatchRecordIfc{as.NewBatchWrite(batchWritePolicy, key, as.PutOp(as.NewBin("v", 1)))}
		expectBatchOperateOK(sendKeyTrueClient.BatchOperate(newSuiteBatchPolicy(), batchRecords))
		gm.Expect(batchWritePolicy.SendKey).To(gm.BeFalse(), "dynamic config must apply to a copy, not the caller's policy")
		gm.Expect(isUserKeyStoredOnServer(key)).To(gm.BeTrue())
	})
})

// Copyright 2014-2026 Aerospike, Inc.
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

	"gopkg.in/yaml.v3"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

type stubConfigProvider struct {
	config *dynconfig.Config
}

func (provider stubConfigProvider) LoadConfig(dsn string) *dynconfig.Config {
	return provider.config
}

// staticPolicyFromYaml parses a configuration file and applies its static
// client section to a default policy, the same path NewCluster takes.
func staticPolicyFromYaml(yamlText string) *ClientPolicy {
	parsed := &dynconfig.Config{}
	gm.Expect(yaml.Unmarshal([]byte(yamlText), parsed)).To(gm.Succeed())
	return NewClientPolicy().mapStatic(&DynConfig{config: parsed})
}

var _ = gg.Describe("Discovery configuration from YAML", gg.Label("discovery"), func() {

	gg.It("maps the ip_map key onto the policy", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    ip_map:
      10.0.0.9: 192.0.2.9
`)

		gm.Expect(policy.IpMap).To(gm.Equal(map[string]string{"10.0.0.9": "192.0.2.9"}))
		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
	})

	gg.It("maps every discovery and translation key onto the policy", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    discovery:
      merge_policy: replace
      execution_mode: tend
      refresh_interval_ms: 60000
      refresh_timeout_ms: 5000
      tend_refresh_deadline_ms: 200
    translation:
      provider: static-map
      on_miss: strict
      static_map:
        10.0.0.2: 198.51.100.7
`)

		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyReplace))
		gm.Expect(policy.DiscoveryExecutionMode).To(gm.Equal(DiscoveryExecutionTend))
		gm.Expect(policy.DiscoveryRefreshInterval).To(gm.Equal(60 * time.Second))
		gm.Expect(policy.DiscoveryRefreshTimeout).To(gm.Equal(5 * time.Second))
		gm.Expect(policy.DiscoveryTendRefreshDeadline).To(gm.Equal(200 * time.Millisecond))
		gm.Expect(policy.TranslationOnMiss).To(gm.Equal(TranslationMissStrict))

		translated, ok := policy.AddressTranslator.Translate(Endpoint{Host: "10.0.0.2", Port: 3000})
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(translated.Name).To(gm.Equal("198.51.100.7"))

		_, ok = policy.AddressTranslator.Translate(Endpoint{Host: "10.0.0.3", Port: 3000})
		gm.Expect(ok).To(gm.BeFalse())
	})

	gg.It("builds the port map translator from the port_map block", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: port-map
      port_map:
        10.0.0.2:3000: 198.51.100.7:4000
`)

		translated, ok := policy.AddressTranslator.Translate(Endpoint{Host: "10.0.0.2", Port: 3000})
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(translated.Name).To(gm.Equal("198.51.100.7"))
		gm.Expect(translated.Port).To(gm.Equal(4000))

		_, ok = policy.AddressTranslator.Translate(Endpoint{Host: "10.0.0.2", Port: 3001})
		gm.Expect(ok).To(gm.BeFalse())
	})

	gg.It("selects the pass-through translator", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: pass-through
`)

		translated, ok := policy.AddressTranslator.Translate(Endpoint{Host: "10.0.0.2", Port: 3000})
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(translated.Name).To(gm.Equal("10.0.0.2"))
		gm.Expect(translated.Port).To(gm.Equal(3000))
	})

	gg.It("selects thread execution mode from the file", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    discovery:
      execution_mode: thread
`)

		gm.Expect(policy.DiscoveryExecutionMode).To(gm.Equal(DiscoveryExecutionThread))
	})

	gg.It("keeps the code translator when the translation block has no provider", func() {
		codeTranslator := NewPassThroughAddressTranslator()
		policy := NewClientPolicy()
		policy.AddressTranslator = codeTranslator

		parsed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    translation:
      on_miss: strict
`), parsed)).To(gm.Succeed())

		patched := policy.mapStatic(&DynConfig{config: parsed})
		gm.Expect(patched.AddressTranslator).To(gm.Equal(codeTranslator))
		gm.Expect(patched.TranslationOnMiss).To(gm.Equal(TranslationMissStrict))
	})

	gg.It("keeps the code configuration for an invalid token and applies the valid keys", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    discovery:
      merge_policy: keep_everything
      refresh_interval_ms: 45000
`)

		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyMerge))
		gm.Expect(policy.DiscoveryRefreshInterval).To(gm.Equal(45 * time.Second))
	})

	gg.It("ignores the whole translation block when its keys contradict each other", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: port-map
      on_miss: strict
      static_map:
        10.0.0.2: 198.51.100.7
`)

		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
		gm.Expect(policy.TranslationOnMiss).To(gm.Equal(TranslationMissPassThrough))
	})

	gg.It("ignores the whole translation block for an unknown provider", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: dns-map
      on_miss: strict
`)

		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
		gm.Expect(policy.TranslationOnMiss).To(gm.Equal(TranslationMissPassThrough))
	})

	gg.It("ignores an invalid port_map and keeps the policy without a translator", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: port-map
      port_map:
        10.0.0.2:3000: 198.51.100.7:notaport
`)

		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
	})

	gg.It("ignores the whole file when a discovery value is invalid, until the file is fixed", func() {
		parsed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    max_connections_per_node: 128
    discovery:
      merge_policy: keep_everything
`), parsed)).To(gm.Succeed())

		provider := &stubConfigProvider{config: parsed}
		config := &DynConfig{
			configInitialized: &atomic.Bool{},
			logUpdate:         &atomic.Bool{},
			configProvider:    provider,
		}

		config.initConfig()

		// The valid keys of the file must not apply either.
		gm.Expect(config.config).To(gm.BeNil())
		gm.Expect(config.configInitialized.Load()).To(gm.BeFalse())
		policy := NewClientPolicy().mapStatic(config)
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(NewClientPolicy().ConnectionQueueSize))

		// A fixed file is picked up on the next load.
		fixed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    max_connections_per_node: 128
    discovery:
      merge_policy: replace
`), fixed)).To(gm.Succeed())
		provider.config = fixed

		config.initConfig()

		gm.Expect(config.config).ToNot(gm.BeNil())
		policy = NewClientPolicy().mapStatic(config)
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(128))
		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyReplace))
	})

	gg.It("ignores the whole file when ip_map and static_map are both set", func() {
		parsed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    ip_map:
      10.0.0.9: 192.0.2.9
    translation:
      static_map:
        10.0.0.2: 198.51.100.7
`), parsed)).To(gm.Succeed())

		config := &DynConfig{
			configInitialized: &atomic.Bool{},
			logUpdate:         &atomic.Bool{},
			configProvider:    stubConfigProvider{config: parsed},
		}

		config.initConfig()

		gm.Expect(config.config).To(gm.BeNil())
		policy := NewClientPolicy().mapStatic(config)
		gm.Expect(policy.IpMap).To(gm.BeNil())
		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
	})

	gg.It("accepts and applies a valid file at load", func() {
		parsed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    discovery:
      merge_policy: replace
    translation:
      provider: pass-through
      on_miss: strict
`), parsed)).To(gm.Succeed())

		config := &DynConfig{
			configInitialized: &atomic.Bool{},
			logUpdate:         &atomic.Bool{},
			configProvider:    stubConfigProvider{config: parsed},
		}

		config.initConfig()

		gm.Expect(config.config).ToNot(gm.BeNil())
		policy := NewClientPolicy().mapStatic(config)
		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyReplace))
		gm.Expect(policy.TranslationOnMiss).To(gm.Equal(TranslationMissStrict))
		gm.Expect(policy.AddressTranslator).ToNot(gm.BeNil())
	})

	gg.It("leaves the policy at its defaults when no discovery keys are present", func() {
		policy := staticPolicyFromYaml(`
static:
  client:
    max_connections_per_node: 128
`)

		defaults := NewClientPolicy()
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(128))
		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(defaults.SeedMergePolicy))
		gm.Expect(policy.DiscoveryExecutionMode).To(gm.Equal(defaults.DiscoveryExecutionMode))
		gm.Expect(policy.DiscoveryRefreshInterval).To(gm.Equal(defaults.DiscoveryRefreshInterval))
		gm.Expect(policy.DiscoveryRefreshTimeout).To(gm.Equal(defaults.DiscoveryRefreshTimeout))
		gm.Expect(policy.DiscoveryTendRefreshDeadline).To(gm.Equal(defaults.DiscoveryTendRefreshDeadline))
		gm.Expect(policy.TranslationOnMiss).To(gm.Equal(defaults.TranslationOnMiss))
		gm.Expect(policy.AddressTranslator).To(gm.BeNil())
		gm.Expect(policy.SeedProvider).To(gm.BeNil())
		gm.Expect(policy.IpMap).To(gm.BeNil())
	})
})

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
	"maps"
	"sync/atomic"

	"gopkg.in/yaml.v3"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/types"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

type recordingSeedProvider struct {
	options map[string]any
}

func (provider *recordingSeedProvider) RefreshSeedCandidates(seedContext *SeedCandidateContext) ([]*Host, error) {
	return nil, nil
}
func (provider *recordingSeedProvider) NeedsPeriodicRefresh() bool { return false }
func (provider *recordingSeedProvider) SupportsDeadline() bool     { return false }
func (provider *recordingSeedProvider) Close()                     {}

type recordingTranslator struct {
	options map[string]any
}

func (translator *recordingTranslator) Translate(advertised Endpoint) (Host, bool) {
	return Host{}, false
}

func unusedSeedFactory(map[string]any) (SeedCandidateProvider, Error) { return nil, nil }
func unusedTranslatorFactory(map[string]any) (AddressTranslator, Error) {
	return nil, nil
}

// registrationHarness mirrors the DynConfig newDynConfigWithCallBack builds
// from the policy, without a config URL. Factory maps are cloned the same way.
func registrationHarness(policy *ClientPolicy, yamlText string) *DynConfig {
	parsed := &dynconfig.Config{}
	gm.Expect(yaml.Unmarshal([]byte(yamlText), parsed)).To(gm.Succeed())

	return &DynConfig{
		configInitialized:              &atomic.Bool{},
		logUpdate:                      &atomic.Bool{},
		configProvider:                 &stubConfigProvider{config: parsed},
		seedCandidateProviderFactories: maps.Clone(policy.seedCandidateProviderFactories),
		addressTranslatorFactories:     maps.Clone(policy.addressTranslatorFactories),
	}
}

func expectFileDropped(config *DynConfig, policy *ClientPolicy) {
	gm.Expect(config.config).To(gm.BeNil())
	patched := policy.copy().mapStatic(config)
	defaults := NewClientPolicy()
	gm.Expect(patched.SeedProvider).To(gm.BeNil())
	gm.Expect(patched.AddressTranslator).To(gm.BeNil())
	gm.Expect(patched.ConnectionQueueSize).To(gm.Equal(defaults.ConnectionQueueSize))
}

func staticConfigError(yamlText string, policy *ClientPolicy) Error {
	parsed := &dynconfig.Config{}
	gm.Expect(yaml.Unmarshal([]byte(yamlText), parsed)).To(gm.Succeed())
	return validateStaticConfig(parsed, policy.seedCandidateProviderFactories, policy.addressTranslatorFactories)
}

var _ = gg.Describe("Custom provider registration", gg.Label("discovery"), func() {

	var providerCalls, translatorCalls atomic.Int64

	registered := func() *ClientPolicy {
		providerCalls.Store(0)
		translatorCalls.Store(0)

		policy := NewClientPolicy()
		gm.Expect(policy.RegisterSeedCandidateProvider("consul",
			func(options map[string]any) (SeedCandidateProvider, Error) {
				providerCalls.Add(1)
				return &recordingSeedProvider{options: options}, nil
			})).To(gm.Succeed())
		gm.Expect(policy.RegisterAddressTranslator("nat-table",
			func(options map[string]any) (AddressTranslator, Error) {
				translatorCalls.Add(1)
				return &recordingTranslator{options: options}, nil
			})).To(gm.Succeed())
		return policy
	}

	registeredYaml := `
static:
  client:
    max_connections_per_node: 99
    discovery:
      provider: consul
      merge_policy: merge
      service: aerospike
      endpoints:
        - 127.0.0.1:1
    translation:
      provider: nat-table
      on_miss: strict
      table:
        10.0.0.1:3000: 127.0.0.1:1
`

	nameOnlyYaml := `
static:
  client:
    discovery:
      provider: consul
    translation:
      provider: nat-table
`

	gg.It("builds the registered provider and translator from the file with their options", func() {
		policy := registered()
		config := registrationHarness(policy, registeredYaml)
		config.initConfig()

		patched := policy.copy().mapStatic(config)

		provider, ok := patched.SeedProvider.(*recordingSeedProvider)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(provider.options).To(gm.Equal(map[string]any{
			"merge_policy": "merge",
			"service":      "aerospike",
			"endpoints":    []any{"127.0.0.1:1"},
		}))
		gm.Expect(patched.SeedMergePolicy).To(gm.Equal(SeedMergePolicyMerge))

		translator, ok := patched.AddressTranslator.(*recordingTranslator)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(translator.options).To(gm.Equal(map[string]any{
			"on_miss": "strict",
			"table":   map[string]any{"10.0.0.1:3000": "127.0.0.1:1"},
		}))
		gm.Expect(patched.TranslationOnMiss).To(gm.Equal(TranslationMissStrict))
		gm.Expect(patched.ConnectionQueueSize).To(gm.Equal(99))
		gm.Expect(providerCalls.Load()).To(gm.Equal(int64(1)))
		gm.Expect(translatorCalls.Load()).To(gm.Equal(int64(1)))
	})

	gg.It("gives the factory empty options for a name-only block", func() {
		policy := registered()
		config := registrationHarness(policy, nameOnlyYaml)
		config.initConfig()

		patched := policy.copy().mapStatic(config)
		provider, ok := patched.SeedProvider.(*recordingSeedProvider)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(provider.options).To(gm.BeEmpty())
		translator, ok := patched.AddressTranslator.(*recordingTranslator)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(translator.options).To(gm.BeEmpty())
	})

	gg.It("rejects every built-in name at registration", func() {
		policy := NewClientPolicy()

		for _, name := range []string{"static", "dns", "aws-tags"} {
			err := policy.RegisterSeedCandidateProvider(name, unusedSeedFactory)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("'" + name + "': built-in name"))
		}
		for _, name := range []string{"static-map", "port-map", "pass-through"} {
			err := policy.RegisterAddressTranslator(name, unusedTranslatorFactory)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("'" + name + "': built-in name"))
		}
	})

	gg.It("rejects a duplicate name at registration", func() {
		policy := registered()

		err := policy.RegisterSeedCandidateProvider("consul", unusedSeedFactory)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("'consul': already registered"))

		err = policy.RegisterAddressTranslator("nat-table", unusedTranslatorFactory)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("'nat-table': already registered"))
	})

	gg.It("rejects an empty name at registration", func() {
		policy := NewClientPolicy()

		err := policy.RegisterSeedCandidateProvider("", unusedSeedFactory)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("empty"))

		err = policy.RegisterAddressTranslator("", unusedTranslatorFactory)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("empty"))
	})

	gg.It("rejects a nil factory at registration", func() {
		policy := NewClientPolicy()

		err := policy.RegisterSeedCandidateProvider("consul", nil)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("nil factory"))

		err = policy.RegisterAddressTranslator("nat-table", nil)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("nil factory"))
	})

	gg.It("invalidates the whole file for an unregistered seed provider name", func() {
		policy := NewClientPolicy()
		config := registrationHarness(policy, registeredYaml)
		config.initConfig()

		expectFileDropped(config, policy)
		err := staticConfigError(registeredYaml, policy)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Invalid static.client.discovery.provider value 'consul': valid values: static"))
	})

	gg.It("names registered seed providers among the valid values for an unknown name", func() {
		policy := registered()
		yamlText := `
static:
  client:
    max_connections_per_node: 99
    discovery:
      provider: etcd
`
		config := registrationHarness(policy, yamlText)
		config.initConfig()

		expectFileDropped(config, policy)
		gm.Expect(providerCalls.Load()).To(gm.BeZero())
		err := staticConfigError(yamlText, policy)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring(
			"Invalid static.client.discovery.provider value 'etcd': valid values: static, consul"))
	})

	gg.It("names registered translators among the valid values for an unknown name", func() {
		policy := registered()
		yamlText := `
static:
  client:
    max_connections_per_node: 99
    translation:
      provider: nat
`
		config := registrationHarness(policy, yamlText)
		config.initConfig()

		expectFileDropped(config, policy)
		gm.Expect(translatorCalls.Load()).To(gm.BeZero())
		err := staticConfigError(yamlText, policy)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring(
			"Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through, nat-table"))
	})

	gg.It("invalidates the file when a factory returns an error and does not call the factory again on reload", func() {
		var calls atomic.Int64
		policy := NewClientPolicy()
		gm.Expect(policy.RegisterSeedCandidateProvider("consul",
			func(options map[string]any) (SeedCandidateProvider, Error) {
				calls.Add(1)
				return nil, newError(types.PARAMETER_ERROR, "no consul agent")
			})).To(gm.Succeed())

		config := registrationHarness(policy, `
static:
  client:
    max_connections_per_node: 77
    discovery:
      provider: consul
`)

		config.initConfig()
		expectFileDropped(config, policy)

		// Same write newDynConfigWithCallBack does after the client-creation load.
		config.skipFactoryBuild = true
		config.initConfig()
		expectFileDropped(config, policy)
		gm.Expect(calls.Load()).To(gm.Equal(int64(1)))
	})

	gg.It("invalidates the file when the factory returns nil", func() {
		policy := NewClientPolicy()
		gm.Expect(policy.RegisterSeedCandidateProvider("consul",
			func(map[string]any) (SeedCandidateProvider, Error) { return nil, nil })).To(gm.Succeed())

		config := registrationHarness(policy, `
static:
  client:
    max_connections_per_node: 77
    discovery:
      provider: consul
`)
		config.initConfig()
		expectFileDropped(config, policy)

		policy = NewClientPolicy()
		gm.Expect(policy.RegisterAddressTranslator("nat-table",
			func(map[string]any) (AddressTranslator, Error) { return nil, nil })).To(gm.Succeed())

		config = registrationHarness(policy, `
static:
  client:
    max_connections_per_node: 77
    translation:
      provider: nat-table
`)
		config.initConfig()
		expectFileDropped(config, policy)
	})

	gg.It("invalidates the file when a factory panics", func() {
		policy := NewClientPolicy()
		gm.Expect(policy.RegisterSeedCandidateProvider("consul",
			func(map[string]any) (SeedCandidateProvider, Error) {
				panic("no consul agent")
			})).To(gm.Succeed())

		config := registrationHarness(policy, `
static:
  client:
    max_connections_per_node: 77
    discovery:
      provider: consul
`)
		config.initConfig()
		expectFileDropped(config, policy)
	})

	gg.It("rejects a registered provider that first appears after client creation", func() {
		policy := registered()
		config := registrationHarness(policy, registeredYaml)
		// The client-creation load already ran; factories must not run now.
		config.skipFactoryBuild = true

		config.initConfig()

		expectFileDropped(config, policy)
		gm.Expect(providerCalls.Load()).To(gm.BeZero())
		gm.Expect(translatorCalls.Load()).To(gm.BeZero())
		err := config.pendingFactoryToBeRegistered(config.configProvider.LoadConfig(""))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("restart the client to apply it"))
	})

	gg.It("invalidates the file when a registered translator also sets static_map", func() {
		policy := registered()
		yamlText := `
static:
  client:
    max_connections_per_node: 99
    translation:
      provider: nat-table
      static_map:
        10.0.0.2: 198.51.100.7
`
		config := registrationHarness(policy, yamlText)
		config.initConfig()

		expectFileDropped(config, policy)
		gm.Expect(translatorCalls.Load()).To(gm.BeZero())
		err := staticConfigError(yamlText, policy)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("static_map is only valid with provider static-map"))
	})

	gg.It("keeps the built instances when the file is reloaded with different options", func() {
		policy := registered()
		config := registrationHarness(policy, registeredYaml)
		config.initConfig()

		first := policy.copy().mapStatic(config)
		config.skipFactoryBuild = true

		nameOnly := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(nameOnlyYaml), nameOnly)).To(gm.Succeed())
		config.configProvider.(*stubConfigProvider).config = nameOnly
		config.initConfig()
		second := policy.copy().mapStatic(config)

		gm.Expect(second.SeedProvider).To(gm.BeIdenticalTo(first.SeedProvider))
		gm.Expect(second.AddressTranslator).To(gm.BeIdenticalTo(first.AddressTranslator))
		gm.Expect(providerCalls.Load()).To(gm.Equal(int64(1)))
		gm.Expect(translatorCalls.Load()).To(gm.Equal(int64(1)))
	})

	gg.It("copy carries registrations and isolates later ones from the original", func() {
		policy := registered()
		copied := policy.copy()

		config := registrationHarness(copied, registeredYaml)
		config.initConfig()
		patched := copied.copy().mapStatic(config)
		_, ok := patched.SeedProvider.(*recordingSeedProvider)
		gm.Expect(ok).To(gm.BeTrue())
		_, ok = patched.AddressTranslator.(*recordingTranslator)
		gm.Expect(ok).To(gm.BeTrue())

		err := copied.RegisterSeedCandidateProvider("consul", unusedSeedFactory)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("'consul': already registered"))

		gm.Expect(copied.RegisterSeedCandidateProvider("etcd", unusedSeedFactory)).To(gm.Succeed())
		gm.Expect(policy.RegisterSeedCandidateProvider("etcd", unusedSeedFactory)).To(gm.Succeed())
	})
})

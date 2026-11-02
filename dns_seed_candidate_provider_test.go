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
	"time"

	"gopkg.in/yaml.v3"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/internal/discovery/dns"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

func dnsPolicyFromYaml(yamlText string) *ClientPolicy {
	config := registrationHarness(NewClientPolicy(), yamlText)
	config.initConfig()
	gm.Expect(config.config).ToNot(gm.BeNil())
	return NewClientPolicy().mapStatic(config)
}

func asDnsProvider(provider SeedCandidateProvider) *dnsSeedCandidateProvider {
	gm.Expect(provider).ToNot(gm.BeNil())
	dnsProvider, ok := provider.(*dnsSeedCandidateProvider)
	gm.Expect(ok).To(gm.BeTrue())
	return dnsProvider
}

var _ = gg.Describe("DnsSeedCandidateProvider", gg.Label("discovery"), func() {

	gg.It("defaults match the Java provider", func() {
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"seeds.example.com"},
			Servers: []string{"127.0.0.1"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		dnsProvider := asDnsProvider(provider)
		gm.Expect(dnsProvider.NeedsPeriodicRefresh()).To(gm.BeTrue())
		gm.Expect(dnsProvider.SupportsDeadline()).To(gm.BeTrue())
		gm.Expect(dnsProvider.timeout).To(gm.Equal(dns.DefaultTimeout))
		gm.Expect(dnsProvider.resolver.Config().Port).To(gm.Equal(dns.DefaultPort))
		gm.Expect(dnsProvider.recordType).To(gm.Equal(DnsRecordTypeA))
		gm.Expect(dnsProvider.tlsName).To(gm.Equal(""))
		provider.Close()
	})

	gg.It("zero Port and Timeout mean the Java numeric defaults", func() {
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names: []string{"seeds.example.com"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		dnsProvider := asDnsProvider(provider)
		gm.Expect(dnsProvider.resolver.Config().Port).To(gm.Equal(3000))
		gm.Expect(dnsProvider.timeout).To(gm.Equal(80 * time.Millisecond))
		provider.Close()
	})

	gg.It("constructs without probing system resolvers when Servers is empty", func() {
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names: []string{"seeds.example.com"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(asDnsProvider(provider).resolver.Config().Servers).To(gm.BeEmpty())
		provider.Close()
	})

	gg.It("sets tls_name on every host from a lookup", func() {
		hosts := hostsFromDNSCandidates([]dns.Candidate{
			{Address: "10.0.0.1", Port: 3000},
			{Address: "10.0.0.2", Port: 4333},
		}, "aerospike-tls")

		gm.Expect(hosts).To(gm.Equal([]*Host{
			{Name: "10.0.0.1", TLSName: "aerospike-tls", Port: 3000},
			{Name: "10.0.0.2", TLSName: "aerospike-tls", Port: 4333},
		}))
	})

	gg.It("rejects invalid constructor settings", func() {
		_, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("at least one name is required"))

		_, err = NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:      []string{"seeds.example.com"},
			RecordType: DnsRecordType(9),
			Servers:    []string{"127.0.0.1"},
		})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Invalid DNS seed provider recordType"))
	})

	gg.It("parses record_type tokens case-insensitively", func() {
		for _, token := range []string{"a", "A"} {
			got, err := parseDnsRecordType(token)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(got).To(gm.Equal(DnsRecordTypeA))
			gm.Expect(got.String()).To(gm.Equal("A"))
		}
		for _, token := range []string{"srv", "SRV"} {
			got, err := parseDnsRecordType(token)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(got).To(gm.Equal(DnsRecordTypeSRV))
			gm.Expect(got.String()).To(gm.Equal("SRV"))
		}
	})
})

var _ = gg.Describe("DNS discovery configuration from YAML", gg.Label("discovery"), func() {

	gg.It("builds the dns provider with every key", func() {
		policy := dnsPolicyFromYaml(`
static:
  client:
    max_connections_per_node: 99
    discovery:
      provider: dns
      merge_policy: replace
      dns:
        names:
          - seeds-a.example.com
          - seeds-b.example.com.
        record_type: srv
        port: 3100
        tls_name: tls-seed
        timeout_ms: 50
        servers:
          - 127.0.0.1:5353
          - "[::1]:5353"
        unknown_key: ignored
`)

		dnsProvider := asDnsProvider(policy.SeedProvider)
		cfg := dnsProvider.resolver.Config()
		gm.Expect(cfg.Names).To(gm.Equal([]string{"seeds-a.example.com", "seeds-b.example.com."}))
		gm.Expect(dnsProvider.recordType).To(gm.Equal(DnsRecordTypeSRV))
		gm.Expect(cfg.Port).To(gm.Equal(3100))
		gm.Expect(dnsProvider.tlsName).To(gm.Equal("tls-seed"))
		gm.Expect(dnsProvider.timeout).To(gm.Equal(50 * time.Millisecond))
		gm.Expect(cfg.Servers).To(gm.Equal([]string{"127.0.0.1:5353", "[::1]:5353"}))
		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyReplace))
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(99))
	})

	gg.It("applies dns block defaults", func() {
		policy := dnsPolicyFromYaml(`
static:
  client:
    discovery:
      provider: dns
      dns:
        names:
          - seeds.example.com
        servers:
          - 127.0.0.1
`)

		dnsProvider := asDnsProvider(policy.SeedProvider)
		cfg := dnsProvider.resolver.Config()
		gm.Expect(cfg.Names).To(gm.Equal([]string{"seeds.example.com"}))
		gm.Expect(dnsProvider.recordType).To(gm.Equal(DnsRecordTypeA))
		gm.Expect(cfg.Port).To(gm.Equal(dns.DefaultPort))
		gm.Expect(dnsProvider.tlsName).To(gm.Equal(""))
		gm.Expect(dnsProvider.timeout).To(gm.Equal(dns.DefaultTimeout))
	})

	gg.It("invalidates the file for an invalid dns block", func() {
		cases := []struct {
			yamlText string
			message  string
		}{
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
`,
				message: "static.client.discovery.dns is required with provider dns",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      dns:
        names: [seeds.example.com]
`,
				message: "static.client.discovery.dns is only valid with provider dns",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: static
      dns:
        names: [seeds.example.com]
`,
				message: "static.client.discovery.dns is only valid with provider dns",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        record_type: a
        servers: ["127.0.0.1"]
`,
				message: "static.client.discovery.dns.names is required",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: []
        servers: ["127.0.0.1"]
`,
				message: "Invalid static.client.discovery.dns.names value '[]': expected a non-empty list of strings",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        record_type: mx
        servers: ["127.0.0.1"]
`,
				message: "Invalid static.client.discovery.dns.record_type value 'mx': valid values: a, srv",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        port: 70000
        servers: ["127.0.0.1"]
`,
				message: "Invalid static.client.discovery.dns.port value '70000': expected an integer from 1 to 65535",
			},
			{
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        timeout_ms: 0
        servers: ["127.0.0.1"]
`,
				message: "Invalid static.client.discovery.dns.timeout_ms value '0': expected an integer from 1 to 2147483647",
			},
		}

		for _, tc := range cases {
			policy := NewClientPolicy()
			config := registrationHarness(policy, tc.yamlText)
			config.initConfig()
			expectFileDropped(config, policy)
			err := staticConfigError(tc.yamlText, policy)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring(tc.message))
		}
	})

	gg.It("invalidates the file when a dns value is rejected by the provider", func() {
		yamlText := `
static:
  client:
    max_connections_per_node: 77
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        servers: [seeds.example.com]
`
		policy := NewClientPolicy()
		config := registrationHarness(policy, yamlText)
		config.initConfig()
		expectFileDropped(config, policy)
		gm.Expect(policy.copy().mapStatic(config).ConnectionQueueSize).To(gm.Equal(NewClientPolicy().ConnectionQueueSize))
	})

	gg.It("keeps the built dns provider when the file is reloaded with different dns settings", func() {
		policy := NewClientPolicy()
		config := registrationHarness(policy, `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        port: 3100
        servers: ["127.0.0.1"]
`)
		config.initConfig()
		first := policy.copy().mapStatic(config)
		dnsProvider := asDnsProvider(first.SeedProvider)
		gm.Expect(dnsProvider.resolver.Config().Port).To(gm.Equal(3100))

		config.skipFactoryBuild = true
		changed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    max_connections_per_node: 97
    discovery:
      provider: dns
      dns:
        names: [other.example.com]
        port: 3000
        servers: ["10.0.0.1"]
`), changed)).To(gm.Succeed())
		config.configProvider.(*stubConfigProvider).config = changed
		config.initConfig()
		second := policy.copy().mapStatic(config)

		gm.Expect(second.SeedProvider).To(gm.BeIdenticalTo(first.SeedProvider))
		gm.Expect(second.ConnectionQueueSize).To(gm.Equal(97))
		gm.Expect(dnsProvider.resolver.Config().Names).To(gm.Equal([]string{"seeds.example.com"}))
		gm.Expect(dnsProvider.resolver.Config().Port).To(gm.Equal(3100))
	})

	gg.It("rejects a dns provider that first appears after client creation", func() {
		policy := NewClientPolicy()
		config := registrationHarness(policy, `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        servers: ["127.0.0.1"]
`)
		config.skipFactoryBuild = true
		config.initConfig()

		expectFileDropped(config, policy)
		err := config.pendingFactoryToBeRegistered(config.configProvider.LoadConfig(""))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("restart the client to apply it"))
	})
})

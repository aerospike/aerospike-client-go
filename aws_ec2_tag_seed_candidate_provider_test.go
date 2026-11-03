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
	"os"
	"time"

	"gopkg.in/yaml.v3"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/internal/discovery/aws"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

func awsPolicyFromYaml(yamlText string) *ClientPolicy {
	config := registrationHarness(NewClientPolicy(), yamlText)
	config.initConfig()
	gm.Expect(config.config).ToNot(gm.BeNil())
	return NewClientPolicy().mapStatic(config)
}

func asAwsProvider(provider SeedCandidateProvider) *awsEc2TagSeedCandidateProvider {
	gm.Expect(provider).ToNot(gm.BeNil())
	awsProvider, ok := provider.(*awsEc2TagSeedCandidateProvider)
	gm.Expect(ok).To(gm.BeTrue())
	return awsProvider
}

func setenvForSpec(name, value string) {
	previous, had := os.LookupEnv(name)
	gm.Expect(os.Setenv(name, value)).To(gm.Succeed())
	gg.DeferCleanup(func() {
		if had {
			_ = os.Setenv(name, previous)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}

func expectInvalidAwsYaml(yamlText, message string) {
	policy := NewClientPolicy()
	config := registrationHarness(policy, yamlText)
	config.initConfig()
	expectFileDropped(config, policy)
	err := staticConfigError(yamlText, policy)
	gm.Expect(err).To(gm.HaveOccurred())
	gm.Expect(err.Error()).To(gm.ContainSubstring(message))
}

var _ = gg.Describe("AwsEc2TagSeedCandidateProvider", gg.Label("discovery"), func() {

	gg.It("refreshes periodically, refuses a tend deadline, and fills unset fields with 3000 / private_ip / 80ms", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:  "eu-west-1",
			Filters: []AwsFilter{AwsTagFilter("aerospike-cluster", "prod-a")},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		awsProvider := asAwsProvider(provider)
		cfg := awsProvider.client.Config()
		gm.Expect(awsProvider.NeedsPeriodicRefresh()).To(gm.BeTrue())
		gm.Expect(awsProvider.SupportsDeadline()).To(gm.BeFalse())
		gm.Expect(cfg.Timeout).To(gm.Equal(aws.DefaultTimeout))
		gm.Expect(cfg.Port).To(gm.Equal(aws.DefaultPort))
		gm.Expect(cfg.AddressField).To(gm.Equal(aws.AddressFieldPrivateIP))
		gm.Expect(cfg.TLSName).To(gm.Equal(""))
		gm.Expect(cfg.Endpoint).To(gm.Equal(""))
		gm.Expect(awsProvider.String()).To(gm.Equal("aws-tags eu-west-1 {tag:aerospike-cluster=[prod-a]}"))
		provider.Close()
	})

	gg.It("treats zero Port and Timeout as 3000 and 80ms", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:  "us-east-1",
			Filters: []AwsFilter{{Name: "tag:a", Values: []string{"b"}}},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cfg := asAwsProvider(provider).client.Config()
		gm.Expect(cfg.Port).To(gm.Equal(3000))
		gm.Expect(cfg.Timeout).To(gm.Equal(80 * time.Millisecond))
		provider.Close()
	})

	gg.It("rejects a missing region as 'null'", func() {
		_, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Invalid AWS EC2 seed provider region: 'null'"))
	})

	gg.It("rejects an unknown address field", func() {
		_, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:       "us-east-1",
			Filters:      []AwsFilter{{Name: "tag:a", Values: []string{"b"}}},
			AddressField: AwsAddressField(9),
			Endpoint:     "http://127.0.0.1",
		})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Invalid AWS EC2 seed provider addressField"))
	})

	gg.It("rejects a nil SeedCandidateContext", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:  "us-east-1",
			Filters: []AwsFilter{{Name: "tag:a", Values: []string{"b"}}},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, refreshErr := provider.RefreshSeedCandidates(nil)
		gm.Expect(refreshErr).To(gm.HaveOccurred())
		gm.Expect(refreshErr.Error()).To(gm.ContainSubstring("SeedCandidateContext is required"))
		provider.Close()
	})

	gg.It("parses address_field tokens without regard to case", func() {
		got, err := parseAwsAddressField("PUBLIC_DNS")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(got).To(gm.Equal(AwsAddressFieldPublicDNS))
	})
})

var _ = gg.Describe("AWS tags discovery configuration from YAML", gg.Label("discovery"), func() {

	gg.It("builds the provider from every aws key and ignores unknown keys", func() {
		policy := awsPolicyFromYaml(`
static:
  client:
    max_connections_per_node: 99
    discovery:
      provider: aws-tags
      merge_policy: merge
      aws:
        region: eu-west-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
              - prod-b
          - name: subnet-id
            values:
              - subnet-0123
        address_field: public_dns
        port: 3100
        tls_name: tls-seed
        timeout_ms: 60
        endpoint: https://vpce-0123.ec2.eu-west-1.vpce.amazonaws.com
        access_key_id: ignored-unknown-key
`)

		awsProvider := asAwsProvider(policy.SeedProvider)
		cfg := awsProvider.client.Config()
		gm.Expect(cfg.Region).To(gm.Equal("eu-west-1"))
		gm.Expect(cfg.Filters).To(gm.Equal([]aws.Filter{
			{Name: "tag:aerospike-cluster", Values: []string{"prod-a", "prod-b"}},
			{Name: "subnet-id", Values: []string{"subnet-0123"}},
		}))
		gm.Expect(cfg.AddressField).To(gm.Equal(aws.AddressFieldPublicDNS))
		gm.Expect(cfg.Port).To(gm.Equal(3100))
		gm.Expect(cfg.TLSName).To(gm.Equal("tls-seed"))
		gm.Expect(cfg.Timeout).To(gm.Equal(60 * time.Millisecond))
		gm.Expect(cfg.Endpoint).To(gm.Equal("https://vpce-0123.ec2.eu-west-1.vpce.amazonaws.com"))
		gm.Expect(policy.SeedMergePolicy).To(gm.Equal(SeedMergePolicyMerge))
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(99))
	})

	gg.It("fills omitted aws keys with the same defaults as the constructor", func() {
		policy := awsPolicyFromYaml(`
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
`)

		cfg := asAwsProvider(policy.SeedProvider).client.Config()
		gm.Expect(cfg.Region).To(gm.Equal("us-east-1"))
		gm.Expect(cfg.AddressField).To(gm.Equal(aws.AddressFieldPrivateIP))
		gm.Expect(cfg.Port).To(gm.Equal(aws.DefaultPort))
		gm.Expect(cfg.TLSName).To(gm.Equal(""))
		gm.Expect(cfg.Timeout).To(gm.Equal(aws.DefaultTimeout))
		gm.Expect(cfg.Endpoint).To(gm.Equal(""))
	})

	gg.It("ignores init_timeout_ms like any unknown aws key, even 0", func() {
		policy := awsPolicyFromYaml(`
static:
  client:
    max_connections_per_node: 98
    discovery:
      provider: aws-tags
      refresh_timeout_ms: 7000
      aws:
        region: us-east-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
        timeout_ms: 60
        init_timeout_ms: 0
`)

		cfg := asAwsProvider(policy.SeedProvider).client.Config()
		gm.Expect(cfg.Timeout).To(gm.Equal(60 * time.Millisecond))
		gm.Expect(policy.DiscoveryRefreshTimeout).To(gm.Equal(7 * time.Second))
		gm.Expect(policy.ConnectionQueueSize).To(gm.Equal(98))
	})

	gg.It("bounds the first refresh by refresh_timeout_ms when the file also sets init_timeout_ms", func() {
		setenvForSpec("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
		setenvForSpec("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
		setenvForSpec("AWS_SESSION_TOKEN", "")
		server := startSequencedDescribeStub(
			describeReply{delay: 300 * time.Millisecond, body: describeOne("10.0.0.2")},
		)
		defer server.Close()

		policy := awsPolicyFromYaml(`
static:
  client:
    discovery:
      provider: aws-tags
      refresh_interval_ms: 3600000
      refresh_timeout_ms: 2000
      aws:
        region: us-east-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
        timeout_ms: 60
        init_timeout_ms: 50
        endpoint: ` + server.URL + `
`)
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
	})

	gg.Describe("drops the file for an invalid aws block", func() {
		cases := []struct {
			intent   string
			yamlText string
			message  string
		}{
			{
				intent: "requires discovery.aws when provider is aws-tags",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
`,
				message: "static.client.discovery.aws is required with provider aws-tags",
			},
			{
				intent: "rejects discovery.aws when provider is omitted",
				yamlText: `
static:
  client:
    discovery:
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
`,
				message: "static.client.discovery.aws is only valid with provider aws-tags",
			},
			{
				intent: "rejects discovery.aws when provider is static",
				yamlText: `
static:
  client:
    discovery:
      provider: static
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
`,
				message: "static.client.discovery.aws is only valid with provider aws-tags",
			},
			{
				intent: "rejects discovery.aws when provider is dns",
				yamlText: `
static:
  client:
    discovery:
      provider: dns
      dns:
        names: [seeds.example.com]
        servers: ["127.0.0.1"]
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
`,
				message: "static.client.discovery.aws is only valid with provider aws-tags",
			},
			{
				intent: "requires region",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        filters:
          - name: tag:a
            values: [b]
`,
				message: "static.client.discovery.aws.region is required",
			},
			{
				intent: "requires filters",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
`,
				message: "static.client.discovery.aws.filters is required",
			},
			{
				intent: "rejects an empty filters list",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters: []
`,
				message: "Invalid static.client.discovery.aws.filters value '[]': expected a non-empty list of name and values",
			},
			{
				intent: "requires each filter name",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - values: [b]
`,
				message: "static.client.discovery.aws.filters[0].name is required",
			},
			{
				intent: "requires each filter values list",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
`,
				message: "static.client.discovery.aws.filters[0].values is required",
			},
			{
				intent: "rejects an empty filter values list",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: []
`,
				message: "Invalid static.client.discovery.aws.filters[0].values value '[]': expected a non-empty list of strings",
			},
			{
				intent: "rejects an unknown address_field",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        address_field: ipv6
`,
				message: "Invalid static.client.discovery.aws.address_field value 'ipv6': valid values: private_ip, public_ip, private_dns, public_dns",
			},
			{
				intent: "rejects a port above 65535",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        port: 70000
`,
				message: "Invalid static.client.discovery.aws.port value '70000': expected an integer from 1 to 65535",
			},
			{
				intent: "rejects timeout_ms 0",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        timeout_ms: 0
`,
				message: "Invalid static.client.discovery.aws.timeout_ms value '0': expected an integer from 1 to 2147483647",
			},
			{
				intent: "rejects an empty tls_name",
				yamlText: `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        tls_name: ""
`,
				message: "Invalid AWS EC2 seed provider tlsName: empty",
			},
		}

		for _, tc := range cases {
			tc := tc
			gg.It(tc.intent, func() {
				expectInvalidAwsYaml(tc.yamlText, tc.message)
			})
		}
	})

	gg.It("drops the file when the provider constructor rejects a YAML value", func() {
		yamlText := `
static:
  client:
    max_connections_per_node: 77
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        endpoint: ftp://ec2.example.com
`
		policy := NewClientPolicy()
		config := registrationHarness(policy, yamlText)
		config.initConfig()
		expectFileDropped(config, policy)
		gm.Expect(policy.copy().mapStatic(config).ConnectionQueueSize).To(gm.Equal(NewClientPolicy().ConnectionQueueSize))
	})

	gg.It("keeps the NewClient provider when a reload changes aws settings", func() {
		policy := NewClientPolicy()
		config := registrationHarness(policy, `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
        port: 3100
`)
		config.initConfig()
		first := policy.copy().mapStatic(config)
		awsProvider := asAwsProvider(first.SeedProvider)
		gm.Expect(awsProvider.client.Config().Port).To(gm.Equal(3100))

		config.skipFactoryBuild = true
		changed := &dynconfig.Config{}
		gm.Expect(yaml.Unmarshal([]byte(`
static:
  client:
    max_connections_per_node: 97
    discovery:
      provider: aws-tags
      aws:
        region: eu-west-1
        filters:
          - name: tag:a
            values: [other]
        port: 3000
`), changed)).To(gm.Succeed())
		config.configProvider.(*stubConfigProvider).config = changed
		config.initConfig()
		second := policy.copy().mapStatic(config)

		gm.Expect(second.SeedProvider).To(gm.BeIdenticalTo(first.SeedProvider))
		gm.Expect(second.ConnectionQueueSize).To(gm.Equal(97))
		gm.Expect(awsProvider.client.Config().Region).To(gm.Equal("us-east-1"))
		gm.Expect(awsProvider.client.Config().Port).To(gm.Equal(3100))
	})

	gg.It("requires a client restart when aws-tags first appears on reload", func() {
		policy := NewClientPolicy()
		config := registrationHarness(policy, `
static:
  client:
    discovery:
      provider: aws-tags
      aws:
        region: us-east-1
        filters:
          - name: tag:a
            values: [b]
`)
		config.skipFactoryBuild = true
		config.initConfig()

		expectFileDropped(config, policy)
		err := config.pendingFactoryToBeRegistered(config.configProvider.LoadConfig(""))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("restart the client to apply it"))
	})
})

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
	"errors"
	"strconv"
	"strings"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/internal/discovery/aws"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const awsConfigPath = "static.client.discovery.aws"

// AwsAddressField is the EC2 instance field returned as the seed address.
type AwsAddressField int

const (
	// AwsAddressFieldPrivateIP is privateIpAddress. This is the default.
	AwsAddressFieldPrivateIP AwsAddressField = iota
	// AwsAddressFieldPublicIP is ipAddress.
	AwsAddressFieldPublicIP
	// AwsAddressFieldPrivateDNS is privateDnsName.
	AwsAddressFieldPrivateDNS
	// AwsAddressFieldPublicDNS is dnsName.
	AwsAddressFieldPublicDNS
)

// AwsFilter is one DescribeInstances filter. instance-state-name is added by the provider.
type AwsFilter struct {
	Name   string
	Values []string
}

// AwsTagFilter returns a DescribeInstances filter named tag:<key>, the same as Java Builder.tagFilter.
func AwsTagFilter(key string, values ...string) AwsFilter {
	return AwsFilter{Name: "tag:" + key, Values: append([]string(nil), values...)}
}

// AwsCredentials are AWS keys used to sign requests. Expiration zero means none.
type AwsCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// AwsCredentialsHook replaces the built-in credential chain when set.
type AwsCredentialsHook func() (*AwsCredentials, error)

// AwsEc2TagSeedCandidateProviderConfig holds the settings for NewAwsEc2TagSeedCandidateProvider.
type AwsEc2TagSeedCandidateProviderConfig struct {
	// Region is the AWS region of the instances and of the EC2 and STS endpoints.
	Region string

	// Filters are DescribeInstances filters. At least one is required.
	// instance-state-name=running is always added and cannot be set.
	Filters []AwsFilter

	// AddressField is the instance field returned as the seed address.
	AddressField AwsAddressField //= AwsAddressFieldPrivateIP

	// Port is set on every host.
	Port int //= 3000

	// TLSName is set on every returned host. Empty leaves TLS name defaulting
	// to the same rule as constructor seeds.
	TLSName string //= ""

	// Timeout bounds every refresh after the first, credentials included.
	// ClientPolicy.DiscoveryRefreshTimeout bounds the first.
	Timeout time.Duration //= 80 milliseconds

	// Endpoint is an EC2 endpoint URL. Empty uses the regional endpoint.
	Endpoint string //= ""

	// Credentials are static keys, the first source of the credential chain.
	// Cannot be combined with CredentialsHook.
	Credentials *AwsCredentials

	// CredentialsHook replaces the built-in credential chain.
	CredentialsHook AwsCredentialsHook
}

type awsEc2TagSeedCandidateProvider struct {
	client *aws.Client
}

// NewAwsEc2TagSeedCandidateProvider validates config and returns a seed
// provider that lists running EC2 instances matching the filters.
func NewAwsEc2TagSeedCandidateProvider(config AwsEc2TagSeedCandidateProviderConfig) (SeedCandidateProvider, Error) {
	addressField, err := awsAddressField(config.AddressField)
	if err != nil {
		return nil, err
	}

	filters := make([]aws.Filter, len(config.Filters))
	for i, filter := range config.Filters {
		filters[i] = aws.Filter{Name: filter.Name, Values: append([]string(nil), filter.Values...)}
	}

	client, clientErr := aws.NewClient(aws.Config{
		Region:          config.Region,
		Filters:         filters,
		AddressField:    addressField,
		Port:            config.Port,
		TLSName:         config.TLSName,
		Timeout:         config.Timeout,
		Endpoint:        config.Endpoint,
		Credentials:     awsCredentials(config.Credentials),
		CredentialsHook: awsCredentialsHook(config.CredentialsHook),
	})
	if clientErr != nil {
		return nil, newError(types.PARAMETER_ERROR, clientErr.Error())
	}

	return &awsEc2TagSeedCandidateProvider{client: client}, nil
}

// RefreshSeedCandidates implements the SeedCandidateProvider interface.
func (provider *awsEc2TagSeedCandidateProvider) RefreshSeedCandidates(seedContext *SeedCandidateContext) ([]*Host, error) {
	if seedContext == nil {
		return nil, errors.New("SeedCandidateContext is required")
	}
	budget := provider.client.Config().Timeout
	if seedContext.TendIteration == 0 {
		budget = seedContext.refreshTimeout
		if budget <= 0 {
			budget = defaultDiscoveryRefreshTimeout
		}
	}
	candidates, err := provider.client.Lookup(budget)
	if err != nil {
		return nil, err
	}
	return hostsFromAWSCandidates(candidates), nil
}

// NeedsPeriodicRefresh implements the SeedCandidateProvider interface.
func (*awsEc2TagSeedCandidateProvider) NeedsPeriodicRefresh() bool {
	return true
}

// SupportsDeadline implements the SeedCandidateProvider interface.
func (*awsEc2TagSeedCandidateProvider) SupportsDeadline() bool {
	return false
}

// Close implements the SeedCandidateProvider interface.
func (*awsEc2TagSeedCandidateProvider) Close() {
}

func (provider *awsEc2TagSeedCandidateProvider) String() string {
	return provider.client.String()
}

func hostsFromAWSCandidates(candidates []aws.Candidate) []*Host {
	hosts := make([]*Host, len(candidates))
	for i, candidate := range candidates {
		hosts[i] = &Host{Name: candidate.Address, TLSName: candidate.TLSName, Port: candidate.Port}
	}
	return hosts
}

func awsAddressField(field AwsAddressField) (aws.AddressField, Error) {
	switch field {
	case AwsAddressFieldPrivateIP:
		return aws.AddressFieldPrivateIP, nil
	case AwsAddressFieldPublicIP:
		return aws.AddressFieldPublicIP, nil
	case AwsAddressFieldPrivateDNS:
		return aws.AddressFieldPrivateDNS, nil
	case AwsAddressFieldPublicDNS:
		return aws.AddressFieldPublicDNS, nil
	default:
		return 0, newError(types.PARAMETER_ERROR, "Invalid AWS EC2 seed provider addressField: "+strconv.Itoa(int(field)))
	}
}

func parseAwsAddressField(token string) (AwsAddressField, Error) {
	switch strings.ToLower(token) {
	case "private_ip":
		return AwsAddressFieldPrivateIP, nil
	case "public_ip":
		return AwsAddressFieldPublicIP, nil
	case "private_dns":
		return AwsAddressFieldPrivateDNS, nil
	case "public_dns":
		return AwsAddressFieldPublicDNS, nil
	}
	return AwsAddressFieldPrivateIP, newError(types.PARAMETER_ERROR, "Invalid "+awsConfigPath+".address_field value '"+token+"': valid values: private_ip, public_ip, private_dns, public_dns")
}

func awsCredentials(credentials *AwsCredentials) *aws.Credentials {
	if credentials == nil {
		return nil
	}
	return &aws.Credentials{
		AccessKeyID:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		SessionToken:    credentials.SessionToken,
		Expiration:      credentials.Expiration,
	}
}

func awsCredentialsHook(hook AwsCredentialsHook) aws.CredentialsHook {
	if hook == nil {
		return nil
	}
	return func() (*aws.Credentials, error) {
		credentials, err := hook()
		if err != nil {
			return nil, err
		}
		if credentials == nil {
			return nil, nil
		}
		return awsCredentials(credentials), nil
	}
}

func awsProviderFromConfig(block *dynconfig.AwsConfig) (SeedCandidateProvider, Error) {
	if block == nil {
		return nil, newError(types.PARAMETER_ERROR, awsConfigPath+".region is required")
	}

	config := AwsEc2TagSeedCandidateProviderConfig{}
	if block.Region != nil {
		config.Region = *block.Region
	}
	if block.Filters != nil {
		config.Filters = awsFiltersFromConfig(*block.Filters)
	}
	if block.AddressField != nil {
		field, err := parseAwsAddressField(*block.AddressField)
		if err != nil {
			return nil, err
		}
		config.AddressField = field
	}
	if block.Port != nil {
		config.Port = *block.Port
	}
	if block.TLSName != nil {
		config.TLSName = *block.TLSName
	}
	if block.TimeoutMs != nil {
		config.Timeout = time.Duration(*block.TimeoutMs) * time.Millisecond
	}
	if block.Endpoint != nil {
		config.Endpoint = *block.Endpoint
	}

	provider, err := NewAwsEc2TagSeedCandidateProvider(config)
	if err != nil {
		detail := err.Error()
		if ae, ok := err.(*AerospikeError); ok {
			detail = ae.msg
		}
		return nil, newError(types.PARAMETER_ERROR, "Invalid "+awsConfigPath+": "+detail)
	}
	return provider, nil
}

func awsFiltersFromConfig(filters []dynconfig.AwsFilter) []AwsFilter {
	out := make([]AwsFilter, len(filters))
	for i, filter := range filters {
		if filter.Name != nil {
			out[i].Name = *filter.Name
		}
		if filter.Values != nil {
			out[i].Values = append([]string(nil), *filter.Values...)
		}
	}
	return out
}

func validateAwsConfig(block *dynconfig.AwsConfig) Error {
	if block.Region == nil {
		return missingYamlKey(awsConfigPath + ".region")
	}
	if block.Filters == nil {
		return missingYamlKey(awsConfigPath + ".filters")
	}
	if len(*block.Filters) == 0 {
		return invalidYamlValue(awsConfigPath+".filters", "[]", "a non-empty list of name and values")
	}
	for i, filter := range *block.Filters {
		path := awsConfigPath + ".filters[" + strconv.Itoa(i) + "]"
		if filter.Name == nil {
			return missingYamlKey(path + ".name")
		}
		if filter.Values == nil {
			return missingYamlKey(path + ".values")
		}
		if len(*filter.Values) == 0 {
			return invalidYamlValue(path+".values", "[]", "a non-empty list of strings")
		}
	}
	if block.AddressField != nil {
		if _, err := parseAwsAddressField(*block.AddressField); err != nil {
			return err
		}
	}
	if err := validateYamlPort(awsConfigPath+".port", block.Port); err != nil {
		return err
	}
	if err := validateYamlTimeoutMs(awsConfigPath+".timeout_ms", block.TimeoutMs); err != nil {
		return err
	}
	if block.TLSName != nil && *block.TLSName == "" {
		return newError(types.PARAMETER_ERROR, "Invalid AWS EC2 seed provider tlsName: empty")
	}
	return nil
}

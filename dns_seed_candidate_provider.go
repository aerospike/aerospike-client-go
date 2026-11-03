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
	"strconv"
	"strings"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	"github.com/aerospike/aerospike-client-go/v8/internal/discovery/dns"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const dnsConfigPath = "static.client.discovery.dns"

// DnsRecordType is the DNS record type queried for each name.
type DnsRecordType int

const (
	// DnsRecordTypeA queries A and AAAA records. This is the default.
	DnsRecordTypeA DnsRecordType = iota
	// DnsRecordTypeSRV queries SRV records and resolves each target to an address.
	DnsRecordTypeSRV
)

// String returns the Java enum name: "A" or "SRV". YAML tokens stay lowercase.
func (recordType DnsRecordType) String() string {
	if recordType == DnsRecordTypeSRV {
		return "SRV"
	}
	return "A"
}

// DnsSeedCandidateProviderConfig holds the settings for NewDnsSeedCandidateProvider.
type DnsSeedCandidateProviderConfig struct {
	// Names are the DNS names to resolve. At least one is required. Names are
	// compared case-insensitively in absolute form. Empty names, ".", and names
	// that contain spaces are rejected.
	Names []string

	// RecordType is A (A and AAAA records) or SRV.
	RecordType DnsRecordType //= DnsRecordTypeA

	// Port is set on hosts from A and AAAA records. SRV uses each record's port.
	Port int //= 3000

	// TLSName is set on every returned host. Empty leaves TLS name defaulting
	// to the same rule as constructor seeds.
	TLSName string //= ""

	// Timeout bounds every I/O of one refresh, shared equally across Servers.
	// In thread mode ClientPolicy.DiscoveryRefreshTimeout bounds the first.
	Timeout time.Duration //= 80 milliseconds

	// Servers are resolver addresses, each an IP with an optional port. An
	// IPv6 address is bracketed when a port is given. Empty uses the system
	// resolvers, each on port 53.
	Servers []string //= nil
}

type dnsSeedCandidateProvider struct {
	resolver   *dns.Resolver
	tlsName    string
	timeout    time.Duration
	recordType DnsRecordType
}

// NewDnsSeedCandidateProvider validates config and returns a seed provider
// that resolves DNS names to IP literals on every refresh.
func NewDnsSeedCandidateProvider(config DnsSeedCandidateProviderConfig) (SeedCandidateProvider, Error) {
	recordType, err := dnsRecordType(config.RecordType)
	if err != nil {
		return nil, err
	}

	resolver, resolveErr := dns.NewResolver(dns.Config{
		Names:      append([]string(nil), config.Names...),
		RecordType: recordType,
		Port:       config.Port,
		Timeout:    config.Timeout,
		Servers:    append([]string(nil), config.Servers...),
	})
	if resolveErr != nil {
		return nil, newError(types.PARAMETER_ERROR, resolveErr.Error())
	}

	return &dnsSeedCandidateProvider{
		resolver:   resolver,
		tlsName:    config.TLSName,
		timeout:    resolver.Config().Timeout,
		recordType: config.RecordType,
	}, nil
}

// RefreshSeedCandidates implements the SeedCandidateProvider interface.
func (provider *dnsSeedCandidateProvider) RefreshSeedCandidates(seedContext *SeedCandidateContext) ([]*Host, error) {
	budget := provider.timeout
	if seedContext != nil && seedContext.TendIteration == 0 && seedContext.refreshTimeout > 0 {
		budget = seedContext.refreshTimeout
	}
	candidates, err := provider.resolver.Lookup(budget)
	if err != nil {
		return nil, err
	}
	return hostsFromDNSCandidates(candidates, provider.tlsName), nil
}

// NeedsPeriodicRefresh implements the SeedCandidateProvider interface.
func (*dnsSeedCandidateProvider) NeedsPeriodicRefresh() bool {
	return true
}

// SupportsDeadline implements the SeedCandidateProvider interface.
func (*dnsSeedCandidateProvider) SupportsDeadline() bool {
	return true
}

// Close implements the SeedCandidateProvider interface.
func (*dnsSeedCandidateProvider) Close() {
}

func (provider *dnsSeedCandidateProvider) String() string {
	return "dns " + strings.Join(provider.resolver.Config().Names, ",") + " " + provider.recordType.String()
}

func hostsFromDNSCandidates(candidates []dns.Candidate, tlsName string) []*Host {
	hosts := make([]*Host, 0, len(candidates))
	for _, candidate := range candidates {
		hosts = append(hosts, &Host{Name: candidate.Address, TLSName: tlsName, Port: candidate.Port})
	}
	return hosts
}

func dnsRecordType(recordType DnsRecordType) (dns.RecordType, Error) {
	switch recordType {
	case DnsRecordTypeA:
		return dns.RecordTypeA, nil
	case DnsRecordTypeSRV:
		return dns.RecordTypeSRV, nil
	default:
		return 0, newError(types.PARAMETER_ERROR, "Invalid DNS seed provider recordType: "+strconv.Itoa(int(recordType)))
	}
}

func parseDnsRecordType(token string) (DnsRecordType, Error) {
	switch strings.ToLower(token) {
	case "a":
		return DnsRecordTypeA, nil
	case "srv":
		return DnsRecordTypeSRV, nil
	}
	return DnsRecordTypeA, newError(types.PARAMETER_ERROR, "Invalid "+dnsConfigPath+".record_type value '"+token+"': valid values: a, srv")
}

func dnsProviderFromConfig(block *dynconfig.DnsConfig) (SeedCandidateProvider, Error) {
	if block == nil {
		return nil, newError(types.PARAMETER_ERROR, dnsConfigPath+".names is required")
	}

	config := DnsSeedCandidateProviderConfig{}
	if block.Names != nil {
		config.Names = append([]string(nil), *block.Names...)
	}
	if block.RecordType != nil {
		recordType, err := parseDnsRecordType(*block.RecordType)
		if err != nil {
			return nil, err
		}
		config.RecordType = recordType
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
	if block.Servers != nil {
		config.Servers = append([]string(nil), *block.Servers...)
	}

	provider, err := NewDnsSeedCandidateProvider(config)
	if err != nil {
		detail := err.Error()
		if ae, ok := err.(*AerospikeError); ok {
			detail = ae.msg
		}
		return nil, newError(types.PARAMETER_ERROR, "Invalid "+dnsConfigPath+": "+detail)
	}
	return provider, nil
}

func validateDnsConfig(block *dynconfig.DnsConfig) Error {
	if block.Names == nil {
		return missingYamlKey(dnsConfigPath + ".names")
	}
	if len(*block.Names) == 0 {
		return invalidYamlValue(dnsConfigPath+".names", "[]", "a non-empty list of strings")
	}
	if block.RecordType != nil {
		if _, err := parseDnsRecordType(*block.RecordType); err != nil {
			return err
		}
	}
	if err := validateYamlPort(dnsConfigPath+".port", block.Port); err != nil {
		return err
	}
	if err := validateYamlTimeoutMs(dnsConfigPath+".timeout_ms", block.TimeoutMs); err != nil {
		return err
	}
	if block.Servers != nil && len(*block.Servers) == 0 {
		return invalidYamlValue(dnsConfigPath+".servers", "[]", "a non-empty list of strings")
	}
	if block.TLSName != nil && *block.TLSName == "" {
		return newError(types.PARAMETER_ERROR, "Invalid DNS seed provider tlsName: empty")
	}
	return nil
}

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

package aws

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// DefaultPort is set on every host.
	DefaultPort = 3000

	// DefaultTimeout bounds a later refresh, credentials included.
	DefaultTimeout = 80 * time.Millisecond

	apiVersion  = "2016-11-15"
	stateFilter = "instance-state-name"
	formType    = "application/x-www-form-urlencoded; charset=utf-8"
	ec2Service  = "ec2"
)

var regionPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)+$`)

// AddressField is the instance field returned as the seed address.
type AddressField int

const (
	// AddressFieldPrivateIP is privateIpAddress. This is the default.
	AddressFieldPrivateIP AddressField = iota
	// AddressFieldPublicIP is ipAddress.
	AddressFieldPublicIP
	// AddressFieldPrivateDNS is privateDnsName.
	AddressFieldPrivateDNS
	// AddressFieldPublicDNS is dnsName.
	AddressFieldPublicDNS
)

// Element is the DescribeInstances XML child name for this field.
func (field AddressField) Element() string {
	switch field {
	case AddressFieldPublicIP:
		return "ipAddress"
	case AddressFieldPrivateDNS:
		return "privateDnsName"
	case AddressFieldPublicDNS:
		return "dnsName"
	default:
		return "privateIpAddress"
	}
}

func (field AddressField) valid() bool {
	return field >= AddressFieldPrivateIP && field <= AddressFieldPublicDNS
}

// Filter is one DescribeInstances filter. instance-state-name is added by the provider.
type Filter struct {
	Name   string
	Values []string
}

// Credentials are AWS keys used to sign requests. Expiration zero means none.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

func (credentials Credentials) String() string {
	return fmt.Sprintf("Credentials{accessKeyId=%s, expiration=%d}", credentials.AccessKeyID, expirationMillis(credentials.Expiration))
}

func expirationMillis(expiration time.Time) int64 {
	if expiration.IsZero() {
		return 0
	}
	return expiration.UnixMilli()
}

// CredentialsHook replaces the built-in credential chain when set.
type CredentialsHook func() (*Credentials, error)

// Config is the validated settings for a Client.
type Config struct {
	Region          string
	Filters         []Filter
	AddressField    AddressField
	Port            int
	TLSName         string
	Timeout         time.Duration
	Endpoint        string
	Credentials     *Credentials
	CredentialsHook CredentialsHook
	Env             Environment // tests only; nil uses SystemEnvironment
}

func normalize(cfg Config) (Config, *url.URL, string, string, error) {
	if !regionPattern.MatchString(cfg.Region) {
		shown := cfg.Region
		if shown == "" {
			shown = "null"
		}
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider region: '%s'", shown)
	}

	filters, err := copyFilters(cfg.Filters)
	if err != nil {
		return Config{}, nil, "", "", err
	}

	if !cfg.AddressField.valid() {
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider addressField: %d", int(cfg.AddressField))
	}

	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	if port < 1 || port > 65535 {
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider port: %d", port)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout <= 0 {
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider timeout: %s", timeout)
	}

	if cfg.Credentials != nil && cfg.CredentialsHook != nil {
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider credentials: credentials and credentialsHook cannot both be set")
	}
	if cfg.Credentials != nil {
		if err := validateCredentials(*cfg.Credentials); err != nil {
			return Config{}, nil, "", "", err
		}
	}

	env := cfg.Env
	if env == nil {
		env = SystemEnvironment{}
	}

	target := cfg.Endpoint
	if target == "" {
		target = env.EC2Endpoint(cfg.Region)
	}
	uri, err := url.Parse(target)
	if err != nil || !validEndpoint(uri) {
		if err != nil {
			return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider endpoint: '%s'", target)
		}
		return Config{}, nil, "", "", fmt.Errorf("Invalid AWS EC2 seed provider endpoint: '%s': expected http or https URL with a host and no query", target)
	}

	path := uri.EscapedPath()
	if path == "" {
		path = "/"
	}

	return Config{
		Region:          cfg.Region,
		Filters:         filters,
		AddressField:    cfg.AddressField,
		Port:            port,
		TLSName:         cfg.TLSName,
		Timeout:         timeout,
		Endpoint:        cfg.Endpoint,
		Credentials:     copyCredentials(cfg.Credentials),
		CredentialsHook: cfg.CredentialsHook,
		Env:             env,
	}, uri, hostHeader(uri), path, nil
}

func copyFilters(filters []Filter) ([]Filter, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("Invalid AWS EC2 seed provider filters: at least one filter is required")
	}

	order := make([]string, 0, len(filters))
	byName := make(map[string][]string, len(filters))
	for _, filter := range filters {
		name := filter.Name
		if name == "" {
			return nil, fmt.Errorf("Invalid AWS EC2 seed provider filter name: '%s'", name)
		}
		if name == stateFilter {
			return nil, fmt.Errorf("Invalid AWS EC2 seed provider filter name: '%s': the provider adds %s=running", name, stateFilter)
		}
		if len(filter.Values) == 0 {
			return nil, fmt.Errorf("Invalid AWS EC2 seed provider filter '%s' values: empty", name)
		}
		values := make([]string, 0, len(filter.Values))
		for _, value := range filter.Values {
			if value == "" {
				return nil, fmt.Errorf("Invalid AWS EC2 seed provider filter '%s' value: '%s'", name, value)
			}
			values = append(values, value)
		}
		if _, exists := byName[name]; !exists {
			order = append(order, name)
		}
		byName[name] = values
	}

	out := make([]Filter, 0, len(order))
	for _, name := range order {
		out = append(out, Filter{Name: name, Values: byName[name]})
	}
	return out, nil
}

func validateCredentials(credentials Credentials) error {
	if credentials.AccessKeyID == "" {
		return fmt.Errorf("Invalid AWS EC2 seed provider credentials: access key id is empty")
	}
	if credentials.SecretAccessKey == "" {
		return fmt.Errorf("Invalid AWS EC2 seed provider credentials: secret access key is empty")
	}
	return nil
}

func copyCredentials(credentials *Credentials) *Credentials {
	if credentials == nil {
		return nil
	}
	copied := *credentials
	return &copied
}

func validEndpoint(uri *url.URL) bool {
	if uri == nil || uri.Host == "" || uri.RawQuery != "" || uri.Fragment != "" || strings.Contains(uri.Host, " ") {
		return false
	}
	scheme := strings.ToLower(uri.Scheme)
	return scheme == "http" || scheme == "https"
}

func describeQuery(filters []Filter) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Action=DescribeInstances&Version=%s", apiVersion)
	for i, filter := range filters {
		appendFilter(&b, i+1, filter.Name, filter.Values)
	}
	appendFilter(&b, len(filters)+1, stateFilter, []string{"running"})
	return b.String()
}

func appendFilter(b *strings.Builder, n int, name string, values []string) {
	fmt.Fprintf(b, "&Filter.%d.Name=%s", n, encode(name))
	for i, value := range values {
		fmt.Fprintf(b, "&Filter.%d.Value.%d=%s", n, i+1, encode(value))
	}
}

func formatFilters(filters []Filter) string {
	parts := make([]string, len(filters))
	for i, filter := range filters {
		parts[i] = filter.Name + "=[" + strings.Join(filter.Values, ", ") + "]"
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

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

package dns

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultPort is set on hosts resolved from A and AAAA records.
	DefaultPort = 3000

	// DefaultTimeout bounds every I/O of one refresh.
	DefaultTimeout = 80 * time.Millisecond

	dnsPort = 53
)

// RecordType is the DNS record type queried for each name.
type RecordType int

const (
	// RecordTypeA queries A and AAAA records.
	RecordTypeA RecordType = iota
	// RecordTypeSRV queries SRV records and resolves each target to an address.
	RecordTypeSRV
)

// Config is the validated settings for a Resolver.
type Config struct {
	Names      []string
	RecordType RecordType
	Port       int
	Timeout    time.Duration
	// Servers are the original resolver addresses. Empty uses the system resolvers.
	Servers []string
}

func normalize(cfg Config) (Config, []string, error) {
	if len(cfg.Names) == 0 {
		return Config{}, nil, fmt.Errorf("Invalid DNS seed provider names: at least one name is required")
	}

	seen := make(map[string]struct{}, len(cfg.Names))
	names := make([]string, 0, len(cfg.Names))
	for _, name := range cfg.Names {
		if name == "" || name == "." || strings.Contains(name, " ") {
			return Config{}, nil, fmt.Errorf("Invalid DNS seed provider name: '%s'", name)
		}
		key := strings.ToLower(absolute(name))
		if _, dup := seen[key]; dup {
			return Config{}, nil, fmt.Errorf("Invalid DNS seed provider names: duplicate name '%s'", name)
		}
		seen[key] = struct{}{}
		names = append(names, name)
	}

	switch cfg.RecordType {
	case RecordTypeA, RecordTypeSRV:
	default:
		return Config{}, nil, fmt.Errorf("Invalid DNS seed provider recordType: %d", cfg.RecordType)
	}

	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	if port < 1 || port > 65535 {
		return Config{}, nil, fmt.Errorf("Invalid DNS seed provider port: %d", port)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout <= 0 {
		return Config{}, nil, fmt.Errorf("Invalid DNS seed provider timeout: %s", timeout)
	}

	var servers, dial []string
	for _, server := range cfg.Servers {
		addr, err := parseServer(server)
		if err != nil {
			return Config{}, nil, err
		}
		servers = append(servers, server)
		dial = append(dial, addr)
	}

	return Config{
		Names:      names,
		RecordType: cfg.RecordType,
		Port:       port,
		Timeout:    timeout,
		Servers:    servers,
	}, dial, nil
}

func absolute(name string) string {
	if strings.HasSuffix(name, ".") {
		return name
	}
	return name + "."
}

// parseServer turns a Java-style resolver address into host:port for Dial.
// IP only uses port 53. Bracketed IPv6 without a port is allowed. Hostnames
// are rejected.
func parseServer(server string) (string, error) {
	if server == "" {
		return "", fmt.Errorf("Invalid DNS seed provider server: null")
	}

	host, port := server, dnsPort
	if h, p, err := net.SplitHostPort(server); err == nil {
		n, convErr := strconv.Atoi(p)
		if convErr != nil {
			return "", fmt.Errorf("Invalid DNS seed provider server: '%s': invalid port", server)
		}
		host, port = h, n
	} else if strings.HasPrefix(server, "[") {
		end := strings.IndexByte(server, ']')
		if end < 0 || end+1 != len(server) {
			return "", fmt.Errorf("Invalid DNS seed provider server: '%s'", server)
		}
		host = server[1:end]
	}

	if net.ParseIP(host) == nil {
		return "", fmt.Errorf("Invalid DNS seed provider server: '%s': expected an IP address", server)
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("Invalid DNS seed provider server: '%s': invalid port", server)
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

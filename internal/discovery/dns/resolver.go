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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
)

var errNoSRVTarget = errors.New("no SRV target resolved")
var errEmptyAnswer = errors.New("empty answer")

// Candidate is one resolved seed address. Address is an IP literal.
type Candidate struct {
	Address string
	Port    int
}

type nameFailure struct {
	name   string
	reason string
}

type candidateKey struct {
	address string
	port    int
}

type srvRecord struct {
	priority int
	weight   int
	port     int
	target   string
}

// Resolver resolves configured DNS names to IP literals.
type Resolver struct {
	config Config
	net    *net.Resolver
	warn   func(string)
}

// NewResolver validates cfg and returns a resolver. warn is optional.
func NewResolver(cfg Config, warn func(string)) (*Resolver, error) {
	normalized, dial, err := normalize(cfg)
	if err != nil {
		return nil, err
	}

	resolver := &net.Resolver{PreferGo: true}
	if len(dial) > 0 {
		servers := append([]string(nil), dial...)
		resolver.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			var last error
			for _, server := range servers {
				conn, err := dialer.DialContext(ctx, network, server)
				if err == nil {
					return conn, nil
				}
				last = err
			}
			return nil, last
		}
	}

	return &Resolver{config: normalized, net: resolver, warn: warn}, nil
}

// Config returns the validated settings.
func (resolver *Resolver) Config() Config {
	return resolver.config
}

// Lookup resolves every name. Partial failures are dropped with a warning.
// An error is returned only when every name fails. I/O is bounded by Timeout.
func (resolver *Resolver) Lookup() ([]Candidate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), resolver.config.Timeout)
	defer cancel()

	hosts := make([]Candidate, 0)
	seen := make(map[candidateKey]struct{})
	failures := make([]nameFailure, 0)

	for _, name := range resolver.config.Names {
		list, err := resolver.lookupName(ctx, name)
		if err != nil {
			failures = append(failures, nameFailure{name: name, reason: lookupReason(err)})
			continue
		}
		if len(list) == 0 {
			failures = append(failures, nameFailure{name: name, reason: errEmptyAnswer.Error()})
			continue
		}
		for _, candidate := range list {
			key := candidateKey{address: candidate.Address, port: candidate.Port}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			hosts = append(hosts, candidate)
		}
	}

	if len(hosts) == 0 {
		return nil, everyNameFailedError(failures)
	}
	for _, failure := range failures {
		resolver.warning("DNS seed name %s failed: %s", failure.name, failure.reason)
	}
	return hosts, nil
}

func (resolver *Resolver) lookupName(ctx context.Context, name string) ([]Candidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver.config.RecordType == RecordTypeSRV {
		return resolver.resolveSRV(ctx, name)
	}
	return resolver.resolveA(ctx, name)
}

func (resolver *Resolver) resolveA(ctx context.Context, name string) ([]Candidate, error) {
	addresses, err := resolver.addresses(ctx, name, false)
	if err != nil {
		return nil, err
	}
	list := make([]Candidate, 0, len(addresses))
	for _, address := range addresses {
		list = append(list, Candidate{Address: address, Port: resolver.config.Port})
	}
	return list, nil
}

func (resolver *Resolver) resolveSRV(ctx context.Context, name string) ([]Candidate, error) {
	_, addrs, err := resolver.net.LookupSRV(ctx, "", "", name)
	if err != nil {
		return nil, err
	}

	records := make([]srvRecord, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		target := strings.ToLower(absolute(addr.Target))
		if strings.Trim(target, ".") == "" {
			continue
		}
		records = append(records, srvRecord{
			priority: int(addr.Priority),
			weight:   int(addr.Weight),
			port:     int(addr.Port),
			target:   target,
		})
	}
	if len(records) == 0 {
		return nil, errEmptyAnswer
	}

	sort.SliceStable(records, func(i, j int) bool {
		if records[i].priority != records[j].priority {
			return records[i].priority < records[j].priority
		}
		if records[i].weight != records[j].weight {
			return records[i].weight > records[j].weight
		}
		if records[i].target != records[j].target {
			return records[i].target < records[j].target
		}
		return records[i].port < records[j].port
	})

	tried := make(map[string]*string, len(records))
	list := make([]Candidate, 0, len(records))
	for _, record := range records {
		cached, exists := tried[record.target]
		if !exists {
			addresses, lookupErr := resolver.addresses(ctx, record.target, true)
			if lookupErr != nil || len(addresses) == 0 {
				reason := errEmptyAnswer.Error()
				if lookupErr != nil {
					reason = lookupReason(lookupErr)
				}
				resolver.warning("DNS seed name %s SRV target %s dropped: %s", name, record.target, reason)
				tried[record.target] = nil
				continue
			}
			address := addresses[0]
			tried[record.target] = &address
			cached = &address
		}
		if cached == nil {
			continue
		}
		list = append(list, Candidate{Address: *cached, Port: record.port})
	}
	if len(list) == 0 {
		return nil, errNoSRVTarget
	}
	return list, nil
}

func (resolver *Resolver) addresses(ctx context.Context, name string, first bool) ([]string, error) {
	var addresses []string
	var last error

	for _, network := range []string{"ip4", "ip6"} {
		if first && len(addresses) > 0 {
			break
		}
		ips, err := resolver.net.LookupIP(ctx, network, name)
		if err != nil {
			last = err
			continue
		}
		values := make([]string, 0, len(ips))
		for _, ip := range ips {
			if s := ipString(ip); s != "" {
				values = append(values, s)
			}
		}
		sort.Strings(values)
		addresses = append(addresses, values...)
	}

	if len(addresses) == 0 {
		if last != nil {
			return nil, last
		}
		return nil, errEmptyAnswer
	}
	return addresses, nil
}

func (resolver *Resolver) warning(format string, args ...any) {
	if resolver.warn == nil {
		return
	}
	resolver.warn(fmt.Sprintf(format, args...))
}

func ipString(ip net.IP) string {
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

func lookupReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errEmptyAnswer) {
		return errEmptyAnswer.Error()
	}
	if errors.Is(err, errNoSRVTarget) {
		return errNoSRVTarget.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTimeout {
			return "timeout"
		}
		if dnsErr.IsNotFound {
			return "NXDOMAIN"
		}
		if strings.Contains(dnsErr.Err, "server misbehaving") {
			return "SERVFAIL"
		}
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "no such host"):
		return "NXDOMAIN"
	case strings.Contains(msg, "server misbehaving"):
		return "SERVFAIL"
	default:
		return msg
	}
}

func everyNameFailedError(failures []nameFailure) error {
	var b strings.Builder
	b.WriteString("DNS seed discovery failed for every name:")
	for i, failure := range failures {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteByte(' ')
		b.WriteString(failure.name)
		b.WriteString(": ")
		b.WriteString(failure.reason)
	}
	return errors.New(b.String())
}

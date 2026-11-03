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
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
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
	// servers holds one resolver per configured server, in order, or one
	// system resolver when none are configured.
	servers []*net.Resolver
}

// NewResolver validates cfg and returns a resolver.
func NewResolver(cfg Config) (*Resolver, error) {
	normalized, dial, err := normalize(cfg)
	if err != nil {
		return nil, err
	}

	if len(dial) == 0 {
		return &Resolver{config: normalized, servers: []*net.Resolver{{PreferGo: true}}}, nil
	}
	servers := make([]*net.Resolver, len(dial))
	for i, address := range dial {
		address := address
		servers[i] = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, address)
			},
		}
	}
	return &Resolver{config: normalized, servers: servers}, nil
}

// ask sends one query to each server in turn until one answers. Each server
// gets an equal share of the time left, so a silent server cannot use it all.
// NXDOMAIN is an answer and ends the query.
func ask[T any](ctx context.Context, servers []*net.Resolver, query func(context.Context, *net.Resolver) (T, error)) (T, error) {
	var zero T
	var last error
	for i, server := range servers {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		deadline, _ := ctx.Deadline()
		attempt, cancel := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(servers)-i))
		result, err := query(attempt, server)
		cancel()
		if err == nil {
			return result, nil
		}
		last = err
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return zero, err
		}
	}
	return zero, last
}

// Config returns the validated settings.
func (resolver *Resolver) Config() Config {
	return resolver.config
}

// Lookup resolves every name. Partial failures are dropped with a warning.
// An error is returned only when every name fails. All I/O is bounded by budget.
func (resolver *Resolver) Lookup(budget time.Duration) ([]Candidate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
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
	addrs, err := ask(ctx, resolver.servers, func(ctx context.Context, server *net.Resolver) ([]*net.SRV, error) {
		_, addrs, err := server.LookupSRV(ctx, "", "", name)
		return addrs, err
	})
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
		ips, err := ask(ctx, resolver.servers, func(ctx context.Context, server *net.Resolver) ([]net.IP, error) {
			return server.LookupIP(ctx, network, name)
		})
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
	logger.Logger.Warn(format, args...)
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

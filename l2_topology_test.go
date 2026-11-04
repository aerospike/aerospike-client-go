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

package aerospike_test

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	asl "github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	l2TendInterval = 250 * time.Millisecond
	l2Wait         = 30 * time.Second
	l2Poll         = 100 * time.Millisecond
	l2DialTimeout  = 2 * time.Second
	l2Set          = "l2"
	l2KeyCount     = 300
	l2MissKeyCount = 200
	l2MissRetries  = 3
	// Longer than the test, so no periodic refresh can run during it.
	l2ReseedRefreshInterval = 10 * time.Minute
	l2ReseedPoll            = 5 * time.Millisecond
	l2TLSProxySuffix        = "-tls"
	l2TLSKeyCount           = 20
	l2HoldToxic             = "hold"
	l2DropThreads           = 4
	l2DropKeyCount          = 20
	l2DropSocketTimeout     = 500 * time.Millisecond
	l2DropTotalTimeout      = 1500 * time.Millisecond
	l2DropMaxRetries        = 2
	l2BreakerMaxErrorRate   = 2
	l2BreakerWindow         = 4
)

var l2PeerPattern = regexp.MustCompile(`\[([0-9A-Fa-f]+),([^,\[\]]*),\[`)

type l2Node struct {
	proxy      string
	port       int
	name       string
	advertised []string

	// Set only when the topology has TLS listeners.
	tlsPort       int
	tlsName       string
	tlsAdvertised []string
}

// l2EmptyTranslator maps no address.
type l2EmptyTranslator struct{}

func (l2EmptyTranslator) Translate(as.Endpoint) (as.Host, bool) {
	return as.Host{}, false
}

// l2CountingSeedProvider returns fixed seeds and counts its calls.
type l2CountingSeedProvider struct {
	seeds []*as.Host
	calls atomic.Int32
}

func (provider *l2CountingSeedProvider) RefreshSeedCandidates(*as.SeedCandidateContext) ([]*as.Host, error) {
	provider.calls.Add(1)
	return provider.seeds, nil
}

func (provider *l2CountingSeedProvider) NeedsPeriodicRefresh() bool { return true }

func (provider *l2CountingSeedProvider) SupportsDeadline() bool { return true }

func (provider *l2CountingSeedProvider) Close() {}

func (provider *l2CountingSeedProvider) callCount() int {
	return int(provider.calls.Load())
}

// l2LogCapture records every client log line and forwards it to the logger it replaced.
type l2LogCapture struct {
	forward interface{ Printf(string, ...any) }

	mu    sync.Mutex
	lines []string
}

func (capture *l2LogCapture) Printf(format string, v ...any) {
	capture.mu.Lock()
	capture.lines = append(capture.lines, fmt.Sprintf(format, v...))
	capture.mu.Unlock()
	capture.forward.Printf(format, v...)
}

// misses returns the strict translation miss warnings for node.
func (capture *l2LogCapture) misses(node string) []string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var misses []string
	for _, line := range capture.lines {
		if strings.Contains(line, "has no mapping for node "+node+" ") {
			misses = append(misses, line)
		}
	}
	return misses
}

// l2Failure is one command that failed under l2Load.
type l2Failure struct {
	start   time.Time
	elapsed time.Duration
	err     as.Error
}

// codes returns the result codes down the error chain, outermost first.
func (failure l2Failure) codes() string {
	var codes []string
	for err := error(failure.err); err != nil; err = errors.Unwrap(err) {
		if aerospikeErr, ok := err.(*as.AerospikeError); ok {
			codes = append(codes, aerospikeErr.ResultCode.String())
		}
	}
	return strings.Join(codes, ">")
}

func (failure l2Failure) end() time.Time {
	return failure.start.Add(failure.elapsed)
}

// l2Load runs a command in a loop on several goroutines until stopped.
type l2Load struct {
	stop      chan struct{}
	stopOnce  sync.Once
	done      sync.WaitGroup
	successes atomic.Int64

	mu     sync.Mutex
	failed []l2Failure
}

func startL2Load(threads int, command func(thread, i int) as.Error) *l2Load {
	load := &l2Load{stop: make(chan struct{})}
	for thread := range threads {
		load.done.Add(1)
		go func() {
			defer load.done.Done()
			for i := 0; ; i++ {
				select {
				case <-load.stop:
					return
				default:
				}
				start := time.Now()
				err := command(thread, i)
				if err == nil {
					load.successes.Add(1)
					continue
				}
				failure := l2Failure{start: start, elapsed: time.Since(start), err: err}
				load.mu.Lock()
				load.failed = append(load.failed, failure)
				load.mu.Unlock()
			}
		}()
	}
	return load
}

// stopWithin stops the load and reports whether every goroutine returned within bound.
func (load *l2Load) stopWithin(bound time.Duration) bool {
	load.stopOnce.Do(func() { close(load.stop) })
	finished := make(chan struct{})
	go func() {
		load.done.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-time.After(bound):
		return false
	}
}

func (load *l2Load) String() string {
	chains := map[string]int{}
	for _, failure := range load.failures() {
		chains[failure.codes()]++
	}
	return fmt.Sprintf("%d successes, failures by error chain %v", load.successes.Load(), chains)
}

// failures returns the failures with any of codes down their error chain, or
// all of them if none is given.
func (load *l2Load) failures(codes ...types.ResultCode) []l2Failure {
	load.mu.Lock()
	defer load.mu.Unlock()
	if len(codes) == 0 {
		return slices.Clone(load.failed)
	}
	var failures []l2Failure
	for _, failure := range load.failed {
		if failure.err.Matches(codes...) {
			failures = append(failures, failure)
		}
	}
	return failures
}

var (
	l2Once  sync.Once
	l2Host  string
	l2Nodes []l2Node
	l2Err   error
)

func TestL2PortMapByHostPortJoinsAllNodes(t *testing.T) {
	assertL2PortMapJoins(t, l2PortMapByEndpoint)
}

func TestL2PortMapByNodeNameJoinsAllNodes(t *testing.T) {
	assertL2PortMapJoins(t, l2PortMapByNodeName)
}

// The dns provider seeds from SRV records for the endpoint, one per listener,
// and port-map brings in the peers.
func TestL2DnsProviderSeedsThroughEndpoint(t *testing.T) {
	host, nodes := l2Topology(t)
	server := os.Getenv("DISCOVERY_DNS_SERVER")
	if server == "" {
		t.Skip("N-4 needs DISCOVERY_DNS_SERVER, the topology has no DNS server")
	}
	provider, err := as.NewDnsSeedCandidateProvider(as.DnsSeedCandidateProviderConfig{
		Names:      []string{"_aerospike._tcp." + host},
		RecordType: as.DnsRecordTypeSRV,
		Servers:    []string{server},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	policy.SeedProvider = provider
	client := newL2Client(t, policy)
	waitL2Cluster(t, client, l2NodeNames(nodes))
}

// Every node masters partitions as its own Node, and each key is served by its
// master with no server proxy hop.
func TestL2EveryPartitionOwnerIsADistinctNode(t *testing.T) {
	host, nodes := l2Topology(t)
	client := newL2Client(t, l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes)), as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))

	masters := as.PartitionMasters(client, *namespace)
	distinct := make(map[*as.Node]struct{}, len(nodes))
	for _, master := range masters {
		distinct[master] = struct{}{}
	}
	if len(distinct) != len(nodes) {
		t.Fatalf("%d distinct partition masters, want %d", len(distinct), len(nodes))
	}
	cluster := client.Cluster()
	for _, node := range client.GetNodes() {
		if byName, err := cluster.GetNodeByName(node.GetName()); err != nil || byName != node {
			t.Fatalf("cluster lookup of %s returned %v, %v, want the client's node", node.GetName(), byName, err)
		}
		if _, ok := distinct[node]; !ok {
			t.Fatalf("node %s masters no partition", node.GetName())
		}
	}

	before := make(map[string]map[string]int64, len(nodes))
	for _, node := range nodes {
		before[node.name] = l2NamespaceStats(t, host, node)
	}

	write := as.NewWritePolicy(0, 0)
	write.ReplicaPolicy = as.MASTER
	read := as.NewPolicy()
	read.ReplicaPolicy = as.MASTER
	served := make(map[string]int64, len(nodes))
	for i := range l2KeyCount {
		key := l2Key(t, "n5-", i)
		owner := masters[key.PartitionId()]
		assertL2RoutedTo(t, cluster, &write.BasePolicy, read, key, owner)
		if err := client.Put(write, key, as.BinMap{"v": i}); err != nil {
			t.Fatalf("put %v: %v", key, err)
		}
		record, err := client.Get(read, key)
		if err != nil {
			t.Fatalf("get %v: %v", key, err)
		}
		if record.Bins["v"] != i {
			t.Fatalf("key %v holds %v, want %d", key, record.Bins["v"], i)
		}
		served[owner.GetName()]++
	}

	for _, node := range nodes {
		after := l2NamespaceStats(t, host, node)
		delta := func(stat string) int64 { return after[stat] - before[node.name][stat] }
		want := served[node.name]
		if want == 0 {
			t.Fatalf("node %s served no keys", node.name)
		}
		if writes := delta("client_write_success"); writes != want {
			t.Fatalf("node %s counted %d writes, want %d", node.name, writes, want)
		}
		if reads := delta("client_read_success"); reads != want {
			t.Fatalf("node %s counted %d reads, want %d", node.name, reads, want)
		}
		if proxied := delta("client_proxy_complete") + delta("client_proxy_error") + delta("client_proxy_timeout"); proxied != 0 {
			t.Fatalf("node %s proxied %d commands, want none", node.name, proxied)
		}
	}
}

// Startup must settle within ClientPolicy.Timeout, and a dial to an advertised
// address the network drops takes that whole timeout. Java fails init with
// "Peers not reachable" here instead.
func TestL2WithoutTranslatorFailsInitWithTimeout(t *testing.T) {
	host, nodes := l2Topology(t)
	client, err := as.NewClientWithPolicyAndHost(l2Policy(), as.NewHost(host, nodes[0].port))
	if err == nil {
		client.Close()
		t.Fatalf("client without a translator started with nodes %v, want TIMEOUT", client.GetNodeNames())
	}
	if !err.Matches(types.TIMEOUT) {
		t.Fatalf("client without a translator failed with %v, want TIMEOUT", err)
	}
}

// The nodes advertise no alternate address, so asking for one fails init
// with an error naming the flag rather than starting with no nodes.
func TestL2UseServicesAlternateWithoutAlternateFailsInit(t *testing.T) {
	host, nodes := l2Topology(t)
	seed := nodes[0]
	info, err := l2RequestInfo(host, seed.port, "service-clear-alt")
	if err != nil {
		t.Fatal(err)
	}
	if alternate := strings.TrimSpace(info["service-clear-alt"]); alternate != "" {
		t.Fatalf("node %s advertises alternate address %s, want none", seed.name, alternate)
	}

	policy := l2Policy()
	policy.UseServicesAlternate = true
	client, initErr := as.NewClientWithPolicyAndHost(policy, as.NewHost(host, seed.port))
	if initErr == nil {
		client.Close()
		t.Fatalf("client with UseServicesAlternate started with nodes %v, want init to fail", client.GetNodeNames())
	}
	if !initErr.Matches(types.INVALID_NODE_ERROR) {
		t.Fatalf("client with UseServicesAlternate failed with %v, want INVALID_NODE_ERROR", initErr)
	}
	for _, part := range []string{"UseServicesAlternate", seed.name} {
		if !strings.Contains(initErr.Error(), part) {
			t.Fatalf("init error %q does not name %s", initErr, part)
		}
	}
}

// The peer with no mapping gets no node, so its partitions have no master,
// and the miss is logged again on every tend.
func TestL2StrictMissCreatesNoNode(t *testing.T) {
	host, nodes := l2Topology(t)
	victim := nodes[len(nodes)-1]
	mapped := nodes[:len(nodes)-1]
	logs := captureL2Logs(t)

	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, mapped))
	policy.TranslationOnMiss = as.TranslationMissStrict
	client := newL2Client(t, policy, as.NewHost(host, nodes[0].port))

	want := l2NodeNames(mapped)
	waitL2Nodes(t, client, want)
	waitL2Misses(t, client, logs, want, victim)
	assertL2MissMessage(t, logs.misses(victim.name)[0], victim, "port-map")
	assertL2MappedKeysWork(t, client, want)
}

func TestL2StrictEmptyIpMapDropsPeers(t *testing.T) {
	policy := l2Policy()
	policy.IpMap = map[string]string{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2StrictMissDropsPeers(t, policy, "static-map")
}

func TestL2StrictEmptyCustomTranslatorDropsPeers(t *testing.T) {
	policy := l2Policy()
	policy.AddressTranslator = l2EmptyTranslator{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2StrictMissDropsPeers(t, policy, fmt.Sprintf("%T", l2EmptyTranslator{}))
}

// A node started mid-test is an active Node within two tend intervals of the
// peers-generation bump that announces it, and then masters partitions. The
// test gives it a listener and a port-map entry before it joins.
func TestL2NewNodeActiveWithinTwoTendIntervals(t *testing.T) {
	host, nodes := l2Topology(t)
	controlDir := os.Getenv("DISCOVERY_CONTROL_DIR")
	if controlDir == "" {
		t.Skip("N-11 needs DISCOVERY_CONTROL_DIR, the topology has no node control")
	}
	first := nodes[0]
	names := l2NodeNames(nodes)
	translator := &l2SwappableTranslator{}
	translator.set(t, l2PortMapByEndpoint(host, nodes))
	policy := l2Policy()
	policy.AddressTranslator = translator
	client := newL2Client(t, policy, as.NewHost(host, first.port))
	waitL2Cluster(t, client, names)

	generation := l2PeersGeneration(t, host, first)
	added := startL2Node(t, controlDir, host, nodes)
	upstreamPort := l2UpstreamPort(t, first.proxy)
	port := addL2Listener(t, added.container, net.JoinHostPort(added.container, strconv.Itoa(upstreamPort)))
	advertised := net.JoinHostPort(added.address, strconv.Itoa(upstreamPort))
	portMap := l2PortMapByEndpoint(host, nodes)
	portMap[advertised] = net.JoinHostPort(host, strconv.Itoa(port))
	translator.set(t, portMap)

	if mapped := l2PeersGeneration(t, host, first); mapped != generation {
		t.Fatalf("peers-generation bumped from %d to %d before %s had a listener and a mapping", generation, mapped, added.container)
	}
	var bump, active time.Time
	var node *as.Node
	if !waitL2(l2ReseedPoll, func() bool {
		now := time.Now()
		if bump.IsZero() && l2PeersGeneration(t, host, first) != generation {
			bump = now
		}
		if active.IsZero() {
			for _, candidate := range client.GetNodes() {
				if candidate.IsActive() && !slices.Contains(names, candidate.GetName()) {
					active, node = now, candidate
				}
			}
		}
		return !bump.IsZero() && !active.IsZero()
	}) {
		t.Fatalf("no peers-generation bump or no active node for %s, nodes %v", added.container, client.GetNodeNames())
	}

	info, err := l2RequestInfo(host, port, "node", "service-clear-std")
	if err != nil {
		t.Fatal(err)
	}
	if info["node"] != node.GetName() {
		t.Fatalf("listener %d serves node %s, client added %s", port, info["node"], node.GetName())
	}
	if !strings.Contains(info["service-clear-std"], advertised) {
		t.Fatalf("%s advertises %q, want %s", node.GetName(), info["service-clear-std"], advertised)
	}
	if node.GetHost().Port != port {
		t.Fatalf("node %s host %v, want listener port %d", node.GetName(), node.GetHost(), port)
	}
	if elapsed, budget := active.Sub(bump), 2*policy.TendInterval; elapsed > budget {
		t.Fatalf("node %s active %s after the peers-generation bump, budget %s", node.GetName(), elapsed, budget)
	}
	if !waitL2(l2Poll, func() bool {
		owners, _ := l2PartitionOwners(client)
		return slices.Contains(owners, node.GetName())
	}) {
		owners, _ := l2PartitionOwners(client)
		t.Fatalf("node %s masters no partition, owners %v", node.GetName(), owners)
	}
}

// In tend mode the provider runs only on its refresh interval, so the client
// re-seeds from the seeds it holds with no provider call after startup.
func TestL2ReseedOnZeroActiveNodes(t *testing.T) {
	provider := assertL2Reseeds(t, as.DiscoveryExecutionTend, nil)
	if calls := provider.callCount(); calls != 1 {
		t.Fatalf("%d seed provider calls, want only the startup call", calls)
	}
}

// In thread mode a client with no nodes also wakes the refresh goroutine. The
// listeners stay down until that refresh has run, since the first tend that
// finds no nodes requests it. The re-seed still uses the seeds in hand.
func TestL2ReseedOnZeroActiveNodesInThreadMode(t *testing.T) {
	assertL2Reseeds(t, as.DiscoveryExecutionThread, func(provider *l2CountingSeedProvider) {
		if !waitL2(l2Poll, func() bool { return provider.callCount() > 1 }) {
			t.Fatal("no seed provider refresh while the client had no nodes, want the urgent refresh")
		}
	})
}

// assertL2Reseeds disables every listener until the client has no nodes, runs
// whileListenersDown if set, enables them again and checks the client
// re-seeds within two tends plus the connect timeout and recovers the full
// partition map. It returns the seed provider the client used.
func assertL2Reseeds(t *testing.T, mode as.DiscoveryExecutionMode, whileListenersDown func(*l2CountingSeedProvider)) *l2CountingSeedProvider {
	t.Helper()
	host, nodes := l2Topology(t)
	provider := &l2CountingSeedProvider{seeds: []*as.Host{as.NewHost(host, nodes[0].port)}}
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	policy.SeedProvider = provider
	policy.DiscoveryExecutionMode = mode
	policy.DiscoveryRefreshInterval = l2ReseedRefreshInterval
	client := newL2Client(t, policy)

	want := l2NodeNames(nodes)
	waitL2Cluster(t, client, want)

	for _, node := range nodes {
		disableL2Listener(t, node.proxy)
	}
	if !waitL2(l2Poll, func() bool { return len(client.GetNodes()) == 0 }) {
		t.Fatalf("client kept nodes %v with every listener disabled", client.GetNodeNames())
	}
	if whileListenersDown != nil {
		whileListenersDown(provider)
	}

	reenabled := time.Now()
	for _, node := range nodes {
		enableL2Listener(t, node.proxy)
	}
	if !waitL2(l2ReseedPoll, func() bool { return len(client.GetNodes()) > 0 }) {
		t.Fatal("client did not re-seed")
	}
	if elapsed, budget := time.Since(reenabled), 2*policy.TendInterval+policy.Timeout; elapsed > budget {
		t.Fatalf("re-seeded %s after the listeners came back, budget %s", elapsed, budget)
	}
	waitL2Cluster(t, client, want)
	return provider
}

// Writes with no retry show which keys the client still serves while one
// node's listener is down.
func TestL2NodeRemovedFromEndpointDegradesAndReturns(t *testing.T) {
	host, nodes := l2Topology(t)
	victim := nodes[len(nodes)-1]
	client := newL2Client(t, l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes)), as.NewHost(host, nodes[0].port))

	want := l2NodeNames(nodes)
	waitL2Cluster(t, client, want)

	masters := as.PartitionMasters(client, *namespace)
	var victimKeys, otherKeys []*as.Key
	for i := range l2KeyCount {
		key := l2Key(t, "n7-", i)
		master := masters[key.PartitionId()]
		if master == nil {
			t.Fatalf("partition %d of key %v has no master", key.PartitionId(), key)
		}
		if master.GetName() == victim.name {
			victimKeys = append(victimKeys, key)
		} else {
			otherKeys = append(otherKeys, key)
		}
	}
	if len(victimKeys) == 0 || len(otherKeys) == 0 {
		t.Fatalf("%d keys on %s and %d on the other nodes, want both", len(victimKeys), victim.name, len(otherKeys))
	}

	policy := as.NewWritePolicy(0, 0)
	policy.MaxRetries = 0
	policy.TotalTimeout = 2 * time.Second

	disableL2Listener(t, victim.proxy)
	if !waitL2(l2Poll, func() bool { return !l2Writes(client, policy, victimKeys[0]) }) {
		t.Fatalf("writes to %s still succeed with its listener disabled", victim.name)
	}
	for _, key := range victimKeys {
		if l2Writes(client, policy, key) {
			t.Fatalf("write of %v to %s succeeded with its listener disabled", key, victim.name)
		}
	}
	for _, key := range otherKeys {
		assertL2KeyWrites(t, client, policy, key, 1)
	}
	others := slices.DeleteFunc(slices.Clone(want), func(name string) bool { return name == victim.name })
	if active := l2ActiveNodeNames(client, victim.name); !slices.Equal(active, others) {
		t.Fatalf("active nodes %v besides %s, want %v", active, victim.name, others)
	}

	enableL2Listener(t, victim.proxy)
	if !waitL2(l2Poll, func() bool {
		return !slices.ContainsFunc(victimKeys, func(key *as.Key) bool { return !l2Writes(client, policy, key) })
	}) {
		t.Fatalf("writes to %s did not recover", victim.name)
	}
	waitL2Cluster(t, client, want)
	for _, key := range append(victimKeys, otherKeys...) {
		assertL2KeyWrites(t, client, policy, key, 2)
	}
}

// A node that stops replying without closing its sockets costs each command
// at most its timeouts, and no command thread stays blocked.
func TestL2SilentDropTimesOutWithinBound(t *testing.T) {
	runL2SilentDrop(t, func(*as.ClientPolicy) {}, l2DropMaxRetries, func(load *l2Load, write *as.WritePolicy, held time.Time, bound time.Duration) {
		if !waitL2(l2Poll, func() bool { return len(load.failures(types.TIMEOUT)) >= l2DropThreads }) {
			t.Fatalf("%d timeouts after the node stopped replying, want %d, %s", len(load.failures(types.TIMEOUT)), l2DropThreads, load)
		}
		if !load.stopWithin(bound) {
			t.Fatalf("command threads still running %s after stop", bound)
		}
		failures := load.failures()
		if other := slices.DeleteFunc(slices.Clone(failures), func(failure l2Failure) bool { return failure.err.Matches(types.TIMEOUT) }); len(other) > 0 {
			t.Fatalf("command failed with %s, want TIMEOUT in every error chain: %v", other[0].codes(), other[0].err)
		}
		first := slices.MinFunc(failures, func(a, b l2Failure) int { return a.end().Compare(b.end()) })
		if wait := first.end().Sub(held); wait > bound {
			t.Fatalf("first timeout %s after the node stopped replying, bound %s", wait, bound)
		}
		slowest := slices.MaxFunc(failures, func(a, b l2Failure) int { return cmp.Compare(a.elapsed, b.elapsed) })
		if slowest.elapsed > bound {
			t.Fatalf("command took %s to time out, bound %s", slowest.elapsed, bound)
		}
	})
}

// With a low MaxErrorRate the timeouts trip the node's error-rate breaker, and
// commands it rejects fail without waiting for the socket.
// Go's breaker rejects only the attempt that finds the rate exceeded and
// retries it, so the writes run with no retry for the rejection to be the
// command's result. Java also expects later commands to fail fast, which Go's
// breaker does not do.
func TestL2SilentDropTripsErrorRateBreaker(t *testing.T) {
	lowerErrorRate := func(policy *as.ClientPolicy) {
		policy.MaxErrorRate = l2BreakerMaxErrorRate
		policy.ErrorRateWindow = l2BreakerWindow
	}
	runL2SilentDrop(t, lowerErrorRate, 0, func(load *l2Load, write *as.WritePolicy, held time.Time, bound time.Duration) {
		if !waitL2(l2Poll, func() bool { return len(load.failures(types.MAX_ERROR_RATE)) > 0 }) {
			t.Fatalf("no MAX_ERROR_RATE after the node stopped replying, want the breaker to trip, %s", load)
		}
		if !load.stopWithin(bound) {
			t.Fatalf("command threads still running %s after stop", bound)
		}
		rejected := load.failures(types.MAX_ERROR_RATE)
		trip := slices.MinFunc(rejected, func(a, b l2Failure) int { return a.end().Compare(b.end()) }).end()
		if wait, tripBound := trip.Sub(held), time.Duration(l2BreakerWindow+1)*l2TendInterval+bound; wait > tripBound {
			t.Fatalf("breaker tripped %s after the node stopped replying, bound %s", wait, tripBound)
		}
		slowest := slices.MaxFunc(rejected, func(a, b l2Failure) int { return cmp.Compare(a.elapsed, b.elapsed) })
		if slowest.elapsed >= write.SocketTimeout {
			t.Fatalf("rejected command took %s, want under the %s socket timeout", slowest.elapsed, write.SocketTimeout)
		}
	})
}

// runL2SilentDrop writes keys the last node masters on several threads, holds
// that node's traffic with no reply and no close, and runs check with the
// time the hold began and the bound a command may take to fail. It then
// releases the node and waits for the full cluster to serve again.
func runL2SilentDrop(t *testing.T, configure func(*as.ClientPolicy), maxRetries int, check func(load *l2Load, write *as.WritePolicy, held time.Time, bound time.Duration)) {
	t.Helper()
	host, nodes := l2Topology(t)
	victim := nodes[len(nodes)-1]
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	configure(policy)
	client := newL2Client(t, policy, as.NewHost(host, nodes[0].port))

	want := l2NodeNames(nodes)
	waitL2Cluster(t, client, want)

	masters := as.PartitionMasters(client, *namespace)
	var keys []*as.Key
	for i := 0; len(keys) < l2DropKeyCount; i++ {
		key := l2Key(t, "f4-", i)
		if master := masters[key.PartitionId()]; master != nil && master.GetName() == victim.name {
			keys = append(keys, key)
		}
	}

	write := as.NewWritePolicy(0, 0)
	write.SocketTimeout = l2DropSocketTimeout
	write.TotalTimeout = l2DropTotalTimeout
	write.MaxRetries = maxRetries
	bound := write.TotalTimeout + write.SocketTimeout

	load := startL2Load(l2DropThreads, func(thread, i int) as.Error {
		return client.Put(write, keys[(thread+i*l2DropThreads)%len(keys)], as.BinMap{"v": i})
	})
	defer load.stopWithin(bound)
	if !waitL2(l2Poll, func() bool { return load.successes.Load() >= l2DropThreads }) {
		t.Fatalf("writes to %s did not succeed before the hold: %v", victim.name, load.failures())
	}

	held := time.Now()
	release := holdL2Listener(t, victim.proxy)
	check(load, write, held, bound)

	release()
	if !waitL2(l2Poll, func() bool { return l2Writes(client, write, keys[0]) }) {
		t.Fatalf("writes to %s did not recover after the hold", victim.name)
	}
	waitL2Cluster(t, client, want)
}

// The handshake checks the certificate against the node's tls-name, not the
// endpoint hostname the client dials, and a seed named by that hostname fails.
func TestL2TLSValidatesAgainstNodeTLSName(t *testing.T) {
	host, nodes := l2Topology(t)
	if subset := os.Getenv("DISCOVERY_SUBSET"); subset != "ee" {
		t.Skip("N-6 needs DISCOVERY_SUBSET=ee, subset is " + subset)
	}
	first := nodes[0]
	if first.tlsPort == 0 {
		t.Skip("N-6 needs an ee topology with TLS listeners, DISCOVERY_TLS_ENDPOINT_PORTS is empty")
	}
	if first.tlsName == host {
		t.Fatalf("tls-name %s is the endpoint hostname", first.tlsName)
	}

	policy := l2PortMapPolicy(t, l2TLSPortMapByEndpoint(host, nodes))
	policy.TlsConfig = l2TLSConfig(t)
	client := newL2Client(t, policy, &as.Host{Name: host, TLSName: first.tlsName, Port: first.tlsPort})

	waitL2Cluster(t, client, l2NodeNames(nodes))
	endpointAddresses, err := net.LookupHost(host)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]l2Node, len(nodes))
	for _, node := range nodes {
		byName[node.name] = node
	}
	for _, node := range client.GetNodes() {
		got, expected := node.GetHost(), byName[node.GetName()]
		if got.Name != host && !slices.Contains(endpointAddresses, got.Name) {
			t.Fatalf("node %s host %s is not %s %v", node.GetName(), got.Name, host, endpointAddresses)
		}
		if got.Port != expected.tlsPort || got.TLSName != expected.tlsName {
			t.Fatalf("node %s host %s, want port %d and tls-name %s", node.GetName(), got, expected.tlsPort, expected.tlsName)
		}
	}
	for i := range l2TLSKeyCount {
		assertL2KeyWrites(t, client, nil, l2Key(t, "n6-", i), i)
	}

	wrongName, err := as.NewClientWithPolicyAndHost(policy, &as.Host{Name: host, TLSName: host, Port: first.tlsPort})
	if err == nil {
		wrongName.Close()
		t.Fatalf("TLS seed validated against the endpoint hostname %s", host)
	}
	var mismatch x509.HostnameError
	if !errors.As(err, &mismatch) || mismatch.Host != host {
		t.Fatalf("TLS seed with tls-name %s failed with %v, want a certificate name mismatch", host, err)
	}
}

func skipUnlessL2(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME")) == "" {
		t.Skip("set DISCOVERY_ENDPOINT_HOSTNAME to run L2 topology tests")
	}
	if subset := os.Getenv("DISCOVERY_SUBSET"); subset != "" && subset != "ce" && subset != "ee" {
		t.Skip("L2 default-profile smoke, DISCOVERY_SUBSET is " + subset)
	}
	// cn-only is the N-17 profile, not the default smoke.
	if shape := os.Getenv("DISCOVERY_TLS_CERT_SHAPE"); shape != "" && shape != "san-only" {
		t.Skip("L2 default-profile smoke, DISCOVERY_TLS_CERT_SHAPE is " + shape)
	}
	for _, name := range []string{
		"DISCOVERY_REACHABLE_NODE",
		"DISCOVERY_PASS_THROUGH_BRANCH",
		"DISCOVERY_PORT_MAP_RANGE",
		"DISCOVERY_CLIENT_NETWORK_MTU",
		"DISCOVERY_PROXY_PORT_RANGE",
		"DISCOVERY_IDLE_RESET",
	} {
		if os.Getenv(name) != "" {
			t.Skip("L2 default-profile smoke, " + name + " is set")
		}
	}
}

func l2Topology(t *testing.T) (string, []l2Node) {
	t.Helper()
	skipUnlessL2(t)
	l2Once.Do(func() {
		l2Host = strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
		l2Nodes, l2Err = discoverL2Nodes(l2Host)
	})
	if l2Err != nil {
		t.Fatal(l2Err)
	}
	return l2Host, l2Nodes
}

func discoverL2Nodes(host string) ([]l2Node, error) {
	ports, err := l2Ports("DISCOVERY_ENDPOINT_PORTS")
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, errors.New("DISCOVERY_ENDPOINT_PORTS is not set")
	}
	tlsPorts, err := l2Ports("DISCOVERY_TLS_ENDPOINT_PORTS")
	if err != nil {
		return nil, err
	}
	rawContainers := os.Getenv("AEROSPIKE_CONTAINER_NAMES")
	if rawContainers == "" {
		return nil, errors.New("AEROSPIKE_CONTAINER_NAMES is not set")
	}
	if containers := strings.Split(rawContainers, ","); len(containers) != len(ports) {
		return nil, fmt.Errorf("DISCOVERY_ENDPOINT_PORTS %v vs AEROSPIKE_CONTAINER_NAMES %v", ports, containers)
	}
	proxyPorts, err := l2ProxyPorts()
	if err != nil {
		return nil, err
	}
	proxies := make(map[int]string, len(proxyPorts))
	for proxy, port := range proxyPorts {
		proxies[port] = proxy
	}
	withTLS := len(tlsPorts) > 0
	commands := []string{"node", "service-clear-std"}
	if withTLS {
		commands = append(commands, "service-tls-std", "peers-tls-std")
	}
	nodes := make([]l2Node, 0, len(ports))
	tlsNames := make(map[string]string)
	for _, port := range ports {
		proxy, found := proxies[port]
		if !found {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP has no proxy for listener %d", port)
		}
		info, err := l2RequestInfo(host, port, commands...)
		if err != nil {
			return nil, fmt.Errorf("info %s:%d: %w", host, port, err)
		}
		name := strings.TrimSpace(info["node"])
		if name == "" {
			return nil, fmt.Errorf("no node name from listener %d", port)
		}
		advertised, err := l2Advertised(info["service-clear-std"])
		if err != nil {
			return nil, fmt.Errorf("node %s service-clear-std: %w", name, err)
		}
		if len(advertised) == 0 {
			return nil, fmt.Errorf("node %s advertises no clear address", name)
		}
		for _, endpoint := range advertised {
			if l2Reachable(endpoint) {
				return nil, fmt.Errorf("node %s advertised address %s is reachable from the client", name, endpoint)
			}
		}
		node := l2Node{proxy: proxy, port: port, name: name, advertised: advertised}
		if withTLS {
			if node.tlsPort, found = proxyPorts[proxy+l2TLSProxySuffix]; !found {
				return nil, fmt.Errorf("DISCOVERY_PROXY_MAP has no TLS proxy %s%s", proxy, l2TLSProxySuffix)
			}
			if !slices.Contains(tlsPorts, node.tlsPort) {
				return nil, fmt.Errorf("TLS proxy %s%s listens on %d, not in DISCOVERY_TLS_ENDPOINT_PORTS %v", proxy, l2TLSProxySuffix, node.tlsPort, tlsPorts)
			}
			if node.tlsAdvertised, err = l2Advertised(info["service-tls-std"]); err != nil {
				return nil, fmt.Errorf("node %s service-tls-std: %w", name, err)
			}
			if len(node.tlsAdvertised) == 0 {
				return nil, fmt.Errorf("node %s advertises no TLS address", name)
			}
			maps.Copy(tlsNames, l2PeerTLSNames(info["peers-tls-std"]))
		}
		nodes = append(nodes, node)
	}
	if withTLS {
		for i := range nodes {
			if nodes[i].tlsName = tlsNames[nodes[i].name]; nodes[i].tlsName == "" {
				return nil, fmt.Errorf("no peer lists a tls-name for node %s", nodes[i].name)
			}
		}
	}
	return nodes, nil
}

// l2PeerTLSNames reads the tls-name of each peer in a peers-tls-std reply,
// gen,port,[[node,tls-name,[address,...]],...]. A node does not list itself.
func l2PeerTLSNames(reply string) map[string]string {
	names := make(map[string]string)
	for _, match := range l2PeerPattern.FindAllStringSubmatch(reply, -1) {
		if match[2] != "" {
			names[match[1]] = match[2]
		}
	}
	return names
}

// l2ProxyPorts parses DISCOVERY_PROXY_MAP, a list of Toxiproxy proxy
// name:listener port pairs, into listener ports by proxy name.
func l2ProxyPorts() (map[string]int, error) {
	proxies := make(map[string]int)
	for _, entry := range strings.Split(os.Getenv("DISCOVERY_PROXY_MAP"), ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		index := strings.LastIndexByte(entry, ':')
		if index <= 0 {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP entry %q is not name:port", entry)
		}
		port, err := strconv.Atoi(entry[index+1:])
		if err != nil {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP entry %q: %w", entry, err)
		}
		proxies[entry[:index]] = port
	}
	if len(proxies) == 0 {
		return nil, errors.New("DISCOVERY_PROXY_MAP is not set")
	}
	return proxies, nil
}

// l2Ports parses a comma-separated port list from the named variable. Unset is empty.
func l2Ports(name string) ([]int, error) {
	var ports []int
	for _, part := range strings.Split(os.Getenv(name), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		port, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ports = append(ports, port)
	}
	return ports, nil
}

func l2RequestInfo(host string, port int, commands ...string) (map[string]string, error) {
	policy := as.NewClientPolicy()
	policy.Timeout = time.Second
	conn, err := as.NewConnection(policy, as.NewHost(host, port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.RequestInfo(commands...)
}

func l2Advertised(reply string) ([]string, error) {
	var advertised []string
	for _, part := range strings.Split(reply, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hosts, err := as.NewHosts(part)
		if err != nil {
			return nil, err
		}
		for _, host := range hosts {
			advertised = append(advertised, host.String())
		}
	}
	return advertised, nil
}

func l2PortMapByEndpoint(host string, nodes []l2Node) map[string]string {
	return l2ListenerPortMap(host, nodes, func(node l2Node) (int, []string) { return node.port, node.advertised })
}

func l2TLSPortMapByEndpoint(host string, nodes []l2Node) map[string]string {
	return l2ListenerPortMap(host, nodes, func(node l2Node) (int, []string) { return node.tlsPort, node.tlsAdvertised })
}

// l2ListenerPortMap maps each advertised address of a node to its listener on host.
func l2ListenerPortMap(host string, nodes []l2Node, listenerOf func(l2Node) (int, []string)) map[string]string {
	portMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		port, advertised := listenerOf(node)
		listener := net.JoinHostPort(host, strconv.Itoa(port))
		for _, address := range advertised {
			portMap[address] = listener
		}
	}
	return portMap
}

func l2PortMapByNodeName(host string, nodes []l2Node) map[string]string {
	portMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		portMap[node.name] = net.JoinHostPort(host, strconv.Itoa(node.port))
	}
	return portMap
}

// l2TLSConfig trusts only the server CA in AEROSPIKE_CA_CERT.
func l2TLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	path := os.Getenv("AEROSPIKE_CA_CERT")
	if path == "" {
		t.Fatal("AEROSPIKE_CA_CERT is not set")
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatalf("no certificate in %s", path)
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

func l2NodeNames(nodes []l2Node) []string {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.name)
	}
	slices.Sort(names)
	return names
}

func l2Policy() *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.TendInterval = l2TendInterval
	policy.Timeout = time.Second
	return policy
}

func l2PortMapPolicy(t *testing.T, portMap map[string]string) *as.ClientPolicy {
	t.Helper()
	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	policy := l2Policy()
	policy.AddressTranslator = translator
	return policy
}

// newL2Client closes the client when the test ends.
func newL2Client(t *testing.T, policy *as.ClientPolicy, seeds ...*as.Host) *as.Client {
	t.Helper()
	client, err := as.NewClientWithPolicyAndHost(policy, seeds...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// captureL2Logs raises the client log level to warnings for the test.
// Call it before creating the client, so the client is closed before the
// logger is restored.
func captureL2Logs(t *testing.T) *l2LogCapture {
	t.Helper()
	capture := &l2LogCapture{forward: asl.Logger.Logger}
	asl.Logger.SetLogger(capture)
	asl.Logger.SetLevel(asl.WARNING)
	t.Cleanup(func() {
		asl.Logger.SetLogger(capture.forward)
		if *debug {
			asl.Logger.SetLevel(asl.DEBUG)
		} else {
			asl.Logger.SetLevel(asl.OFF)
		}
	})
	return capture
}

// disableL2Listener enables the listener again when the test ends.
func disableL2Listener(t *testing.T, proxy string) {
	t.Helper()
	if err := setL2Listener(proxy, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := setL2Listener(proxy, true); err != nil {
			t.Error(err)
		}
	})
}

func enableL2Listener(t *testing.T, proxy string) {
	t.Helper()
	if err := setL2Listener(proxy, true); err != nil {
		t.Fatal(err)
	}
}

func setL2Listener(proxy string, enabled bool) error {
	return l2Toxiproxy(http.MethodPost, "/proxies/"+proxy, fmt.Sprintf(`{"enabled":%t}`, enabled), http.StatusOK)
}

// holdL2Listener stops the node's replies without closing its connections, and
// returns the release, which also runs when the test ends.
func holdL2Listener(t *testing.T, proxy string) (release func()) {
	t.Helper()
	toxic := fmt.Sprintf(`{"name":%q,"type":"timeout","stream":"upstream","toxicity":1.0,"attributes":{"timeout":0}}`, l2HoldToxic)
	if err := l2Toxiproxy(http.MethodPost, "/proxies/"+proxy+"/toxics", toxic, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() {
		once.Do(func() {
			if err := l2Toxiproxy(http.MethodDelete, "/proxies/"+proxy+"/toxics/"+l2HoldToxic, "", http.StatusNoContent); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

// addL2Listener creates a Toxiproxy listener named proxy above every listener
// in use, removes it when the test ends and returns its port.
func addL2Listener(t *testing.T, proxy, upstream string) int {
	t.Helper()
	port := 0
	for _, listener := range l2ToxiproxyProxies(t) {
		if _, listenPort, err := net.SplitHostPort(listener.Listen); err == nil {
			if used, err := strconv.Atoi(listenPort); err == nil {
				port = max(port, used)
			}
		}
	}
	port++
	listener := fmt.Sprintf(`{"name":%q,"listen":"0.0.0.0:%d","upstream":%q,"enabled":true}`, proxy, port, upstream)
	if err := l2Toxiproxy(http.MethodPost, "/proxies", listener, http.StatusCreated); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l2Toxiproxy(http.MethodDelete, "/proxies/"+proxy, "", http.StatusNoContent); err != nil {
			t.Error(err)
		}
	})
	return port
}

// l2UpstreamPort returns the node port a Toxiproxy listener forwards to.
func l2UpstreamPort(t *testing.T, proxy string) int {
	t.Helper()
	listener, ok := l2ToxiproxyProxies(t)[proxy]
	if !ok {
		t.Fatalf("toxiproxy has no listener %s", proxy)
	}
	_, upstreamPort, err := net.SplitHostPort(listener.Upstream)
	if err != nil {
		t.Fatalf("listener %s upstream %q: %v", proxy, listener.Upstream, err)
	}
	port, err := strconv.Atoi(upstreamPort)
	if err != nil {
		t.Fatalf("listener %s upstream %q: %v", proxy, listener.Upstream, err)
	}
	return port
}

type l2ToxiproxyListener struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
}

func l2ToxiproxyProxies(t *testing.T) map[string]l2ToxiproxyListener {
	t.Helper()
	reply, err := l2ToxiproxyReply(http.MethodGet, "/proxies", "", http.StatusOK)
	if err != nil {
		t.Fatal(err)
	}
	var proxies map[string]l2ToxiproxyListener
	if err := json.Unmarshal(reply, &proxies); err != nil {
		t.Fatalf("toxiproxy proxies %s: %v", reply, err)
	}
	return proxies
}

func l2Toxiproxy(method, path, body string, want int) error {
	_, err := l2ToxiproxyReply(method, path, body, want)
	return err
}

func l2ToxiproxyReply(method, path, body string, want int) ([]byte, error) {
	toxiproxy := strings.TrimRight(os.Getenv("TOXIPROXY_URL"), "/")
	if toxiproxy == "" {
		return nil, errors.New("TOXIPROXY_URL is not set")
	}
	request, err := http.NewRequest(method, toxiproxy+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	httpClient := http.Client{Timeout: 10 * time.Second}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	reply, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	if response.StatusCode != want {
		return nil, fmt.Errorf("toxiproxy %s %s: status %d %s", method, path, response.StatusCode, reply)
	}
	return reply, nil
}

// l2SwappableTranslator is a port-map translator the test can replace while the client runs.
type l2SwappableTranslator struct {
	current atomic.Pointer[as.AddressTranslator]
}

func (translator *l2SwappableTranslator) Translate(advertised as.Endpoint) (as.Host, bool) {
	return (*translator.current.Load()).Translate(advertised)
}

func (translator *l2SwappableTranslator) set(t *testing.T, portMap map[string]string) {
	t.Helper()
	portMapTranslator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	translator.current.Store(&portMapTranslator)
}

type l2StartedNode struct {
	container string
	address   string
}

// startL2Node asks the host to start a node and stops it when the test ends,
// waiting until the cluster is back to nodes.
func startL2Node(t *testing.T, controlDir, host string, nodes []l2Node) l2StartedNode {
	t.Helper()
	ack := l2Control(t, controlDir, "start")
	started := l2StartedNode{container: ack["container"], address: ack["address"]}
	if started.container == "" || started.address == "" {
		t.Fatalf("start acknowledgement %v has no container or address", ack)
	}
	t.Cleanup(func() {
		l2Control(t, controlDir, "stop "+started.container)
		waitL2ClusterStable(t, host, nodes)
	})
	return started
}

// l2Control writes command to controlDir as <id>.request and returns the host's
// <id>.ack, one key=value per line.
func l2Control(t *testing.T, controlDir, command string) map[string]string {
	t.Helper()
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	pending := filepath.Join(controlDir, id+".tmp")
	if err := os.WriteFile(pending, []byte(command+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(pending, filepath.Join(controlDir, id+".request")); err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(controlDir, id+".ack")
	var reply []byte
	if !waitL2(l2Poll, func() bool {
		var err error
		reply, err = os.ReadFile(ackPath)
		return err == nil
	}) {
		t.Fatalf("no acknowledgement for %q in %s", command, controlDir)
	}
	if err := os.Remove(ackPath); err != nil {
		t.Error(err)
	}
	ack := make(map[string]string)
	for _, line := range strings.Split(string(reply), "\n") {
		if key, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok && key != "" {
			ack[key] = value
		}
	}
	if failure, ok := ack["error"]; ok {
		t.Fatalf("%q failed: %s", command, failure)
	}
	return ack
}

// waitL2ClusterStable waits until every node reports the same stable cluster of len(nodes).
func waitL2ClusterStable(t *testing.T, host string, nodes []l2Node) {
	t.Helper()
	command := fmt.Sprintf("cluster-stable:size=%d;ignore-migrations=false", len(nodes))
	var keys []string
	if !waitL2(l2Poll, func() bool {
		keys = keys[:0]
		for _, node := range nodes {
			info, err := l2RequestInfo(host, node.port, command)
			if err != nil {
				keys = append(keys, err.Error())
				continue
			}
			keys = append(keys, info[command])
		}
		return len(slices.Compact(slices.Clone(keys))) == 1 && !strings.HasPrefix(keys[0], "ERROR")
	}) {
		t.Fatalf("cluster not stable at size %d: %v", len(nodes), keys)
	}
}

func l2PeersGeneration(t *testing.T, host string, node l2Node) int64 {
	t.Helper()
	info, err := l2RequestInfo(host, node.port, "peers-generation")
	if err != nil {
		t.Fatal(err)
	}
	generation, err := strconv.ParseInt(info["peers-generation"], 10, 64)
	if err != nil {
		t.Fatalf("node %s peers-generation %q: %v", node.name, info["peers-generation"], err)
	}
	return generation
}

// l2NamespaceStats returns the client command counters of the test namespace on node.
func l2NamespaceStats(t *testing.T, host string, node l2Node) map[string]int64 {
	t.Helper()
	command := "namespace/" + *namespace
	info, err := l2RequestInfo(host, node.port, command)
	if err != nil {
		t.Fatal(err)
	}
	wanted := []string{"client_write_success", "client_read_success", "client_proxy_complete", "client_proxy_error", "client_proxy_timeout"}
	stats := make(map[string]int64, len(wanted))
	for _, stat := range strings.Split(info[command], ";") {
		if name, value, ok := strings.Cut(stat, "="); ok && slices.Contains(wanted, name) {
			count, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatalf("node %s %s=%q: %v", node.name, name, value, err)
			}
			stats[name] = count
		}
	}
	for _, name := range wanted {
		if _, ok := stats[name]; !ok {
			t.Fatalf("node %s has no namespace statistic %s", node.name, name)
		}
	}
	return stats
}

// assertL2RoutedTo checks the client sends writes and reads of key to owner.
func assertL2RoutedTo(t *testing.T, cluster *as.Cluster, write, read *as.BasePolicy, key *as.Key, owner *as.Node) {
	t.Helper()
	writePartition, err := as.PartitionForWrite(cluster, write, key)
	if err != nil {
		t.Fatal(err)
	}
	writeNode, err := writePartition.GetNodeWrite(cluster)
	if err != nil {
		t.Fatal(err)
	}
	readPartition, err := as.PartitionForRead(cluster, read, key)
	if err != nil {
		t.Fatal(err)
	}
	readNode, err := readPartition.GetNodeRead(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if writeNode != owner || readNode != owner {
		t.Fatalf("key %v writes to %v and reads from %v, want its master %v", key, writeNode, readNode, owner)
	}
}

func assertL2PortMapJoins(t *testing.T, portMapOf func(host string, nodes []l2Node) map[string]string) {
	t.Helper()
	host, nodes := l2Topology(t)
	client := newL2Client(t, l2PortMapPolicy(t, portMapOf(host, nodes)), as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))
}

func waitL2Nodes(t *testing.T, client *as.Client, want []string) {
	t.Helper()
	var got []string
	if !waitL2(l2Poll, func() bool {
		got = slices.Sorted(slices.Values(client.GetNodeNames()))
		return slices.Equal(got, want)
	}) {
		t.Fatalf("nodes %v, want %v", got, want)
	}
}

// waitL2Cluster waits until the client holds the want nodes and they master
// every partition of the test namespace.
func waitL2Cluster(t *testing.T, client *as.Client, want []string) {
	t.Helper()
	waitL2Nodes(t, client, want)
	var owners []string
	var unowned int
	if !waitL2(l2Poll, func() bool {
		owners, unowned = l2PartitionOwners(client)
		return unowned == 0
	}) {
		t.Fatalf("%d partitions of namespace %s have no active master, owners %v", unowned, *namespace, owners)
	}
	if !slices.Equal(owners, want) {
		t.Fatalf("partition owners %v, want %v", owners, want)
	}
}

func l2PartitionOwners(client *as.Client) ([]string, int) {
	owners := make(map[string]struct{})
	unowned := 0
	for _, node := range as.PartitionMasters(client, *namespace) {
		if node == nil || !node.IsActive() {
			unowned++
			continue
		}
		owners[node.GetName()] = struct{}{}
	}
	return slices.Sorted(maps.Keys(owners)), unowned
}

// waitL2 polls condition until it holds or l2Wait passes.
func waitL2(poll time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(l2Wait)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(poll)
	}
	return true
}

// waitL2Misses waits until each peer's miss has been logged on several tends,
// with the client holding exactly the want nodes throughout.
func waitL2Misses(t *testing.T, client *as.Client, logs *l2LogCapture, want []string, peers ...l2Node) {
	t.Helper()
	underRetried := func(peer l2Node) bool { return len(logs.misses(peer.name)) < l2MissRetries }
	if waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, want) {
			t.Fatalf("nodes %v, want %v", got, want)
		}
		return !slices.ContainsFunc(peers, underRetried)
	}) {
		return
	}
	for _, peer := range peers {
		if underRetried(peer) {
			t.Fatalf("missed peer %s logged %d times, want at least %d", peer.name, len(logs.misses(peer.name)), l2MissRetries)
		}
	}
}

func assertL2MissMessage(t *testing.T, message string, peer l2Node, translator string) {
	t.Helper()
	for _, part := range []string{peer.name, peer.advertised[0], "standard", translator, "pass_through"} {
		if !strings.Contains(message, part) {
			t.Fatalf("%q not in miss warning: %s", part, message)
		}
	}
}

func assertL2StrictMissDropsPeers(t *testing.T, policy *as.ClientPolicy, translator string) {
	t.Helper()
	host, nodes := l2Topology(t)
	seed, peers := nodes[0], nodes[1:]
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, seed.port))

	want := []string{seed.name}
	waitL2Misses(t, client, logs, want, peers...)
	for _, peer := range peers {
		assertL2MissMessage(t, logs.misses(peer.name)[0], peer, translator)
	}
	assertL2SeedKeysWork(t, client, seed)
}

// assertL2MappedKeysWork checks that only mapped nodes are masters, that some
// partitions have none, and that every key, owned or not, can be written.
func assertL2MappedKeysWork(t *testing.T, client *as.Client, mapped []string) {
	t.Helper()
	masters := as.PartitionMasters(client, *namespace)
	noMaster := 0
	for _, node := range masters {
		if node == nil {
			noMaster++
		} else if !slices.Contains(mapped, node.GetName()) {
			t.Fatalf("node %s is a master, want only %v", node.GetName(), mapped)
		}
	}
	owned, missed := 0, 0
	for i := range l2KeyCount {
		key := l2Key(t, "n8-", i)
		if masters[key.PartitionId()] == nil {
			missed++
		} else {
			owned++
		}
		assertL2KeyWrites(t, client, nil, key, i)
	}
	if noMaster == 0 || owned == 0 || missed == 0 {
		t.Fatalf("%d partitions without a master, %d keys on mapped masters, %d keys on missed partitions, want all above zero", noMaster, owned, missed)
	}
}

// assertL2SeedKeysWork checks that the seed serves every key routed to it and
// that the keys of partitions no node holds are not routed at all.
func assertL2SeedKeysWork(t *testing.T, client *as.Client, seed l2Node) {
	t.Helper()
	cluster := client.Cluster()
	policy := client.GetDefaultWritePolicy()
	onSeed, notOwned := 0, 0
	for i := range l2MissKeyCount {
		key := l2Key(t, "miss-", i)
		partition, err := as.PartitionForWrite(cluster, &policy.BasePolicy, key)
		if err != nil {
			t.Fatal(err)
		}
		node, err := partition.GetNodeWrite(cluster)
		if err != nil {
			if !err.Matches(types.INVALID_NODE_ERROR) {
				t.Fatal(err)
			}
			notOwned++
			continue
		}
		if node.GetName() != seed.name {
			t.Fatalf("key %v routed to %s, want the seed %s", key, node.GetName(), seed.name)
		}
		assertL2KeyWrites(t, client, policy, key, i)
		onSeed++
	}
	if onSeed == 0 || notOwned == 0 {
		t.Fatalf("%d keys on the seed and %d keys with no node, want both above zero", onSeed, notOwned)
	}
}

func l2Key(t *testing.T, prefix string, i int) *as.Key {
	t.Helper()
	key, err := as.NewKey(*namespace, l2Set, prefix+strconv.Itoa(i))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func l2Writes(client *as.Client, policy *as.WritePolicy, key *as.Key) bool {
	return client.Put(policy, key, as.BinMap{"v": 0}) == nil
}

func assertL2KeyWrites(t *testing.T, client *as.Client, policy *as.WritePolicy, key *as.Key, value int) {
	t.Helper()
	if err := client.Put(policy, key, as.BinMap{"v": value}); err != nil {
		t.Fatalf("put %v: %v", key, err)
	}
	record, err := client.Get(nil, key)
	if err != nil {
		t.Fatalf("get %v: %v", key, err)
	}
	if got := record.Bins["v"]; got != value {
		t.Fatalf("key %v has v=%v, want %d", key, got, value)
	}
}

// l2ActiveNodeNames returns the sorted names of the active nodes other than except.
func l2ActiveNodeNames(client *as.Client, except string) []string {
	var names []string
	for _, node := range client.GetNodes() {
		if node.IsActive() && node.GetName() != except {
			names = append(names, node.GetName())
		}
	}
	slices.Sort(names)
	return names
}

func l2Reachable(endpoint string) bool {
	conn, err := net.DialTimeout("tcp", endpoint, l2DialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

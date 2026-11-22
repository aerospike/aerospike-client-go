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
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	// Longer than the test, so no periodic refresh can run during it.
	l2ReseedRefreshInterval = 10 * time.Minute
	l2ReseedPoll            = 5 * time.Millisecond
	l2TLSKeyCount           = 20
	l2DropThreads           = 4
	l2DropKeyCount          = 20
	l2DropSocketTimeout     = 500 * time.Millisecond
	l2DropTotalTimeout      = 1500 * time.Millisecond
	l2DropMaxRetries        = 2
	l2BreakerMaxErrorRate   = 2
	l2BreakerWindow         = 4
)

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

func (provider *l2CountingSeedProvider) callCount() int {
	return int(provider.calls.Load())
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

// Without a translator the peers' advertised addresses are unreachable, so the
// client starts holding only the seed. This is the behaviour the client has
// always had, and it is kept so existing deployments start the same way; some
// other clients fail init here instead.
func TestL2UnreachablePeersLeaveSeedOnlyClient(t *testing.T) {
	host, nodes := l2Topology(t)
	seed := nodes[0]
	client, err := as.NewClientWithPolicyAndHost(l2Policy(), as.NewHost(host, seed.port))
	if err != nil {
		t.Fatalf("client without a translator failed init with %v, want a client holding only seed %s", err, seed.name)
	}
	defer client.Close()
	if got := client.GetNodeNames(); !slices.Equal(got, []string{seed.name}) {
		t.Fatalf("nodes %v, want only seed %s", got, seed.name)
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

func l2Writes(client *as.Client, policy *as.WritePolicy, key *as.Key) bool {
	return client.Put(policy, key, as.BinMap{"v": 0}) == nil
}

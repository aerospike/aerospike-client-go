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
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	l2PassThroughA = " needs DISCOVERY_PASS_THROUGH_BRANCH=A, each node advertising its own alternate-access-address hostname (run_l2.sh L2_PROFILE=pass-through-names)"
	l2PassThroughB = " needs DISCOVERY_PASS_THROUGH_BRANCH=B and DISCOVERY_CONTROL_DIR, every node advertising one alternate-access-address hostname with its own port (run_l2.sh L2_PROFILE=pass-through-ports)"
)

// l2SingleAddressTranslator maps every endpoint to target. One-caller fake for N-18 custom.
type l2SingleAddressTranslator struct {
	target as.Host
}

func (translator l2SingleAddressTranslator) Translate(as.Endpoint) (as.Host, bool) {
	return translator.target, true
}

func TestL2PassThroughDistinctNamesRouteToEveryOwner(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "A", "N-15"+l2PassThroughA)
	hostnames := map[string]struct{}{}
	for _, node := range nodes {
		alternate := l2Alternate(t, host, node)
		name, port := l2HostPort(t, alternate)
		hostnames[name] = struct{}{}
		if port != node.port {
			t.Fatalf("node %s alternate port %d, want listener %d", node.name, port, node.port)
		}
	}
	if len(hostnames) != len(nodes) {
		t.Fatalf("hostnames %v, want one per node", hostnames)
	}
	assertL2PassThroughRoutes(t, l2PassThroughPolicy(), host, nodes, "n15-")
}

func TestL2PassThroughSharedNameRoutesToEveryOwner(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-16"+l2PassThroughB)
	assertL2PassThroughSharedName(t, host, nodes, l2PassThroughPolicy(), "n16-")
}

func TestL2PassThroughSharedNameStrictRoutesToEveryOwner(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-16 strict"+l2PassThroughB)
	policy := l2PassThroughPolicy()
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2PassThroughSharedName(t, host, nodes, policy, "n16s-")
}

func TestL2PassThroughNewNodeOnSharedNameActiveWithinTwoTendIntervals(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-16 scale-out"+l2PassThroughB)
	controlDir := l2ControlDir(t, "N-16 scale-out"+l2PassThroughB)
	first := nodes[0]
	names := l2NodeNames(nodes)
	hostname, _ := l2HostPort(t, l2Alternate(t, host, first))
	policy := l2PassThroughPolicy()
	client := newL2Client(t, policy, l2PassThroughSeed(t, host, first))
	waitL2Cluster(t, client, names)

	generation := l2PeersGeneration(t, host, first)
	port := nextL2ListenerPort(t)
	started := startL2NodeArgs(t, controlDir, host, nodes, "start alternate-port="+strconv.Itoa(port))
	addL2ListenerOn(t, started.container, port, net.JoinHostPort(started.container, strconv.Itoa(l2UpstreamPort(t, first.proxy))))
	if mapped := l2PeersGeneration(t, host, first); mapped != generation {
		t.Fatalf("peers-generation bumped from %d to %d before %s had a listener", generation, mapped, started.container)
	}

	joined := waitL2Joined(t, client, host, first, generation, names)
	node := joined.node
	info, err := l2RequestInfo(host, port, "node", "service-clear-alt")
	if err != nil {
		t.Fatal(err)
	}
	if info["node"] != node.GetName() {
		t.Fatalf("listener %d serves node %s, client added %s", port, info["node"], node.GetName())
	}
	want := net.JoinHostPort(hostname, strconv.Itoa(port))
	if !strings.Contains(info["service-clear-alt"], want) {
		t.Fatalf("%s advertises %q, want %s", node.GetName(), info["service-clear-alt"], want)
	}
	if node.GetHost().Port != port {
		t.Fatalf("node %s host %v, want listener port %d", node.GetName(), node.GetHost(), port)
	}
	if elapsed, budget := joined.active.Sub(joined.bump), 2*policy.TendInterval; elapsed > budget {
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

func TestL2PassThroughCollidingNodesCreateNoNode(t *testing.T) {
	assertL2PassThroughCollidingNodesCreateNoNode(t, l2PassThroughPolicy(), "pass-through")
}

func TestL2PassThroughStaticMapCollidingNodesCreateNoNode(t *testing.T) {
	policy := l2Policy()
	policy.UseServicesAlternate = true
	assertL2PassThroughCollidingNodesCreateNoNode(t, policy, "static-map")
}

func TestL2PassThroughCustomTranslatorCollidingNodesCreateNoNode(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-18 custom"+l2PassThroughB)
	first := nodes[0]
	seed := l2PassThroughSeed(t, host, first)
	ips, err := net.LookupHost(seed.Name)
	if err != nil || len(ips) == 0 {
		t.Fatalf("lookup %s: %v", seed.Name, err)
	}
	target := as.Host{Name: ips[0], Port: seed.Port}
	policy := l2Policy()
	policy.UseServicesAlternate = true
	policy.AddressTranslator = l2SingleAddressTranslator{target: target}
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, seed)
	waitL2Nodes(t, client, []string{first.name})
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, []string{first.name}) {
			t.Fatalf("nodes %v, want [%s]", got, first.name)
		}
		for _, peer := range nodes[1:] {
			if len(logs.collisions(peer.name)) < l2CollisionRetries {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("custom translator collisions not retried every tend: %v", logs.containing("to the same address"))
	}
	for _, peer := range nodes[1:] {
		assertL2Collision(t, logs.collisions(peer.name)[0], peer.name, first.name, net.JoinHostPort(target.Name, strconv.Itoa(target.Port)), fmt.Sprintf("%T", l2SingleAddressTranslator{}))
	}
	assertL2SeedKeysWork(t, client, first)
}

func TestL2PassThroughPortMapCollidingNodesCreateNoNode(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-18 port-map"+l2PassThroughB)
	first, a, b := nodes[0], nodes[1], nodes[2]
	portMap := l2PortMapByEndpoint(host, nodes)
	for _, advertised := range b.advertised {
		portMap[advertised] = net.JoinHostPort(host, strconv.Itoa(a.port))
	}
	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	policy := l2Policy()
	policy.AddressTranslator = translator
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, first.port))
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, []string{first.name}) {
			t.Fatalf("nodes %v, want [%s]", got, first.name)
		}
		return len(logs.collisions(a.name)) >= l2CollisionRetries && len(logs.collisions(b.name)) >= l2CollisionRetries
	}) {
		t.Fatalf("port-map collisions not retried every tend: %v", logs.containing("to the same address"))
	}
	assertL2Collision(t, logs.collisions(a.name)[0], a.name, b.name, net.JoinHostPort(host, strconv.Itoa(a.port)), "port-map")
	assertL2SeedKeysWork(t, client, first)
}

func TestL2PassThroughNewcomerCollidingWithNodeIsNotCreated(t *testing.T) {
	host, nodes := l2PassThroughBranch(t, "B", "N-18 newcomer"+l2PassThroughB)
	controlDir := l2ControlDir(t, "N-18 newcomer"+l2PassThroughB)
	first := nodes[0]
	existing := nodes[len(nodes)-1]
	names := l2NodeNames(nodes)
	endpoint := l2Alternate(t, host, existing)
	_, port := l2HostPort(t, endpoint)
	client := newL2Client(t, l2PassThroughPolicy(), l2PassThroughSeed(t, host, first))
	waitL2Cluster(t, client, names)

	started := startL2NodeArgs(t, controlDir, host, nodes, "start alternate-port="+strconv.Itoa(port))
	waitL2ClusterSize(t, host, nodes, len(nodes)+1)
	advertising := l2PeerNames(t, host, first, "peers-clear-alt", endpoint)
	advertising = slices.DeleteFunc(advertising, func(name string) bool { return name == existing.name })
	if len(advertising) != 1 {
		t.Fatalf("newcomers advertising %s: %v, want 1 (started %s)", endpoint, advertising, started.container)
	}
	newcomer := advertising[0]
	logs := captureL2Logs(t)
	if !waitL2(l2Poll, func() bool {
		return len(logs.collisions(newcomer)) >= l2CollisionRetries || len(client.GetNodes()) > len(names)
	}) {
		t.Fatalf("no collision logged for %s: %v", newcomer, logs.containing("to the same address"))
	}
	got := slices.Sorted(slices.Values(client.GetNodeNames()))
	if slices.Contains(got, newcomer) {
		t.Fatalf("%s is a Node: %v", newcomer, got)
	}
	if !slices.Equal(got, names) {
		t.Fatalf("nodes %v, want %v", got, names)
	}
	if len(logs.collisions(newcomer)) < l2CollisionRetries {
		t.Fatalf("no collision logged for %s: %v", newcomer, logs.containing("to the same address"))
	}
	hostname, _ := l2HostPort(t, endpoint)
	assertL2Collision(t, logs.collisions(newcomer)[0], newcomer, existing.name, net.JoinHostPort(hostname, strconv.Itoa(port)), "pass-through")
}

func assertL2PassThroughCollidingNodesCreateNoNode(t *testing.T, policy *as.ClientPolicy, translator string) {
	t.Helper()
	host, nodes := l2PassThroughBranch(t, "B", "N-18 "+translator+l2PassThroughB)
	controlDir := l2ControlDir(t, "N-18 "+translator+l2PassThroughB)
	first := nodes[0]
	names := l2NodeNames(nodes)
	hostname, _ := l2HostPort(t, l2Alternate(t, host, first))
	port := nextL2ListenerPort(t)
	endpoint := net.JoinHostPort(hostname, strconv.Itoa(port))
	a := startL2Extra(t, controlDir, "start alternate-port="+strconv.Itoa(port))
	b := startL2Extra(t, controlDir, "start alternate-port="+strconv.Itoa(port))
	stopL2Extras(t, controlDir, host, nodes, []l2StartedNode{a, b})
	waitL2ClusterSize(t, host, nodes, len(nodes)+2)
	addL2ListenerOn(t, a.container, port, net.JoinHostPort(a.container, strconv.Itoa(l2UpstreamPort(t, first.proxy))))

	colliding := l2PeerNames(t, host, first, "peers-clear-alt", endpoint)
	if len(colliding) != 2 {
		t.Fatalf("nodes advertising %s: %v, want 2", endpoint, colliding)
	}
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, l2PassThroughSeed(t, host, first))
	waitL2Nodes(t, client, names)
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, names) {
			t.Fatalf("nodes %v, want %v", got, names)
		}
		return len(logs.collisions(colliding[0])) >= l2CollisionRetries && len(logs.collisions(colliding[1])) >= l2CollisionRetries
	}) {
		t.Fatalf("colliding nodes not retried every tend: %v", logs.containing("to the same address"))
	}
	assertL2Collision(t, logs.collisions(colliding[0])[0], colliding[0], colliding[1], endpoint, translator)
	assertL2OthersServe(t, client, names)
}

func assertL2PassThroughSharedName(t *testing.T, host string, nodes []l2Node, policy *as.ClientPolicy, prefix string) {
	t.Helper()
	hostnames := map[string]struct{}{}
	for _, node := range nodes {
		alternate := l2Alternate(t, host, node)
		name, port := l2HostPort(t, alternate)
		hostnames[name] = struct{}{}
		if port != node.port {
			t.Fatalf("node %s alternate port %d, want listener %d", node.name, port, node.port)
		}
	}
	if len(hostnames) != 1 {
		t.Fatalf("hostnames %v, want 1 shared name", hostnames)
	}
	assertL2PassThroughRoutes(t, policy, host, nodes, prefix)
}

func assertL2PassThroughRoutes(t *testing.T, policy *as.ClientPolicy, host string, nodes []l2Node, prefix string) {
	t.Helper()
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, l2PassThroughSeed(t, host, nodes[0]))
	waitL2Cluster(t, client, l2NodeNames(nodes))
	masters := as.PartitionMasters(client, *namespace)
	distinct := make(map[*as.Node]struct{}, len(nodes))
	for _, master := range masters {
		if master != nil {
			distinct[master] = struct{}{}
		}
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
		assertL2Address(t, l2Alternate(t, host, l2NodeOf(t, nodes, node.GetName())), node.GetHost())
	}
	assertL2ServedByOwners(t, client, host, nodes, prefix)
	if warnings := logs.containing("has no mapping"); len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
}

func assertL2OthersServe(t *testing.T, client *as.Client, names []string) {
	t.Helper()
	masters := as.PartitionMasters(client, *namespace)
	write := as.NewWritePolicy(0, 0)
	write.ReplicaPolicy = as.MASTER
	served, noMaster := 0, 0
	for i := range l2KeyCount {
		key := l2Key(t, "n18-", i)
		node := masters[key.PartitionId()]
		if node == nil {
			noMaster++
			continue
		}
		if !slices.Contains(names, node.GetName()) {
			t.Fatalf("node %s is a master, want only %v", node.GetName(), names)
		}
		assertL2KeyWrites(t, client, write, key, i)
		served++
	}
	if served == 0 || noMaster == 0 {
		t.Fatalf("%d keys served, %d without a master, want both above zero", served, noMaster)
	}
}

func l2PassThroughBranch(t *testing.T, branch, reason string) (string, []l2Node) {
	t.Helper()
	skipUnlessL2(t)
	if os.Getenv("DISCOVERY_PASS_THROUGH_BRANCH") != branch {
		t.Skip(reason)
	}
	if branch == "B" && os.Getenv("DISCOVERY_CONTROL_DIR") == "" {
		t.Skip(reason)
	}
	return l2Discover(t)
}

func l2PassThroughPolicy() *as.ClientPolicy {
	policy := l2Policy()
	policy.UseServicesAlternate = true
	policy.AddressTranslator = as.NewPassThroughAddressTranslator()
	return policy
}

func l2PassThroughSeed(t *testing.T, host string, node l2Node) *as.Host {
	t.Helper()
	name, port := l2HostPort(t, l2Alternate(t, host, node))
	return as.NewHost(name, port)
}

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
	"os"
	"slices"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const l2ReachableBlocker = " needs DISCOVERY_REACHABLE_NODE, a node advertising an address the client can reach (run_l2.sh L2_PROFILE=reachable, or the discovery-l2 workflow's reachable profile)"

func TestL2ReachableUnmappedPeerPassesThroughUnchanged(t *testing.T) {
	host, nodes := l2ReachableTopology(t)
	reachable := l2ReachableNode(t, nodes)
	mapped := slices.DeleteFunc(slices.Clone(nodes), func(node l2Node) bool { return node.proxy == reachable.proxy })
	advertised := reachable.advertised[0]
	portMap := l2PortMapByEndpoint(host, mapped)
	name, port := l2HostPort(t, advertised)
	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := translator.Translate(as.Endpoint{NodeName: reachable.name, Host: name, Port: port, SourceList: as.SourceListStandard}); ok {
		t.Fatalf("%s is mapped", advertised)
	}
	if as.NewClientPolicy().TranslationOnMiss != as.TranslationMissPassThrough {
		t.Fatalf("default TranslationOnMiss is %v, want pass_through", as.NewClientPolicy().TranslationOnMiss)
	}

	logs := captureL2Logs(t)
	client := newL2Client(t, l2PortMapPolicy(t, portMap), as.NewHost(host, mapped[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))

	node, err := client.Cluster().GetNodeByName(reachable.name)
	if err != nil {
		t.Fatal(err)
	}
	if !node.IsActive() {
		t.Fatalf("node %s is not active", reachable.name)
	}
	assertL2Address(t, advertised, node.GetHost())
	misses := logs.misses(reachable.name)
	if len(misses) != 1 {
		t.Fatalf("one miss warning for %s, got %v", reachable.name, misses)
	}
	for _, part := range []string{reachable.name, advertised, "port-map", "Connecting to the advertised address"} {
		if !strings.Contains(misses[0], part) {
			t.Fatalf("%q not in miss warning: %s", part, misses[0])
		}
	}
}

func TestL2ReachableAlternateDefaultJoinsNodesWithAlternate(t *testing.T) {
	assertL2ReachableAlternateJoins(t, l2ReachableAlternatePolicy(), false)
}

func TestL2ReachableAlternateIdentityIpMapStrictJoins(t *testing.T) {
	host, nodes := l2ReachableTopology(t)
	hostname, _ := l2HostPort(t, l2Alternate(t, host, l2ReachableWithAlternate(t, host, nodes)[0]))
	policy := l2ReachableAlternatePolicy()
	policy.IpMap = map[string]string{hostname: hostname}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2ReachableAlternateJoins(t, policy, false)
}

func TestL2ReachableAlternateEmptyCustomPassThroughJoins(t *testing.T) {
	policy := l2ReachableAlternatePolicy()
	policy.AddressTranslator = l2EmptyTranslator{}
	policy.TranslationOnMiss = as.TranslationMissPassThrough
	assertL2ReachableAlternateJoins(t, policy, true)
}

func TestL2ReachableAlternateEmptyIpMapStrictDropsPeer(t *testing.T) {
	policy := l2ReachableAlternatePolicy()
	policy.IpMap = map[string]string{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2ReachableAlternateMisses(t, policy, "static-map")
}

func TestL2ReachableAlternateEmptyCustomStrictDropsPeer(t *testing.T) {
	policy := l2ReachableAlternatePolicy()
	policy.AddressTranslator = l2EmptyTranslator{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2ReachableAlternateMisses(t, policy, fmt.Sprintf("%T", l2EmptyTranslator{}))
}

func TestL2ReachableAlternateSeedWithoutAlternateFailsInit(t *testing.T) {
	host, nodes := l2ReachableTopology(t)
	reachable := l2ReachableNode(t, nodes)
	client, err := as.NewClientWithPolicyAndHost(l2ReachableAlternatePolicy(), as.NewHost(host, reachable.port))
	if err == nil {
		client.Close()
		t.Fatalf("client seeded at %s started with nodes %v, want init to fail", reachable.name, client.GetNodeNames())
	}
	if !err.Matches(types.INVALID_NODE_ERROR) {
		t.Fatalf("seed without alternate failed with %v, want INVALID_NODE_ERROR", err)
	}
	for _, part := range []string{"UseServicesAlternate", reachable.name} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("init error %q does not name %s", err, part)
		}
	}
}

func assertL2ReachableAlternateJoins(t *testing.T, policy *as.ClientPolicy, warnsOnMiss bool) {
	t.Helper()
	host, nodes := l2ReachableTopology(t)
	reachable := l2ReachableNode(t, nodes)
	withAlternate := l2ReachableWithAlternate(t, host, nodes)
	names := l2NodeNames(withAlternate)
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, withAlternate[0].port))
	waitL2Nodes(t, client, names)
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, names) {
			t.Fatalf("nodes %v, want %v", got, names)
		}
		return len(logs.containing("Node "+reachable.name, "UseServicesAlternate")) >= l2MissRetries
	}) {
		t.Fatalf("node %s without alternate not reported every tend: %v", reachable.name, logs.containing("UseServicesAlternate"))
	}
	cluster := client.Cluster()
	for _, expected := range withAlternate {
		node, err := cluster.GetNodeByName(expected.name)
		if err != nil {
			t.Fatal(err)
		}
		assertL2Address(t, l2Alternate(t, host, expected), node.GetHost())
	}
	misses := logs.containing("has no mapping")
	if !warnsOnMiss {
		if len(misses) != 0 {
			t.Fatalf("translation miss logged: %v", misses)
		}
		return
	}
	if len(misses) != len(withAlternate) {
		t.Fatalf("one miss warning per node, got %v", misses)
	}
	for _, expected := range withAlternate {
		found := logs.containing(expected.name, "Connecting to the advertised address")
		if len(found) != 1 {
			t.Fatalf("%s miss warnings: %v", expected.name, misses)
		}
	}
}

func assertL2ReachableAlternateMisses(t *testing.T, policy *as.ClientPolicy, translator string) {
	t.Helper()
	host, nodes := l2ReachableTopology(t)
	withAlternate := l2ReachableWithAlternate(t, host, nodes)
	seed, peer := withAlternate[0], withAlternate[1]
	endpoint := l2Alternate(t, host, peer)
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, seed.port))
	waitL2Misses(t, client, logs, []string{seed.name}, peer)
	message := logs.misses(peer.name)[0]
	for _, part := range []string{peer.name, endpoint, "alternate", translator, "pass_through"} {
		if !strings.Contains(message, part) {
			t.Fatalf("%q not in miss warning: %s", part, message)
		}
	}
	assertL2SeedKeysWork(t, client, seed)
}

func l2ReachableTopology(t *testing.T) (string, []l2Node) {
	t.Helper()
	return l2Profile(t, "N-10"+l2ReachableBlocker, "DISCOVERY_REACHABLE_NODE")
}

func l2ReachableAlternatePolicy() *as.ClientPolicy {
	policy := l2Policy()
	policy.UseServicesAlternate = true
	return policy
}

func l2ReachableNode(t *testing.T, nodes []l2Node) l2Node {
	t.Helper()
	name := os.Getenv("DISCOVERY_REACHABLE_NODE")
	for i, node := range nodes {
		if node.proxy == name {
			if i == 0 {
				t.Fatalf("reachable node %s must not be the first listener", node.name)
			}
			return node
		}
	}
	t.Fatalf("DISCOVERY_REACHABLE_NODE %s has no listener: %v", name, nodes)
	return l2Node{}
}

func l2ReachableWithAlternate(t *testing.T, host string, nodes []l2Node) []l2Node {
	t.Helper()
	var with []l2Node
	for _, node := range nodes {
		if len(l2Alternates(t, host, node)) > 0 {
			with = append(with, node)
		}
	}
	if len(with) < 2 {
		t.Fatalf("fewer than two nodes with an alternate address: %v", nodes)
	}
	return with
}

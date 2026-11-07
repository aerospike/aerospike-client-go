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
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const l2PortMapRangeBlocker = " needs DISCOVERY_PORT_MAP_RANGE, DISCOVERY_PORT_MAP_PORT_BASE and DISCOVERY_CONTROL_DIR, nodes on a fixed subnet with a listener per address of a range (run_l2.sh L2_PROFILE=port-map-range)"

func TestL2PortMapRangePrefilledJoinsNodeInRange(t *testing.T) {
	host, nodes := l2PortMapRangeTopology(t)
	controlDir := l2ControlDir(t, "N-19"+l2PortMapRangeBlocker)
	first := nodes[0]
	names := l2NodeNames(nodes)
	portMap := l2RangePortMap(t, host, nodes)
	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	policy := l2Policy()
	policy.AddressTranslator = translator
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, first.port))
	waitL2Cluster(t, client, names)
	if warnings := logs.containing("has no mapping"); len(warnings) != 0 {
		t.Fatalf("unexpected warnings before scale-out: %v", warnings)
	}

	address := l2FreeRangeAddress(t, nodes)
	generation := l2PeersGeneration(t, host, first)
	started := startL2NodeArgs(t, controlDir, host, nodes, "start address="+address)
	if started.address != address {
		t.Fatalf("started address %s, want %s", started.address, address)
	}

	joined := waitL2Joined(t, client, host, first, generation, names)
	node := joined.node
	port := l2RangeListenerPort(t, address)
	info, infoErr := l2RequestInfo(host, port, "node", "service-clear-std")
	if infoErr != nil {
		t.Fatal(infoErr)
	}
	if info["node"] != node.GetName() {
		t.Fatalf("listener %d serves node %s, client added %s", port, info["node"], node.GetName())
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
	owners, _ := l2PartitionOwners(client)
	if len(owners) != len(names)+1 {
		t.Fatalf("partition owners %v, want %d nodes", owners, len(names)+1)
	}
}

func TestL2PortMapRangeNodeOutsideRangeDecidedByOnMiss(t *testing.T) {
	host, nodes := l2PortMapRangeTopology(t)
	controlDir := l2ControlDir(t, "N-19"+l2PortMapRangeBlocker)
	first := nodes[0]
	names := l2NodeNames(nodes)
	portMap := l2RangePortMap(t, host, nodes)
	_, to := l2PortMapRangeBounds(t)
	address := l2AddressPrefix(to) + strconv.Itoa(l2LastOctet(to)+1)
	advertised := net.JoinHostPort(address, strconv.Itoa(l2ServicePort(nodes)))

	started := startL2NodeArgs(t, controlDir, host, nodes, "start address="+address)
	if started.address != address {
		t.Fatalf("started address %s, want %s", started.address, address)
	}
	waitL2ClusterSize(t, host, nodes, len(nodes)+1)

	name := l2PeerName(t, host, first, "peers-clear-std", advertised)
	if name == "" {
		t.Fatalf("no peer advertises %s", advertised)
	}
	if _, ok := portMap[advertised]; ok {
		t.Fatalf("%s is in the pre-filled port-map", advertised)
	}

	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	strict := l2Policy()
	strict.AddressTranslator = translator
	strict.TranslationOnMiss = as.TranslationMissStrict
	logs := captureL2Logs(t)
	client := newL2Client(t, strict, as.NewHost(host, first.port))
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, names) {
			t.Fatalf("nodes %v, want %v", got, names)
		}
		return len(logs.misses(name)) >= l2MissRetries
	}) {
		t.Fatalf("missed node %s not retried every tend: %v", name, logs.containing("has no mapping"))
	}
	message := logs.misses(name)[0]
	for _, part := range []string{name, advertised, "standard", "port-map", "pass_through"} {
		if !strings.Contains(message, part) {
			t.Fatalf("%q not in miss warning: %s", part, message)
		}
	}

	passThrough := l2Policy()
	passThrough.AddressTranslator = translator
	if passThrough.TranslationOnMiss != as.TranslationMissPassThrough {
		t.Fatalf("default TranslationOnMiss is %v, want pass_through", passThrough.TranslationOnMiss)
	}
	logs = captureL2Logs(t)
	client = newL2Client(t, passThrough, as.NewHost(host, first.port))
	tried := "Add node `" + advertised + "`"
	if !waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, names) {
			t.Fatalf("nodes %v, want %v", got, names)
		}
		return len(logs.containing(tried)) >= l2MissRetries
	}) {
		t.Fatalf("unchanged address %s not tried every tend: %v", advertised, logs.containing("Add node"))
	}
	misses := logs.misses(name)
	if len(misses) != 1 {
		t.Fatalf("one miss warning for %s across retries: %v", name, misses)
	}
	if !strings.Contains(misses[0], "Connecting to the advertised address") {
		t.Fatalf("miss warning is not the pass_through one: %s", misses[0])
	}
}

func l2PortMapRangeTopology(t *testing.T) (string, []l2Node) {
	t.Helper()
	return l2Profile(t, "N-19"+l2PortMapRangeBlocker, "DISCOVERY_PORT_MAP_RANGE", "DISCOVERY_PORT_MAP_PORT_BASE", "DISCOVERY_CONTROL_DIR")
}

func l2RangePortMap(t *testing.T, host string, nodes []l2Node) map[string]string {
	t.Helper()
	from, to := l2PortMapRangeBounds(t)
	prefix := l2AddressPrefix(from)
	port := l2ServicePort(nodes)
	listeners := l2ListenerPorts(t)
	portMap := make(map[string]string)
	for octet := l2LastOctet(from); octet <= l2LastOctet(to); octet++ {
		address := prefix + strconv.Itoa(octet)
		listener := l2RangeListenerPort(t, address)
		if _, ok := listeners[listener]; !ok {
			t.Fatalf("no listener on %d for %s: %v", listener, address, listeners)
		}
		portMap[net.JoinHostPort(address, strconv.Itoa(port))] = net.JoinHostPort(host, strconv.Itoa(listener))
	}
	for _, node := range nodes {
		if _, ok := portMap[node.advertised[0]]; !ok {
			t.Fatalf("%s outside %s", node.advertised, os.Getenv("DISCOVERY_PORT_MAP_RANGE"))
		}
	}
	return portMap
}

func l2FreeRangeAddress(t *testing.T, nodes []l2Node) string {
	t.Helper()
	from, to := l2PortMapRangeBounds(t)
	prefix := l2AddressPrefix(from)
	port := l2ServicePort(nodes)
	used := map[string]struct{}{}
	for _, node := range nodes {
		for _, advertised := range node.advertised {
			used[advertised] = struct{}{}
		}
	}
	for octet := l2LastOctet(from); octet <= l2LastOctet(to); octet++ {
		address := prefix + strconv.Itoa(octet)
		if _, ok := used[net.JoinHostPort(address, strconv.Itoa(port))]; !ok {
			return address
		}
	}
	t.Fatalf("no free address in %s", os.Getenv("DISCOVERY_PORT_MAP_RANGE"))
	return ""
}

func l2PortMapRangeBounds(t *testing.T) (string, string) {
	t.Helper()
	from, to, ok := strings.Cut(os.Getenv("DISCOVERY_PORT_MAP_RANGE"), "-")
	if !ok || l2AddressPrefix(from) != l2AddressPrefix(to) {
		t.Fatalf("DISCOVERY_PORT_MAP_RANGE %q is not first-last on one prefix", os.Getenv("DISCOVERY_PORT_MAP_RANGE"))
	}
	return from, to
}

func l2RangeListenerPort(t *testing.T, address string) int {
	t.Helper()
	base, err := strconv.Atoi(os.Getenv("DISCOVERY_PORT_MAP_PORT_BASE"))
	if err != nil {
		t.Fatal(err)
	}
	return base + l2LastOctet(address)
}

func l2ServicePort(nodes []l2Node) int {
	_, port, _ := net.SplitHostPort(nodes[0].advertised[0])
	n, _ := strconv.Atoi(port)
	return n
}

func l2AddressPrefix(address string) string {
	return address[:strings.LastIndexByte(address, '.')+1]
}

func l2LastOctet(address string) int {
	n, _ := strconv.Atoi(address[strings.LastIndexByte(address, '.')+1:])
	return n
}

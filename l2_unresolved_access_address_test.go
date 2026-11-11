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
)

const l2UnresolvedAccessBlocker = "Unresolved access address needs DISCOVERY_UNRESOLVED_HOSTNAME and DISCOVERY_CONTROL_DIR (run_l2.sh L2_PROFILE=unresolved-access)"

// A node advertising an unresolvable alternate-access-address joins verbatim on a fixed server.
// A released server exits at startup and logs CRITICAL naming the hostname.
func TestL2UnresolvedAccessAddressJoinsOrExitsByServer(t *testing.T) {
	host, nodes := l2Profile(t, l2UnresolvedAccessBlocker, "DISCOVERY_UNRESOLVED_HOSTNAME", "DISCOVERY_CONTROL_DIR")
	controlDir := os.Getenv("DISCOVERY_CONTROL_DIR")
	hostname := os.Getenv("DISCOVERY_UNRESOLVED_HOSTNAME")
	port := nextL2ListenerPort(t)
	started := startL2NodeArgs(t, controlDir, host, nodes, "start alternate-port="+strconv.Itoa(port))
	if os.Getenv("DISCOVERY_VERBATIM_ACCESS_ADDRESS") == "1" {
		assertL2UnresolvedNodeJoins(t, controlDir, host, nodes, started, hostname, port)
		return
	}
	assertL2UnresolvedNodeExits(t, controlDir, host, nodes, started, hostname)
}

// assertL2UnresolvedNodeJoins checks the started node joins and peers list its alternate verbatim.
func assertL2UnresolvedNodeJoins(t *testing.T, controlDir, host string, nodes []l2Node, started l2StartedNode, hostname string, port int) {
	t.Helper()
	first := nodes[0]
	endpoint := net.JoinHostPort(hostname, strconv.Itoa(port))
	addL2ListenerOn(t, started.container, port, net.JoinHostPort(started.container, strconv.Itoa(l2UpstreamPort(t, first.proxy))))
	waitL2ClusterSize(t, host, nodes, len(nodes)+1)
	status := l2Control(t, controlDir, "status "+started.container)
	assertL2Unresolvable(t, status, hostname)
	if status["running"] != "true" {
		t.Fatalf("%s running %q, want true", started.container, status["running"])
	}
	info, err := l2RequestInfo(host, port, "node", "service-clear-alt")
	if err != nil {
		t.Fatal(err)
	}
	advertised := strings.TrimSpace(info["service-clear-alt"])
	peer := l2PeerName(t, host, first, "peers-clear-alt", endpoint)
	t.Logf("L2-UNRESOLVED-ADVERTISED node=%s service-clear-alt=%s listedBy=%s as=%s", info["node"], advertised, first.name, peer)
	if advertised != endpoint {
		t.Fatalf("service-clear-alt %q, want %s", advertised, endpoint)
	}
	if peer != info["node"] {
		t.Fatalf("peer listed as %q, want %s", peer, info["node"])
	}
}

// assertL2UnresolvedNodeExits checks the started node exits non-zero with a CRITICAL log naming hostname.
func assertL2UnresolvedNodeExits(t *testing.T, controlDir, host string, nodes []l2Node, started l2StartedNode, hostname string) {
	t.Helper()
	var status map[string]string
	exited := waitL2(l2Poll, func() bool {
		status = l2Control(t, controlDir, "status "+started.container)
		return status["running"] != "true"
	})
	if !exited {
		t.Fatalf("%s still running after %s", started.container, l2Wait)
	}
	assertL2Unresolvable(t, status, hostname)
	if status["exit-code"] == "0" {
		t.Fatalf("%s exit code %q, want non-zero", started.container, status["exit-code"])
	}
	lines := l2UnresolvedLogLines(t, status["log"], hostname)
	t.Logf("L2-UNRESOLVED-LOG %q", lines)
	if !slices.ContainsFunc(lines, func(line string) bool { return strings.Contains(line, "CRITICAL") }) {
		t.Fatalf("no CRITICAL log line names %s: %v", hostname, lines)
	}
	waitL2ClusterSize(t, host, nodes, len(nodes))
}

// assertL2Unresolvable checks the node's own lookup of hostname found no address.
func assertL2Unresolvable(t *testing.T, status map[string]string, hostname string) {
	t.Helper()
	t.Logf("L2-UNRESOLVED-LOOKUP getent hosts %s in=%s exit=%s output=[%s]",
		hostname, status["lookup-in"], status["lookup-exit"], strings.TrimSpace(status["lookup"]))
	if status["lookup-in"] == "" {
		t.Fatal("no lookup")
	}
	if status["lookup-exit"] != "2" {
		t.Fatalf("%s resolves: %s", hostname, status["lookup"])
	}
}

// l2UnresolvedLogLines returns log lines naming hostname, minus the config echo.
func l2UnresolvedLogLines(t *testing.T, log, hostname string) []string {
	t.Helper()
	body, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	echo := "alternate-access-address " + hostname
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.Contains(line, hostname) && !strings.Contains(line, echo) {
			lines = append(lines, line)
		}
	}
	return lines
}

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
	"strconv"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const l2PortMapNoAccessBlocker = "Port map with no access address needs DISCOVERY_UNRESOLVED_HOSTNAME (run_l2.sh L2_PROFILE=unresolved-access)"

// With no access address configured, a port map still reaches every node at its listener
// and each key is served by its master.
func TestL2PortMapNoAccessAddressRoutesEveryPartition(t *testing.T) {
	host, nodes := l2Profile(t, l2PortMapNoAccessBlocker, "DISCOVERY_UNRESOLVED_HOSTNAME")
	for _, node := range nodes {
		if alternates := l2Alternates(t, host, node); len(alternates) != 0 {
			t.Fatalf("node %s alternate list %v, want none", node.name, alternates)
		}
	}

	client := newL2Client(t, l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes)), as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))
	for _, node := range client.GetNodes() {
		listener := l2NodeOf(t, nodes, node.GetName()).port
		assertL2Address(t, net.JoinHostPort(host, strconv.Itoa(listener)), node.GetHost())
	}
	assertL2ServedByOwners(t, client, host, nodes, "no-access-")
}

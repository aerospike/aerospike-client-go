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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const l2SourcePortBlocker = "F-3 needs DISCOVERY_PROXY_PORT_RANGE, Toxiproxy with a narrow net.ipv4.ip_local_port_range (run_l2.sh L2_PROFILE=port-range)"

func TestL2SourcePortExhaustionSurfacesErrorAndRecovers(t *testing.T) {
	host, nodes := l2SourcePortTopology(t)
	fields := strings.Fields(os.Getenv("DISCOVERY_PROXY_PORT_RANGE"))
	if len(fields) != 2 {
		t.Fatalf("DISCOVERY_PROXY_PORT_RANGE %q is not '<first> <last>'", os.Getenv("DISCOVERY_PROXY_PORT_RANGE"))
	}
	first, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatal(err)
	}
	last, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	ports := last - first + 1
	threads := 2 * ports * len(nodes)
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	policy.ConnectionQueueSize = 4 * threads
	client := newL2Client(t, policy, as.NewHost(host, nodes[0].port))
	// transaction-error-count only counts while metrics are enabled.
	client.EnableMetrics(nil)
	waitL2Cluster(t, client, l2NodeNames(nodes))

	write := as.NewWritePolicy(0, 0)
	write.SocketTimeout = 2 * time.Second
	write.TotalTimeout = 4 * time.Second
	bound := write.TotalTimeout + write.SocketTimeout
	start := l2ConnectionStats(t, client)
	keys := make([]*as.Key, 1000)
	for i := range keys {
		keys[i] = l2Key(t, "f3-", i)
	}
	load := startL2Load(threads, func(thread, i int) as.Error {
		return client.Put(write, keys[(thread*31+i)%len(keys)], as.BinMap{"v": i})
	})
	waitL2(l2Poll, func() bool { return len(load.failures()) >= threads })
	finished := load.stopWithin(bound)
	failures := load.failures()
	if !finished {
		t.Fatalf("command threads still blocked %s after stop: %s", bound, load)
	}
	if len(failures) == 0 {
		t.Fatalf("%d threads never exceeded the proxy's %d source ports per node", threads, ports)
	}
	slowest := failures[0].elapsed
	for _, failure := range failures {
		if failure.elapsed > bound {
			t.Fatalf("an error took %s, bound %s", failure.elapsed, bound)
		}
		if failure.elapsed > slowest {
			slowest = failure.elapsed
		}
		if failure.err.Matches(types.TIMEOUT) {
			t.Fatalf("source port exhaustion surfaced as a client timeout: %v", failure.err)
		}
	}

	end := l2ConnectionStats(t, client)
	errorDelta := int64(0)
	for name, after := range end {
		errorDelta += after.errors - start[name].errors
	}
	if errorDelta == 0 {
		t.Fatalf("client counted no error: %+v", end)
	}

	read := as.NewPolicy()
	recovered := waitL2(l2Poll, func() bool {
		for _, node := range client.GetNodes() {
			if !node.IsActive() {
				return false
			}
		}
		return l2RoundSucceeds(client, write, read, 4*len(nodes))
	})
	if !recovered {
		t.Fatal("no successful round of commands after the load dropped")
	}
	for i := range 100 {
		key := l2Key(t, "f3-", i)
		if err := client.Put(write, key, as.BinMap{"v": i}); err != nil {
			t.Fatalf("put after recovery: %v", err)
		}
		if _, err := client.Get(read, key); err != nil {
			t.Fatalf("get after recovery: %v", err)
		}
	}
}

func l2SourcePortTopology(t *testing.T) (string, []l2Node) {
	t.Helper()
	return l2Profile(t, l2SourcePortBlocker, "DISCOVERY_PROXY_PORT_RANGE")
}

func l2RoundSucceeds(client *as.Client, write *as.WritePolicy, read *as.BasePolicy, count int) bool {
	for i := range count {
		key, err := as.NewKey(*namespace, l2Set, "f3-round-"+strconv.Itoa(i))
		if err != nil {
			return false
		}
		if client.Put(write, key, as.BinMap{"v": i}) != nil {
			return false
		}
		if _, err := client.Get(read, key); err != nil {
			return false
		}
	}
	return true
}

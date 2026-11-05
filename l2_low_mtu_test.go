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
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const l2LowMtuBlocker = "F-2 needs DISCOVERY_CLIENT_NETWORK_MTU, a client network created with a small MTU (run_l2.sh L2_PROFILE=low-mtu)"

func TestL2LowMtuLargeRecordNeverHangs(t *testing.T) {
	host, nodes := l2LowMtuTopology(t)
	raw, err := os.ReadFile("/sys/class/net/eth0/mtu")
	if err != nil {
		t.Fatal(err)
	}
	mtu, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	want, err := strconv.Atoi(os.Getenv("DISCOVERY_CLIENT_NETWORK_MTU"))
	if err != nil {
		t.Fatal(err)
	}
	if mtu != want {
		t.Fatalf("eth0 MTU %d, want %d", mtu, want)
	}

	client := newL2Client(t, l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes)), as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))
	masters := as.PartitionMasters(client, *namespace)
	write := as.NewWritePolicy(0, 0)
	write.SocketTimeout = 5 * time.Second
	write.TotalTimeout = 10 * time.Second
	read := as.NewPolicy()
	read.SocketTimeout = write.SocketTimeout
	read.TotalTimeout = write.TotalTimeout
	bound := write.TotalTimeout + write.SocketTimeout
	rng := rand.New(rand.NewSource(int64(mtu)))

	for _, node := range client.GetNodes() {
		key := l2KeyOn(t, masters, node, "f2-")
		for _, size := range []int{64 * 1024, 1024 * 1024} {
			value := make([]byte, size)
			rng.Read(value)
			writeResult := runL2Bounded(t, bound, func() as.Error {
				return client.Put(write, key, as.BinMap{"v": value})
			})
			var got []byte
			readResult := runL2Bounded(t, bound, func() as.Error {
				record, err := client.Get(read, key)
				if err != nil {
					return err
				}
				got, _ = record.Bins["v"].([]byte)
				return nil
			})
			if readResult[:2] == "ok" && !bytes.Equal(got, value) {
				t.Fatalf("node %s %d-byte record mismatch", node.GetName(), size)
			}
			t.Logf("node=%s bytes=%d write=%s read=%s", node.GetName(), size, writeResult, readResult)
		}
	}
}

func l2LowMtuTopology(t *testing.T) (string, []l2Node) {
	t.Helper()
	return l2Profile(t, l2LowMtuBlocker, "DISCOVERY_CLIENT_NETWORK_MTU")
}

func l2KeyOn(t *testing.T, masters []*as.Node, node *as.Node, prefix string) *as.Key {
	t.Helper()
	for i := 0; ; i++ {
		key := l2Key(t, prefix, i)
		if masters[key.PartitionId()] == node {
			return key
		}
	}
}

func runL2Bounded(t *testing.T, bound time.Duration, command func() as.Error) string {
	t.Helper()
	done := make(chan as.Error, 1)
	start := time.Now()
	go func() { done <- command() }()
	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err == nil {
			return "ok " + elapsed.String()
		}
		if err.Matches(types.TIMEOUT) {
			t.Fatalf("client timeout after %s: %v", elapsed, err)
		}
		return "error " + elapsed.String() + " " + err.Error()
	case <-time.After(bound):
		t.Fatalf("command still running after %s", bound)
		return ""
	}
}

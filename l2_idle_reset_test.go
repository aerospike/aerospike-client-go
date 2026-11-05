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
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const l2IdleResetBlocker = "F-1 needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)"

func TestL2IdleResetAbsorbedByPool(t *testing.T) {
	host, nodes := l2IdleResetTopology(t)
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	if policy.IdleTimeout != as.NewClientPolicy().IdleTimeout {
		t.Fatalf("IdleTimeout %s, want the default %s", policy.IdleTimeout, as.NewClientPolicy().IdleTimeout)
	}
	for _, node := range nodes {
		addL2Toxic(t, node.proxy, "f1-rst", "reset_peer", "upstream", 0, `{"timeout":0}`)
	}

	client := newL2Client(t, policy, as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))

	write := as.NewWritePolicy(0, 0)
	read := as.NewPolicy()
	keys := make([]*as.Key, 100)
	for i := range keys {
		keys[i] = l2Key(t, "f1-", i)
	}
	load := startL2Load(4, func(thread, i int) as.Error {
		key := keys[(thread*50+i)%len(keys)]
		if err := client.Put(write, key, as.BinMap{"v": i}); err != nil {
			return err
		}
		_, err := client.Get(read, key)
		return err
	})
	if !waitL2(l2Poll, func() bool { return load.successes.Load()+int64(len(load.failures())) >= 200 }) {
		t.Fatalf("warmup did not finish: %s", load)
	}
	if !load.stopWithin(l2Wait) {
		t.Fatal("warmup threads blocked")
	}
	if failures := load.failures(); len(failures) != 0 {
		t.Fatalf("warmup errors: %v", failures[0].err)
	}

	probes := make([]*net.TCPConn, 0, len(nodes))
	t.Cleanup(func() {
		for _, probe := range probes {
			_ = probe.Close()
		}
	})
	for _, node := range nodes {
		probes = append(probes, l2IdleProbe(t, host, node.port))
	}

	pooled := 0
	for _, node := range client.GetNodes() {
		pooled += node.ConnsCount()
	}
	if pooled < len(nodes) {
		t.Fatalf("no idle pooled connection to reset, open connections %d", pooled)
	}

	for _, node := range nodes {
		disableL2Listener(t, node.proxy)
		enableL2Listener(t, node.proxy)
	}
	for i, node := range nodes {
		result := l2IdleProbeResult(probes[i])
		if !strings.HasPrefix(result, "RST") {
			t.Fatalf("listener %d did not reset an idle flow with RST: %s", node.port, result)
		}
	}

	start := l2ConnectionStats(t, client)
	var failures []string
	for i := range 100 {
		key := l2Key(t, "f1-", i)
		if err := client.Put(write, key, as.BinMap{"v": i}); err != nil {
			failures = append(failures, "put "+key.String()+": "+err.Error())
		}
		if _, err := client.Get(read, key); err != nil {
			failures = append(failures, "get "+key.String()+": "+err.Error())
		}
	}
	end := l2ConnectionStats(t, client)
	var undetected []string
	for name, after := range end {
		if after.closed-start[name].closed == 0 {
			undetected = append(undetected, name)
		}
	}
	if len(undetected) != 0 {
		t.Fatalf("no reset pooled connection was detected on %v", undetected)
	}
	if len(failures) != 0 {
		t.Fatalf("commands failed to the caller after idle RST: %v", failures)
	}
}

func l2IdleResetTopology(t *testing.T) (string, []l2Node) {
	t.Helper()
	return l2Profile(t, l2IdleResetBlocker, "DISCOVERY_IDLE_RESET")
}

func l2IdleProbe(t *testing.T, host string, port int) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tcp := conn.(*net.TCPConn)
	_ = tcp.SetDeadline(time.Now().Add(5 * time.Second))
	command := []byte("node\n")
	header := make([]byte, 8+len(command))
	binary.BigEndian.PutUint64(header, (2<<56)|(1<<48)|uint64(len(command)))
	copy(header[8:], command)
	if _, err := tcp.Write(header); err != nil {
		t.Fatal(err)
	}
	var replyHeader [8]byte
	if _, err := io.ReadFull(tcp, replyHeader[:]); err != nil {
		t.Fatal(err)
	}
	length := int(binary.BigEndian.Uint64(replyHeader[:]) & 0xFFFFFFFFFFFF)
	reply := make([]byte, length)
	if _, err := io.ReadFull(tcp, reply); err != nil {
		t.Fatal(err)
	}
	_ = tcp.SetDeadline(time.Time{})
	return tcp
}

func l2IdleProbeResult(conn *net.TCPConn) string {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var b [1]byte
	n, err := conn.Read(b[:])
	if err == nil {
		return "DATA"
	}
	if n == 0 && err == io.EOF {
		return "FIN"
	}
	if strings.Contains(strings.ToLower(err.Error()), "reset") {
		return "RST " + err.Error()
	}
	return "ERROR " + err.Error()
}

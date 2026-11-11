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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

func TestL5OptionsBuildsThePassThroughPolicy(t *testing.T) {
	opts := l5MustOptions(t, "", func(string) (string, bool) { return "", false })
	if opts.workers != 4 || opts.keys != 1000 {
		t.Fatalf("defaults workers=%d keys=%d", opts.workers, opts.keys)
	}
	if opts.seed.Name != "127.0.0.1" || opts.seed.Port != 3000 {
		t.Fatalf("seed = %s:%d", opts.seed.Name, opts.seed.Port)
	}

	policy := opts.policy()
	if !policy.UseServicesAlternate {
		t.Fatal("pass-through leaves UseServicesAlternate unset")
	}
	if policy.TranslationOnMiss != as.TranslationMissStrict {
		t.Fatal("on-miss defaults to strict, which is not the ClientPolicy zero value")
	}
	if policy.Timeout != 1000*time.Millisecond || policy.TendInterval != 250*time.Millisecond {
		t.Fatalf("timeout %s tend %s", policy.Timeout, policy.TendInterval)
	}
	if opts.read.SocketTimeout != policy.Timeout || opts.read.TotalTimeout != policy.Timeout || opts.read.MaxRetries != 0 {
		t.Fatalf("read policy timeout=%s/%s retries=%d", opts.read.SocketTimeout, opts.read.TotalTimeout, opts.read.MaxRetries)
	}
	if opts.write.SocketTimeout != policy.Timeout || opts.write.TotalTimeout != policy.Timeout || opts.write.MaxRetries != 0 {
		t.Fatalf("write policy timeout=%s/%s retries=%d", opts.write.SocketTimeout, opts.write.TotalTimeout, opts.write.MaxRetries)
	}

	host, ok := policy.AddressTranslator.Translate(as.Endpoint{Host: "10.1.1.1", Port: 3000, TLSName: "tls"})
	if !ok || host.Name != "10.1.1.1" || host.Port != 3000 || host.TLSName != "tls" {
		t.Fatalf("pass-through translate = %+v ok=%t", host, ok)
	}
}

func TestL5OptionsBuildsThePortMapPolicy(t *testing.T) {
	opts := l5MustOptions(t, "--seed 127.0.0.1:3000 --shape port-map --port-map n1=[2001:db8::1]:4000 --on-miss pass-through --tend-interval 250 --timeout 1000 --namespace test --set smoke --workers 2 --keys 3", nil)
	policy := opts.policy()
	if policy.UseServicesAlternate {
		t.Fatal("port-map set UseServicesAlternate")
	}
	if policy.TranslationOnMiss != as.TranslationMissPassThrough {
		t.Fatal("on-miss pass-through was not applied")
	}
	if opts.workers != 2 || opts.keys != 3 {
		t.Fatalf("workers=%d keys=%d", opts.workers, opts.keys)
	}
	host, ok := policy.AddressTranslator.Translate(as.Endpoint{NodeName: "n1", Host: "10.0.0.8", Port: 3000})
	if !ok || host.Name != "2001:db8::1" || host.Port != 4000 {
		t.Fatalf("port-map translate = %+v ok=%t", host, ok)
	}
}

func TestL5OptionsReadsTheAdminPassword(t *testing.T) {
	opts := l5MustOptions(t, l5BaseArgs()+" --user admin", func(key string) (string, bool) {
		if key == "AEROSPIKE_ADMIN_PASSWORD" {
			return "secret", true
		}
		return "", false
	})
	policy := opts.policy()
	if policy.User != "admin" || policy.Password != "secret" {
		t.Fatalf("user %q password %q", policy.User, policy.Password)
	}
}

func TestL5OptionsRejectsBadFlags(t *testing.T) {
	cases := []struct {
		name string
		args string
		env  func(string) (string, bool)
		want string
	}{
		{name: "missing seed", args: "--shape pass-through --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "missing --seed"},
		{name: "missing shape", args: "--seed 127.0.0.1:3000 --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "missing --shape"},
		{name: "missing tend", args: "--seed 127.0.0.1:3000 --shape pass-through --timeout 1000 --namespace test --set smoke", want: "missing --tend-interval"},
		{name: "missing timeout", args: "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 250 --namespace test --set smoke", want: "missing --timeout"},
		{name: "missing namespace", args: "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 250 --timeout 1000 --set smoke", want: "missing --namespace"},
		{name: "missing set", args: "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 250 --timeout 1000 --namespace test", want: "missing --set"},
		{name: "bad shape", args: "--seed 127.0.0.1:3000 --shape both --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "--shape must be pass-through or port-map"},
		{name: "bad on-miss", args: l5BaseArgs() + " --on-miss drop", want: "--on-miss must be strict or pass-through"},
		{name: "port map without shape", args: l5BaseArgs() + " --port-map n1=127.0.0.1:3000", want: "--port-map only with --shape port-map"},
		{name: "shape without port map", args: "--seed 127.0.0.1:3000 --shape port-map --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "missing --port-map with --shape port-map"},
		{name: "bad port map", args: "--seed 127.0.0.1:3000 --shape port-map --port-map n1=nope --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "--port-map entry must be NODE=HOST:PORT"},
		{name: "duplicate port map name", args: "--seed 127.0.0.1:3000 --shape port-map --port-map n1=127.0.0.1:3000,n1=127.0.0.1:3001 --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "--port-map names n1 twice"},
		{name: "bad seed", args: "--seed 127.0.0.1 --shape pass-through --tend-interval 250 --timeout 1000 --namespace test --set smoke", want: "--seed must be HOST:PORT"},
		{name: "workers", args: l5BaseArgs() + " --workers 0", want: "--workers must be a positive integer"},
		{name: "keys", args: l5BaseArgs() + " --keys -5", want: "--keys must be a positive integer"},
		{name: "timeout", args: "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 250 --timeout 1ms --namespace test --set smoke", want: "--timeout must be a positive integer"},
		{name: "duplicate", args: l5BaseArgs() + " --seed 127.0.0.1:3001", want: "--seed given twice"},
		{name: "unknown", args: l5BaseArgs() + " --cluster test", want: "unknown flag: --cluster"},
		{name: "positional", args: "extra " + l5BaseArgs(), want: "unexpected argument: extra"},
		{name: "dangling", args: l5BaseArgs() + " --user", want: "--user needs a value"},
		{name: "password", args: l5BaseArgs() + " --user admin", want: "AEROSPIKE_ADMIN_PASSWORD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := tc.env
			if getenv == nil {
				getenv = func(string) (string, bool) { return "", false }
			}
			_, err := parseL5Options(strings.Fields(tc.args), getenv)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestL5OptionsAcceptsAnIPv6Seed(t *testing.T) {
	opts := l5MustOptions(t, "--seed [::1]:3000 --shape pass-through --tend-interval 250 --timeout 1000 --namespace test --set smoke", nil)
	if opts.seed.Name != "::1" || opts.seed.Port != 3000 {
		t.Fatalf("seed = %s:%d", opts.seed.Name, opts.seed.Port)
	}
}

func TestL5OutputPrintsReadyBeforeHeldLines(t *testing.T) {
	var buf bytes.Buffer
	out := &l5Output{stdout: &buf}
	out.write(l5LogLine{Type: "log", T: 3, Level: "WARN", Message: "early"})
	if buf.Len() != 0 {
		t.Fatalf("held line reached stdout: %s", buf.String())
	}

	out.open(l5Ready{
		Type: "ready", T: 9, Contract: l5Contract, EpochMs: 1700000000000,
		Client: "go", ClientVersion: "v", Shape: "port-map",
	})
	lines := l5Decode(t, &buf)
	if len(lines) != 2 || lines[0]["type"] != "ready" || lines[1]["type"] != "log" {
		t.Fatalf("lines = %#v", lines)
	}
	if lines[0]["contract"] != float64(l5Contract) || lines[0]["client"] != "go" || lines[0]["t"] != float64(9) {
		t.Fatalf("ready = %#v", lines[0])
	}
	if lines[1]["t"] != float64(9) || lines[1]["message"] != "early" {
		t.Fatalf("held log = %#v", lines[1])
	}
}

func TestL5OutputDoesNotMoveTBackwards(t *testing.T) {
	var buf bytes.Buffer
	out := &l5Output{stdout: &buf}
	out.open(l5Ready{Type: "ready", T: 1, Contract: l5Contract, Client: "go", ClientVersion: "v", Shape: "pass-through"})
	buf.Reset()
	out.write(l5LogLine{Type: "log", T: 15550, Level: "WARN", Message: "refresh failed"})
	out.write(l5Message{Type: "error", T: 14631, Message: "late poll"})
	lines := l5Decode(t, &buf)
	if len(lines) != 2 || lines[0]["t"] != float64(15550) || lines[1]["t"] != float64(15550) {
		t.Fatalf("lines = %#v", lines)
	}
}

func TestL5OutputWriteNowSkipsTheHold(t *testing.T) {
	var buf bytes.Buffer
	out := &l5Output{stdout: &buf}
	out.write(l5Message{Type: "error", T: 1, Message: "held"})
	out.writeNow(l5Message{Type: "error", T: 2, Message: "client not created: refused"})
	lines := l5Decode(t, &buf)
	if len(lines) != 1 || lines[0]["message"] != "client not created: refused" {
		t.Fatalf("before open = %#v", lines)
	}
	out.open(l5Ready{Type: "ready", T: 4, Contract: l5Contract, Client: "go", ClientVersion: "v", Shape: "pass-through"})
	lines = l5Decode(t, &buf)
	if len(lines) != 3 || lines[1]["type"] != "ready" || lines[2]["message"] != "held" {
		t.Fatalf("after open = %#v", lines)
	}
}

func TestL5MessagesHaveTheContractFields(t *testing.T) {
	if l5Contract != 1 {
		t.Fatal("rows.py speaks contract 1")
	}
	reachable := false
	lastError := "timeout"
	failure := "partition 1 put: refused"
	serving, closed, usable, span := int64(30), int64(20), int64(40), int64(10)
	samples := []any{
		l5Ready{Type: "ready", T: 1, Contract: l5Contract, EpochMs: 2, Client: "go", ClientVersion: "(devel)", Shape: "pass-through"},
		l5Snapshot{
			Type: "snapshot", T: 1, Reason: "heartbeat",
			Nodes:   []l5NodeView{{Name: "n", Host: "db.l5.aerospike.internal", Port: 3000, Address: "10.73.0.138", Active: true, Reachable: &reachable}},
			Unowned: 1, Owners: map[string]int{"n": 4095}, LongestUnownedMs: 15,
			Succeeded: 2, Failed: 1, MaxLatencyMs: 4, LastError: &lastError,
		},
		l5LogLine{Type: "log", T: 1, Level: "WARN", Message: "a<b&c"},
		l5Served{Type: "served", T: 1, Step: "after", Partitions: l5Partitions, Written: 1, Read: 1, Mismatched: 0, Failures: 1, FirstFailure: &failure, ElapsedMs: 5},
		l5Reset{Type: "reset", T: 1},
		l5WatchReply{Type: "watch", T: 1, Address: "db:3000", Node: "n", ServingAt: &serving, LastClosedAt: &closed, UsableAt: &usable, UsableMs: &span, OwnershipChecked: true},
		l5Message{Type: "error", T: 1, Message: "unknown command: serv"},
	}
	required := map[string][]string{
		"ready":    {"contract", "epochMs", "client", "clientVersion", "shape"},
		"snapshot": {"reason", "nodes", "succeeded", "failed", "maxLatencyMs", "lastError", "owners", "longestUnownedMs"},
		"log":      {"level", "message"},
		"served":   {"step", "partitions", "written", "read", "mismatched", "failures", "firstFailure", "elapsedMs"},
		"reset":    {},
		"watch":    {"address", "node", "servingAt", "lastClosedAt", "usableAt", "usableMs", "ownershipChecked"},
		"error":    {"message"},
	}
	nodeFields := []string{"name", "host", "port", "address", "active", "reachable"}
	for _, sample := range samples {
		msg := l5DecodeOne(t, l5JSON(sample))
		kind, _ := msg["type"].(string)
		if _, ok := msg["t"].(float64); !ok {
			t.Fatalf("%s has no numeric t: %#v", kind, msg["t"])
		}
		for _, field := range required[kind] {
			if _, ok := msg[field]; !ok {
				t.Errorf("%s missing %s in %s", kind, field, l5JSON(sample))
			}
		}
		if kind == "log" && strings.Contains(string(l5JSON(sample)), `\u003c`) {
			t.Fatalf("log escaped '<': %s", l5JSON(sample))
		}
		if kind == "snapshot" {
			if _, ok := msg["clientErrors"]; ok {
				t.Fatal("snapshot includes clientErrors, which this driver does not count")
			}
			if _, ok := msg["clientTimeouts"]; ok {
				t.Fatal("snapshot includes clientTimeouts")
			}
			nodes, _ := msg["nodes"].([]any)
			if len(nodes) != 1 {
				t.Fatalf("nodes = %#v", msg["nodes"])
			}
			node, _ := nodes[0].(map[string]any)
			for _, field := range nodeFields {
				if _, ok := node[field]; !ok {
					t.Errorf("node missing %s", field)
				}
			}
			if node["reachable"] != false || node["port"] != float64(3000) || node["host"] != "db.l5.aerospike.internal" || node["address"] != "10.73.0.138" {
				t.Fatalf("node = %#v", node)
			}
		}
		if kind == "watch" {
			if _, ok := msg["addresses"]; ok {
				t.Fatal("watch reports every address; the harness dials the first one")
			}
		}
	}

	absent := l5DecodeOne(t, l5JSON(l5WatchReply{Type: "watch", T: 1, Address: "db:3000", Node: "n", OwnershipChecked: true}))
	if absent["servingAt"] != nil || absent["usableAt"] != nil || absent["usableMs"] != nil {
		t.Fatalf("unset watch times = %#v", absent)
	}
}

func TestL5ProbeDialsOnlyTheFirstAddress(t *testing.T) {
	ln := l5Listen(t)
	defer ln.Close()
	go l5Accept(ln)
	port := ln.Addr().(*net.TCPAddr).Port

	var lookedUp []string
	var dialed []string
	probe := newL5Probe("probe.l5.test", port, func(host string) ([]string, error) {
		lookedUp = append(lookedUp, host)
		// The second address does not answer. Dialing it, or dialing the name,
		// is the bug that made a watch look served before the client could connect.
		return []string{"127.0.0.1", "198.51.100.1"}, nil
	}, func(addr string, timeout time.Duration) error {
		dialed = append(dialed, addr)
		if timeout != l5ProbeTimeout {
			t.Errorf("timeout = %s", timeout)
		}
		return l5Dial(addr, timeout)
	})

	probe.attempt(20)
	if probe.servingAt == nil || *probe.servingAt != 20 || probe.lastClosedAt != nil {
		t.Fatalf("serving=%v lastClosed=%v", probe.servingAt, probe.lastClosedAt)
	}
	probe.attempt(30)
	if len(lookedUp) != 1 || lookedUp[0] != "probe.l5.test" || *probe.servingAt != 20 {
		t.Fatalf("lookup = %v serving = %v", lookedUp, probe.servingAt)
	}
	want := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if len(dialed) != 2 || dialed[0] != want || dialed[1] != want {
		t.Fatalf("dialed %v, want only %s", dialed, want)
	}
}

func TestL5ProbeClosedPortKeepsTheLastFailure(t *testing.T) {
	calls := 0
	probe := newL5Probe("probe.l5.test", 1, func(string) ([]string, error) {
		calls++
		return []string{"127.0.0.1", "198.51.100.1"}, nil
	}, l5Dial)
	probe.attempt(0)
	probe.attempt(10)
	if probe.servingAt != nil || probe.lastClosedAt == nil || *probe.lastClosedAt != 10 || calls != 1 {
		t.Fatalf("serving=%v lastClosed=%v lookups=%d", probe.servingAt, probe.lastClosedAt, calls)
	}
}

func TestL5WatchStopsDialingOnceItIsServing(t *testing.T) {
	calls := 0
	watch := newL5Watch("10.0.0.8:3000", "10.0.0.8", 3000, "n1")
	watch.probe.lookup = func(string) ([]string, error) {
		calls++
		return []string{"10.0.0.8", "198.51.100.1"}, nil
	}
	var dialed []string
	watch.probe.dial = func(addr string, timeout time.Duration) error {
		dialed = append(dialed, addr)
		if timeout != l5ProbeTimeout {
			t.Errorf("timeout = %s", timeout)
		}
		return nil
	}
	watch.probeIfDown(10)
	watch.probeIfDown(20)
	if calls != 1 || len(dialed) != 1 || dialed[0] != "10.0.0.8:3000" || watch.probe.servingAt == nil || *watch.probe.servingAt != 10 {
		t.Fatalf("calls=%d dialed=%v serving=%v", calls, dialed, watch.probe.servingAt)
	}
}

func TestL5ProbeRetriesAFailedLookup(t *testing.T) {
	calls := 0
	probe := newL5Probe("probe.l5.test", 3000, func(string) ([]string, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("temporary")
		}
		if calls == 2 {
			return nil, nil
		}
		return []string{"10.0.0.8", "10.0.0.9"}, nil
	}, func(addr string, timeout time.Duration) error {
		if addr != "10.0.0.8:3000" || timeout != l5ProbeTimeout {
			t.Fatalf("dial %s timeout %s", addr, timeout)
		}
		time.Sleep(5 * time.Millisecond)
		return nil
	})

	probe.attempt(5)
	probe.attempt(8)
	if probe.ip != "" || probe.servingAt != nil || probe.lastClosedAt == nil || *probe.lastClosedAt != 8 {
		t.Fatalf("after failed lookups: ip=%q serving=%v lastClosed=%v", probe.ip, probe.servingAt, probe.lastClosedAt)
	}
	probe.attempt(12)
	if probe.ip != "10.0.0.8" || probe.servingAt == nil || *probe.servingAt != 12 || *probe.lastClosedAt != 8 {
		t.Fatalf("after connect: ip=%q serving=%v lastClosed=%v", probe.ip, probe.servingAt, probe.lastClosedAt)
	}
	if calls != 3 {
		t.Fatalf("lookup calls = %d", calls)
	}
	probe.attempt(15)
	if calls != 3 || *probe.servingAt != 12 {
		t.Fatalf("lookup repeated or servingAt moved: calls=%d serving=%d", calls, *probe.servingAt)
	}
}

func TestL5ProbeConnectTimeoutIsBounded(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		done <- l5Dial("192.0.2.1:1", l5ProbeTimeout)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("dial to a documentation address connected")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dial ignored the 50ms timeout")
	}
}

func TestL5GapResetRestartsAnOpenStretch(t *testing.T) {
	gap := l5Gap{since: -1}
	if gap.observe(100, 0) != 0 || gap.observe(110, 3) != 0 || gap.observe(150, 3) != 40 {
		t.Fatal("open stretch was not counted from the poll that found it")
	}
	if gap.observe(180, 0) != 70 || gap.observe(200, 0) != 70 {
		t.Fatal("closed stretch was not kept")
	}
	gap.observe(210, 2)
	gap.reset(250)
	if gap.closed != 0 || gap.since != 250 {
		t.Fatalf("reset left closed=%d since=%d", gap.closed, gap.since)
	}
	if gap.observe(280, 2) != 30 || gap.observe(300, 0) != 50 {
		t.Fatal("stretch after reset still included time from before reset")
	}
	gap.reset(300)
	if gap.observe(310, 0) != 0 || gap.since != -1 {
		t.Fatalf("reset of a closed gap left since=%d", gap.since)
	}
}

func TestL5LatencyIgnoresAttemptsStartedBeforeReset(t *testing.T) {
	var latency l5Latency
	latency.record(0, 5*int64(time.Millisecond))
	latency.record(0, 2*int64(time.Millisecond))
	if latency.ms() != 5 {
		t.Fatalf("max = %d", latency.ms())
	}
	latency.reset(10 * int64(time.Millisecond))
	latency.record(9*int64(time.Millisecond), 100*int64(time.Millisecond))
	if latency.ms() != 0 {
		t.Fatalf("attempt started before reset counted: %d", latency.ms())
	}
	latency.record(10*int64(time.Millisecond), 14*int64(time.Millisecond))
	if latency.ms() != 4 {
		t.Fatalf("max after reset = %d", latency.ms())
	}
}

func TestL5PeerAddressKeepsTheLastIPv4(t *testing.T) {
	node := &as.Node{}
	routing := &l5Routing{peerIP: map[*as.Node]string{}}
	if got := routing.peerAddress(node, "db.l5.aerospike.internal"); got != "db.l5.aerospike.internal" {
		t.Fatalf("before a connection, address = %s", got)
	}
	routing.peerIP[node] = "10.73.0.138"
	if got := routing.peerAddress(node, "db.l5.aerospike.internal"); got != "10.73.0.138" {
		t.Fatalf("address = %s", got)
	}
}

func TestL5LoadKeyMatchesTheJavaFormula(t *testing.T) {
	if got := l5LoadKey(1, 2, 4, 1000); got != "load-9" {
		t.Fatalf("key = %s", got)
	}
	if got := l5LoadKey(3, 1, 4, 5); got != "load-2" {
		t.Fatalf("wrapped key = %s", got)
	}
}

func TestL5NotFoundIsAMissingRecord(t *testing.T) {
	if l5NotFound(nil) || l5NotFound(as.ErrTimeout) {
		t.Fatal("a timeout or a nil error was treated as a missing record")
	}
	if !l5NotFound(as.ErrKeyNotFound) || !as.ErrKeyNotFound.Matches(types.KEY_NOT_FOUND_ERROR) {
		t.Fatal("KEY_NOT_FOUND_ERROR was not treated as a missing record")
	}
}

func TestL5ServeKeysAreTheFirstCandidatePerPartition(t *testing.T) {
	keys := l5ServeKeys("test", "smoke", "after")
	if len(keys) != l5Partitions {
		t.Fatalf("%d keys", len(keys))
	}
	index := make([]int, l5Partitions)
	for partition, key := range keys {
		if key == nil {
			t.Fatalf("partition %d has no key", partition)
		}
		if key.PartitionId() != partition || key.Namespace() != "test" || key.SetName() != "smoke" {
			t.Fatalf("partition %d holds %s/%s in partition %d", partition, key.Namespace(), key.SetName(), key.PartitionId())
		}
		name := key.Value().String()
		if !strings.HasPrefix(name, "after-") {
			t.Fatalf("partition %d key %q", partition, name)
		}
		index[partition], _ = strconv.Atoi(strings.TrimPrefix(name, "after-"))
	}
	first := make(map[int]int, l5Partitions)
	for i := 0; len(first) < l5Partitions; i++ {
		candidate, _ := as.NewKey("test", "smoke", "after-"+strconv.Itoa(i))
		if _, ok := first[candidate.PartitionId()]; !ok {
			first[candidate.PartitionId()] = i
		}
	}
	for partition, got := range index {
		if got != first[partition] {
			t.Fatalf("partition %d took after-%d, but after-%d lands there first", partition, got, first[partition])
		}
	}
}

func TestL5ServeCountsEachOutcome(t *testing.T) {
	opts := l5MustOptions(t, "", nil)
	keys := l5ServeKeys(opts.namespace, opts.set, "after")
	pid := func(key *as.Key) int { return key.PartitionId() }
	store := &l5FakeStore{
		put: func(key *as.Key, value any) as.Error {
			if pid(key) == 0 || pid(key) == 7 {
				return as.ErrTimeout
			}
			return nil
		},
		get: func(key *as.Key, written any) (*as.Record, as.Error) {
			switch pid(key) {
			case 1:
				return nil, as.ErrKeyNotFound
			case 2:
				return &as.Record{Bins: as.BinMap{"v": "after-9999"}}, nil
			case 3:
				return nil, as.ErrNetTimeout
			case 4:
				return &as.Record{Bins: as.BinMap{"v": 4}}, nil
			case 5:
				return nil, nil
			}
			return &as.Record{Bins: as.BinMap{"v": written}}, nil
		},
	}

	served := l5Serve(store, opts, "after")
	if served.Type != "served" || served.Step != "after" || served.Partitions != l5Partitions || served.ElapsedMs < 0 {
		t.Fatalf("served = %+v", served)
	}
	if served.Written != l5Partitions-2 || served.Read != l5Partitions-2-5 || served.Mismatched != 4 || served.Failures != 3 {
		t.Fatalf("written=%d read=%d mismatched=%d failures=%d", served.Written, served.Read, served.Mismatched, served.Failures)
	}
	if served.FirstFailure == nil || !strings.HasPrefix(*served.FirstFailure, "partition 0 put: ") {
		t.Fatalf("firstFailure = %v", served.FirstFailure)
	}

	if len(store.puts) != l5Partitions {
		t.Fatalf("%d puts, want one attempt per partition", len(store.puts))
	}
	for i, call := range store.puts {
		if !bytes.Equal(call.key.Digest(), keys[i].Digest()) || call.value != "after-"+strconv.Itoa(i) || call.policy != opts.write {
			t.Fatalf("put %d = %+v", i, call)
		}
	}
	if len(store.gets) != l5Partitions-2 {
		t.Fatalf("%d gets, want none after a failed put", len(store.gets))
	}
	for _, call := range store.gets {
		if pid(call.key) == 0 || pid(call.key) == 7 || call.policy != opts.read {
			t.Fatalf("get %+v", call)
		}
	}
}

func TestL5LoadRetriesTheSameCommandUntilItSucceeds(t *testing.T) {
	opts := l5MustOptions(t, "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 2 --timeout 1000 --namespace test --set smoke --workers 1 --keys 1000", nil)
	puts := 0
	store := &l5FakeStore{
		put: func(*as.Key, any) as.Error {
			if puts++; puts <= 2 {
				return as.ErrTimeout
			}
			return nil
		},
		get: func(*as.Key, any) (*as.Record, as.Error) { return nil, as.ErrKeyNotFound },
	}
	ready := 0
	load := newL5Load(store, newL5Clock(), opts, func() { ready++ })
	load.start()
	deadline := time.Now().Add(5 * time.Second)
	for load.succeeded.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	load.stop(time.Now().Add(time.Second))
	<-load.done

	if load.failed.Load() != 2 || load.succeeded.Load() < 2 || ready != 1 {
		t.Fatalf("failed=%d succeeded=%d ready=%d", load.failed.Load(), load.succeeded.Load(), ready)
	}
	if len(store.puts) < 3 || len(store.gets) < 1 {
		t.Fatalf("puts=%d gets=%d", len(store.puts), len(store.gets))
	}
	for _, call := range store.puts[:3] {
		if call.key.Value().String() != "load-0" || call.value != int64(0) || call.policy != opts.write {
			t.Fatalf("retry changed the command: %+v", call)
		}
	}
	if store.gets[0].key.Value().String() != "load-1" || store.gets[0].policy != opts.read {
		t.Fatalf("get = %+v", store.gets[0])
	}
	if load.latency.max < int64(2*opts.tend) {
		t.Fatalf("latency %s did not count from the first attempt", time.Duration(load.latency.max))
	}
}

func TestL5LoadReportsFailuresAndReadyOnce(t *testing.T) {
	opts := l5MustOptions(t, "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 1 --timeout 1000 --namespace test --set smoke --workers 2 --keys 10", nil)
	var mu sync.Mutex
	calls := 0
	store := &l5FakeStore{
		put: func(*as.Key, any) as.Error {
			mu.Lock()
			defer mu.Unlock()
			if calls++; calls == 1 {
				return as.ErrTimeout
			}
			return nil
		},
		get: func(*as.Key, any) (*as.Record, as.Error) { return nil, nil },
	}
	readyCh := make(chan struct{}, 4)
	load := newL5Load(store, newL5Clock(), opts, func() { readyCh <- struct{}{} })
	load.start()
	deadline := time.Now().Add(5 * time.Second)
	for load.succeeded.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	load.stop(time.Now().Add(time.Second))
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("stop took %s", waited)
	}
	select {
	case <-load.done:
	default:
		t.Fatal("workers still running after stop")
	}

	if load.succeeded.Load() < 20 || load.failed.Load() != 1 {
		t.Fatalf("succeeded=%d failed=%d", load.succeeded.Load(), load.failed.Load())
	}
	if last := load.lastError.Load(); last == nil || *last != as.ErrTimeout.Error() {
		t.Fatalf("lastError = %v", last)
	}
	if len(readyCh) != 1 {
		t.Fatalf("ready sent %d times", len(readyCh))
	}
	for _, call := range store.gets {
		if call.key.Value().String() == "" || call.policy != opts.read {
			t.Fatalf("get = %+v", call)
		}
	}
}

func TestL5LoadStopInterruptsTheRetrySleep(t *testing.T) {
	opts := l5MustOptions(t, "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 3600000 --timeout 1000 --namespace test --set smoke --workers 1", nil)
	store := &l5FakeStore{put: func(*as.Key, any) as.Error { return as.ErrTimeout }}
	load := newL5Load(store, newL5Clock(), opts, func() { t.Error("ready without a success") })
	load.start()
	for load.failed.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	load.stop(time.Now().Add(5 * time.Second))
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("stop waited %s on a one-hour retry sleep", waited)
	}
	if load.succeeded.Load() != 0 || load.failed.Load() != 1 {
		t.Fatalf("succeeded=%d failed=%d", load.succeeded.Load(), load.failed.Load())
	}
}

func TestL5CommandsRejectMalformedInput(t *testing.T) {
	driver, buf := l5CommandDriver()
	driver.routing.gap = l5Gap{since: 10, closed: 50}
	driver.load.latency.max = 99

	if !driver.command("watch") || !driver.command("watch 127.0.0.1:1") || !driver.command("watch not-a-host node") {
		t.Fatal("a bad watch stopped the command loop")
	}
	if driver.watch.Load() != nil {
		t.Fatal("a bad watch was started")
	}
	if !driver.command("serve") || !driver.command("reset now") || !driver.command("quit extra") || !driver.command("nope") {
		t.Fatal("a bad command stopped the command loop")
	}
	if driver.routing.gap.closed != 50 || driver.load.latency.max != 99 {
		t.Fatal("a rejected reset changed the counters")
	}

	first := newL5Watch("127.0.0.1:1", "127.0.0.1", 1, "n1")
	driver.watch.Store(first)
	if !driver.command("watch 127.0.0.1:2 n2") || driver.watch.Load() != first {
		t.Fatal("a second watch replaced the first")
	}
	if driver.command("quit") {
		t.Fatal("quit did not stop the command loop")
	}

	lines := l5Decode(t, buf)
	var messages []string
	for _, line := range lines {
		if line["type"] != "error" {
			t.Fatalf("command wrote %v", line)
		}
		messages = append(messages, line["message"].(string))
	}
	want := []string{
		"malformed command: watch",
		"malformed command: watch 127.0.0.1:1",
		"malformed command: watch not-a-host node",
		"malformed command: serve",
		"malformed command: reset now",
		"malformed command: quit extra",
		"unknown command: nope",
		"a watch is already running: watch 127.0.0.1:2 n2",
	}
	if strings.Join(messages, "\n") != strings.Join(want, "\n") {
		t.Fatalf("messages =\n%s", strings.Join(messages, "\n"))
	}
}

func TestL5ResetClearsTheGapAndTheLatency(t *testing.T) {
	driver, buf := l5CommandDriver()
	driver.routing.gap = l5Gap{since: 10, closed: 50}
	driver.load.latency.max = int64(time.Second)
	if !driver.command("reset") {
		t.Fatal("reset stopped the command loop")
	}
	if driver.routing.gap.closed != 0 || driver.routing.gap.since < 0 || driver.load.latency.ms() != 0 {
		t.Fatalf("gap closed=%d since=%d latency=%d", driver.routing.gap.closed, driver.routing.gap.since, driver.load.latency.ms())
	}
	lines := l5Decode(t, buf)
	if len(lines) != 1 || lines[0]["type"] != "reset" {
		t.Fatalf("lines = %#v", lines)
	}
}

func TestL5RoutingSendsHeartbeatAndChange(t *testing.T) {
	routing := &l5Routing{}
	change, heartbeat := routing.note("a", 100)
	if change || !heartbeat || routing.next != 200 {
		t.Fatalf("first poll change=%t heartbeat=%t next=%d", change, heartbeat, routing.next)
	}
	change, heartbeat = routing.note("a", 150)
	if change || heartbeat {
		t.Fatal("an unchanged poll inside the heartbeat window was sent")
	}
	change, heartbeat = routing.note("b", 160)
	if !change || heartbeat {
		t.Fatalf("change=%t heartbeat=%t", change, heartbeat)
	}
	change, heartbeat = routing.note("c", 200)
	if !change || !heartbeat || routing.next != 300 {
		t.Fatalf("both change=%t heartbeat=%t next=%d", change, heartbeat, routing.next)
	}
}

func TestL5LoggerKeepsDebugOffStdout(t *testing.T) {
	defer func() {
		logger.Logger.SetLevel(logger.OFF)
		logger.Logger.SetLogger(log.New(os.Stdout, "", log.LstdFlags))
	}()

	var stdout bytes.Buffer
	out := &l5Output{stdout: &stdout}
	out.open(l5Ready{Type: "ready", T: 0, Contract: l5Contract, Client: "go", ClientVersion: "v", Shape: "pass-through"})
	installL5Logger(newL5Clock(), out, false)
	logger.Logger.Debug("hidden debug %s", "x")
	logger.Logger.Info("Add node BB9 10.0.0.1 3000")
	logger.Logger.Warn("refresh failed: %s", "timeout")
	logger.Logger.Error("Invalid peer %s", "a.example")

	lines := l5Decode(t, &stdout)
	if len(lines) != 3 || lines[1]["level"] != "WARN" || lines[2]["level"] != "WARN" {
		t.Fatalf("lines = %#v", lines)
	}
	if lines[1]["message"] != "refresh failed: timeout" || lines[2]["message"] != "Invalid peer a.example" {
		t.Fatalf("messages = %#v %#v", lines[1]["message"], lines[2]["message"])
	}

	stdout.Reset()
	stderr, restore := l5CaptureStderr(t)
	defer restore()
	installL5Logger(l5Clock{start: time.Now().Add(-15 * time.Millisecond)}, out, true)
	logger.Logger.Info("Move node BB9 a.example from 10.0.0.1 3000 to 10.0.0.2 3000")
	restore()
	if stdout.Len() != 0 {
		t.Fatalf("debug mode wrote stdout: %s", stdout.String())
	}
	text := strings.TrimSpace(stderr.String())
	stamp, message, ok := strings.Cut(strings.TrimPrefix(text, "t="), " ")
	ms, err := strconv.Atoi(stamp)
	if !strings.HasPrefix(text, "t=") || !ok || err != nil || ms < 15 || ms > 5000 {
		t.Fatalf("stderr = %q, want t=<driver ms> from the driver's clock", text)
	}
	if message != "Move node BB9 a.example from 10.0.0.1 3000 to 10.0.0.2 3000" || strings.Count(text, "\n") != 0 {
		t.Fatalf("stderr message = %q", message)
	}
}

type l5StoreCall struct {
	policy any
	key    *as.Key
	value  any
}

// l5FakeStore records each call. get receives the value last put on that key.
type l5FakeStore struct {
	mu     sync.Mutex
	put    func(key *as.Key, value any) as.Error
	get    func(key *as.Key, written any) (*as.Record, as.Error)
	puts   []l5StoreCall
	gets   []l5StoreCall
	values map[string]any
}

func (s *l5FakeStore) PutBins(policy *as.WritePolicy, key *as.Key, bins ...*as.Bin) as.Error {
	value := bins[0].Value.GetObject()
	s.mu.Lock()
	s.puts = append(s.puts, l5StoreCall{policy: policy, key: key, value: value})
	s.mu.Unlock()
	err := s.put(key, value)
	if err == nil {
		s.mu.Lock()
		if s.values == nil {
			s.values = map[string]any{}
		}
		s.values[key.String()] = value
		s.mu.Unlock()
	}
	return err
}

func (s *l5FakeStore) Get(policy *as.BasePolicy, key *as.Key, binNames ...string) (*as.Record, as.Error) {
	s.mu.Lock()
	s.gets = append(s.gets, l5StoreCall{policy: policy, key: key})
	written := s.values[key.String()]
	s.mu.Unlock()
	return s.get(key, written)
}

func l5BaseArgs() string {
	return "--seed 127.0.0.1:3000 --shape pass-through --tend-interval 250 --timeout 1000 --namespace test --set smoke"
}

func l5MustOptions(t *testing.T, extra string, getenv func(string) (string, bool)) *l5Options {
	t.Helper()
	if getenv == nil {
		getenv = func(string) (string, bool) { return "", false }
	}
	if extra == "" {
		extra = l5BaseArgs()
	}
	opts, err := parseL5Options(strings.Fields(extra), getenv)
	if err != nil {
		t.Fatal(err)
	}
	return opts
}

func l5CommandDriver() (*l5Driver, *bytes.Buffer) {
	var buf bytes.Buffer
	out := &l5Output{stdout: &buf}
	out.open(l5Ready{Type: "ready", T: 0, Contract: l5Contract, Client: "go", ClientVersion: "v", Shape: "pass-through"})
	buf.Reset()
	return &l5Driver{
		clock:   newL5Clock(),
		out:     out,
		load:    &l5Load{},
		routing: &l5Routing{gap: l5Gap{since: -1}},
	}, &buf
}

func l5Listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func l5Accept(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}
}

func l5CaptureStderr(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	old := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, reader)
		close(done)
	}()
	var once sync.Once
	return &buf, func() {
		once.Do(func() {
			os.Stderr = old
			writer.Close()
			<-done
			reader.Close()
		})
	}
}

func l5Decode(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for scanner.Scan() {
		lines = append(lines, l5DecodeOne(t, scanner.Bytes()))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func l5DecodeOne(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		t.Fatalf("decode %s: %v", line, err)
	}
	return msg
}

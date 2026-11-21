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

package main

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// requiredFlags is a valid set of the required flags plus extra. A flag in
// extra that is already required replaces the required value.
func requiredFlags(extra ...string) []string {
	values := map[string]string{
		"--seed": "db.example.com:3000", "--tend-interval": "250", "--timeout": "1000",
		"--namespace": "test", "--set": "l5",
	}
	var args []string
	for index := 0; index+1 < len(extra); index += 2 {
		if _, required := values[extra[index]]; required {
			values[extra[index]] = extra[index+1]
			continue
		}
		args = append(args, extra[index], extra[index+1])
	}
	if len(extra)%2 == 1 {
		args = append(args, extra[len(extra)-1])
	}
	for _, flag := range []string{"--seed", "--tend-interval", "--timeout", "--namespace", "--set"} {
		args = append([]string{flag, values[flag]}, args...)
	}
	return args
}

func TestFlagsDefaultToTheContractValues(t *testing.T) {
	options, err := parseDriverOptions(requiredFlags("--shape", "pass-through"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if options.workers != 4 || options.keys != 1000 || options.onMiss != as.TranslationMissStrict {
		t.Fatalf("defaults = workers %d keys %d on-miss %v, want 4, 1000, strict", options.workers, options.keys, options.onMiss)
	}
	if options.seedHost != "db.example.com" || options.seedPort != 3000 {
		t.Fatalf("seed = %s:%d", options.seedHost, options.seedPort)
	}
	if options.tendInterval != 250*time.Millisecond || options.timeout != time.Second {
		t.Fatalf("tend interval %v, timeout %v", options.tendInterval, options.timeout)
	}
}

func TestBadFlagsAreRejectedWithTheReason(t *testing.T) {
	cases := map[string]struct {
		args   []string
		reason string
	}{
		"missing namespace":          {[]string{"--seed", "h:3000", "--shape", "pass-through", "--tend-interval", "250", "--timeout", "1000", "--set", "s"}, "--namespace is required"},
		"unknown shape":              {requiredFlags("--shape", "static-map"), "--shape must be pass-through or port-map"},
		"port-map shape without map": {requiredFlags("--shape", "port-map"), "--port-map is required with --shape port-map"},
		"map with pass-through":      {requiredFlags("--shape", "pass-through", "--port-map", "BB9=h:3001"), "--port-map only with --shape port-map"},
		"map entry without target":   {requiredFlags("--shape", "port-map", "--port-map", "BB9"), "--port-map entry must be NODE=HOST:PORT"},
		"map key given twice":        {requiredFlags("--shape", "port-map", "--port-map", "BB9=h:3001,BB9=h:3002"), "--port-map names BB9 twice"},
		"user without password":      {requiredFlags("--shape", "pass-through", "--user", "admin"), "--user needs AEROSPIKE_ADMIN_PASSWORD"},
		"ca file without tls name":   {requiredFlags("--shape", "pass-through", "--tls-ca-file", "/ca.pem"), "--tls-ca-file only with --tls-name"},
		"zero workers":               {requiredFlags("--shape", "pass-through", "--workers", "0"), "--workers must be a positive integer"},
		"seed without port":          {requiredFlags("--shape", "pass-through", "--seed", "db.example.com"), "--seed must be HOST:PORT"},
		"seed port out of range":     {requiredFlags("--shape", "pass-through", "--seed", "db.example.com:70000"), "--seed must be HOST:PORT"},
		"flag given twice":           {append(requiredFlags("--shape", "pass-through"), "--keys", "5", "--keys", "6"), "--keys given twice"},
		"unknown flag":               {requiredFlags("--shape", "pass-through", "--retries", "3"), "unknown flag: --retries"},
		"flag without value":         {append(requiredFlags("--shape", "pass-through"), "--keys"), "--keys needs a value"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseDriverOptions(testCase.args, "", false)
			if err == nil || !strings.Contains(err.Error(), testCase.reason) {
				t.Fatalf("error = %v, want it to contain %q", err, testCase.reason)
			}
		})
	}
}

func TestPassThroughShapeUsesTheAlternateListAndTranslatesNothing(t *testing.T) {
	options, err := parseDriverOptions(requiredFlags("--shape", "pass-through", "--on-miss", "pass-through"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	policy := options.clientPolicy()
	if !policy.UseServicesAlternate || policy.TranslationOnMiss != as.TranslationMissPassThrough {
		t.Fatalf("UseServicesAlternate %v, TranslationOnMiss %v", policy.UseServicesAlternate, policy.TranslationOnMiss)
	}
	host, ok := policy.AddressTranslator.Translate(as.Endpoint{NodeName: "BB9", Host: "node-1.pass-through.test", Port: 3101})
	if !ok || host.Name != "node-1.pass-through.test" || host.Port != 3101 {
		t.Fatalf("translated to %v %v, want the advertised address", host, ok)
	}
}

func TestPortMapShapeMapsByNodeNameAndByAdvertisedAddress(t *testing.T) {
	options, err := parseDriverOptions(requiredFlags("--shape", "port-map",
		"--port-map", "BB9=lb.example.com:4001,10.0.0.7:3000=lb.example.com:4002"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	policy := options.clientPolicy()
	if policy.UseServicesAlternate || policy.TranslationOnMiss != as.TranslationMissStrict {
		t.Fatalf("UseServicesAlternate %v, TranslationOnMiss %v", policy.UseServicesAlternate, policy.TranslationOnMiss)
	}

	byName, ok := policy.AddressTranslator.Translate(as.Endpoint{NodeName: "BB9", Host: "10.0.0.5", Port: 3000})
	if !ok || byName.Name != "lb.example.com" || byName.Port != 4001 {
		t.Fatalf("by node name = %v %v", byName, ok)
	}
	byAddress, ok := policy.AddressTranslator.Translate(as.Endpoint{NodeName: "CC1", Host: "10.0.0.7", Port: 3000})
	if !ok || byAddress.Name != "lb.example.com" || byAddress.Port != 4002 {
		t.Fatalf("by advertised address = %v %v", byAddress, ok)
	}
	if _, ok := policy.AddressTranslator.Translate(as.Endpoint{NodeName: "DD2", Host: "10.0.0.8", Port: 3000}); ok {
		t.Fatal("an unmapped node translated")
	}
}

func TestCommandsMakeOneAttemptBoundedByTheTimeout(t *testing.T) {
	options, err := parseDriverOptions(requiredFlags("--shape", "pass-through", "--timeout", "750"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	read, write := options.readPolicy(), options.writePolicy()
	for name, policy := range map[string]*as.BasePolicy{"read": read, "write": &write.BasePolicy} {
		if policy.MaxRetries != 0 || policy.SocketTimeout != 750*time.Millisecond || policy.TotalTimeout != 750*time.Millisecond {
			t.Fatalf("%s policy: retries %d, socket %v, total %v", name, policy.MaxRetries, policy.SocketTimeout, policy.TotalTimeout)
		}
	}
}

func TestTLSNameGoesOnTheSeedAndABadCAFileIsABadFlag(t *testing.T) {
	options, err := parseDriverOptions(requiredFlags("--shape", "pass-through", "--tls-name", "aerospike-tls"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if options.seed().TLSName != "aerospike-tls" || options.clientPolicy().TlsConfig == nil {
		t.Fatalf("seed TLS name %q, TLS config %v", options.seed().TLSName, options.clientPolicy().TlsConfig)
	}

	notACertificate := t.TempDir() + "/ca.pem"
	if err := os.WriteFile(notACertificate, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for caFile, reason := range map[string]string{
		notACertificate:                 "--tls-ca-file has no certificate",
		t.TempDir() + "/missing-ca.pem": "--tls-ca-file: open",
	} {
		_, err := parseDriverOptions(requiredFlags("--shape", "pass-through", "--tls-name", "aerospike-tls", "--tls-ca-file", caFile), "", false)
		if err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("error = %v, want it to contain %q", err, reason)
		}
	}
}

func TestAPortMapTheTranslatorRejectsIsABadFlag(t *testing.T) {
	_, err := parseDriverOptions(requiredFlags("--shape", "port-map", "--port-map", "10.0.0.7:3000=lb.example.com:4001,10.0.0.7:03000=lb.example.com:4002"), "", false)
	if err == nil || !strings.HasPrefix(err.Error(), "--port-map: ") || !strings.Contains(err.Error(), "duplicates another key") {
		t.Fatalf("error = %v, want the translator's reason for two keys naming one advertised address", err)
	}
}

func TestLoadCyclesEveryKeyAcrossWorkers(t *testing.T) {
	const workers, keys = 4, 10
	seen := map[int]int{}
	for iteration := 0; iteration < keys; iteration++ {
		for worker := 0; worker < workers; worker++ {
			seen[loadKeyIndex(worker, iteration, workers, keys)]++
		}
	}
	if len(seen) != keys {
		t.Fatalf("load touched %d of %d keys", len(seen), keys)
	}
	if got := loadKeyIndex(1, 3, workers, keys); got != (1+3*workers)%keys {
		t.Fatalf("worker 1 iteration 3 uses key %d", got)
	}
}

func TestMaxLatencyCountsOnlyCommandsStartedSinceTheLastReset(t *testing.T) {
	var stats loadStats
	stats.recordSuccess(10*time.Millisecond, 500*time.Millisecond)
	stats.reset(600 * time.Millisecond)
	stats.recordSuccess(599*time.Millisecond, 900*time.Millisecond)
	stats.recordSuccess(620*time.Millisecond, 700*time.Millisecond)

	object := line{}
	stats.addTo(object)
	if object["maxLatencyMs"] != int64(80) || object["succeeded"] != int64(3) {
		t.Fatalf("maxLatencyMs %v, succeeded %v; want 80 and 3", object["maxLatencyMs"], object["succeeded"])
	}
}

func TestKeysByPartitionKeepsTheFirstKeyOfEveryPartition(t *testing.T) {
	keys, err := keysByPartition("test", "l5", "after")
	if err != nil {
		t.Fatal(err)
	}
	firstIndex := map[int]int{}
	for index := 0; len(firstIndex) < partitionCount; index++ {
		key, _ := as.NewKey("test", "l5", "after-"+strconv.Itoa(index))
		if _, known := firstIndex[key.PartitionId()]; !known {
			firstIndex[key.PartitionId()] = index
		}
	}
	for partition, key := range keys {
		if key.PartitionId() != partition {
			t.Fatalf("partition %d holds a key of partition %d", partition, key.PartitionId())
		}
		if want := "after-" + strconv.Itoa(firstIndex[partition]); key.Value().String() != want {
			t.Fatalf("partition %d holds %s, want the first key %s", partition, key.Value(), want)
		}
	}
}

// scriptedStore answers puts and gets by key name.
type scriptedStore struct {
	gets    map[string]int
	putErr  map[string]as.Error
	getErr  map[string]as.Error
	stored  map[string]string
	missing map[string]bool
	changed map[string]string
}

func (store *scriptedStore) put(key *as.Key, value string) as.Error {
	name := key.Value().String()
	if err := store.putErr[name]; err != nil {
		return err
	}
	store.stored[name] = value
	return nil
}

func (store *scriptedStore) get(key *as.Key) (any, bool, as.Error) {
	name := key.Value().String()
	store.gets[name]++
	if err := store.getErr[name]; err != nil {
		return nil, false, err
	}
	if store.missing[name] {
		return nil, false, nil
	}
	if value, changed := store.changed[name]; changed {
		return value, true, nil
	}
	return store.stored[name], true, nil
}

func newScriptedStore() *scriptedStore {
	return &scriptedStore{gets: map[string]int{}, putErr: map[string]as.Error{}, getErr: map[string]as.Error{}, stored: map[string]string{},
		missing: map[string]bool{}, changed: map[string]string{}}
}

// steppingClock advances by step on every read.
func steppingClock(step time.Duration) func() time.Duration {
	var clock time.Duration
	return func() time.Duration {
		clock += step
		return clock
	}
}

func TestServeCountsReadsMismatchesAndFailuresPerPartition(t *testing.T) {
	keys, err := keysByPartition("test", "l5", "after")
	if err != nil {
		t.Fatal(err)
	}
	store := newScriptedStore()
	store.missing[keys[1].Value().String()] = true
	store.changed[keys[2].Value().String()] = "other"
	store.putErr[keys[3].Value().String()] = scriptedFailure()
	store.getErr[keys[4].Value().String()] = scriptedFailure()

	reply, err := serve(store, "test", "l5", "after", steppingClock(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	want := line{"step": "after", "partitions": partitionCount, "written": partitionCount - 1, "read": partitionCount - 4,
		"mismatched": 2, "failures": 2, "elapsedMs": int64(5)}
	for field, value := range want {
		if reply[field] != value {
			t.Fatalf("%s = %v, want %v (reply %v)", field, reply[field], value, reply)
		}
	}
	if failure, _ := reply["firstFailure"].(string); !strings.HasPrefix(failure, "partition 3 put: ") {
		t.Fatalf("firstFailure %q, want the put of partition 3", failure)
	}
	if store.gets[keys[3].Value().String()] != 0 {
		t.Fatal("read back a key whose put failed")
	}
	if got := store.stored[keys[7].Value().String()]; got != "after-7" {
		t.Fatalf("partition 7 stored %q, want after-7", got)
	}
}

func TestLargeWritesValuesOfExactlyTheRequestedSize(t *testing.T) {
	if _, _, _, ok := parseLarge([]string{"large", "mtu", "4", "8"}); ok {
		t.Fatal("accepted 4 bytes, shorter than the key name mtu-7")
	}
	step, size, keys, ok := parseLarge([]string{"large", "mtu", "65536", "3"})
	if !ok {
		t.Fatal("rejected a valid large command")
	}
	store := newScriptedStore()
	reply, err := writeReadLarge(store, "test", "l5", step, size, keys, steppingClock(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if reply["written"] != 3 || reply["read"] != 3 || reply["failures"] != 0 || reply["maxMs"] != int64(1) {
		t.Fatalf("reply %v", reply)
	}
	value := store.stored["mtu-2"]
	if len(value) != 65536 || !strings.HasPrefix(value, "mtu-2") {
		t.Fatalf("value of %d characters starting %q", len(value), value[:8])
	}
}

func TestWatchServesAfterThreeRoundsWithEveryAddressConnected(t *testing.T) {
	probe := newWatchProbe()
	addresses := []string{"10.0.0.1", "10.0.0.2"}
	rounds := []struct {
		start     int64
		connected []bool
	}{
		{100, []bool{true, false}},
		{110, []bool{true, true}},
		{120, []bool{true, true}},
		{130, []bool{false, true}},
		{140, []bool{true, true}},
		{150, []bool{true, true}},
	}
	for _, round := range rounds {
		if probe.recordRound(round.start, addresses, round.connected) {
			t.Fatalf("serving after round %d", round.start)
		}
	}
	if !probe.recordRound(160, addresses, []bool{true, true}) {
		t.Fatal("not serving after three connected rounds")
	}
	if *probe.servingAt != 140 || *probe.lastClosedAt != 130 {
		t.Fatalf("servingAt %d lastClosedAt %d, want 140 and 130", *probe.servingAt, *probe.lastClosedAt)
	}
	got := probe.addresses()
	if got[0]["address"] != "10.0.0.1" || got[0]["firstConnectedAt"] != int64(100) || got[1]["firstConnectedAt"] != int64(110) {
		t.Fatalf("addresses %v", got)
	}
}

func TestWatchNeverServesWhileTheNameResolvesToNothing(t *testing.T) {
	probe := newWatchProbe()
	for round := int64(0); round < 5; round++ {
		if probe.recordRound(round*10, nil, nil) {
			t.Fatal("serving with no address")
		}
	}
	if probe.servingAt != nil || *probe.lastClosedAt != 40 {
		t.Fatalf("servingAt %v lastClosedAt %v", probe.servingAt, *probe.lastClosedAt)
	}
}

func TestOutputWritesReadyFirstAndDropsFailuresBeforeIt(t *testing.T) {
	var stdout, stderr bytes.Buffer
	output := newDriverOutput(&stdout, &stderr)

	output.write("log", func(int64) line { return line{"level": "WARN", "message": "before ready"} })
	output.writeIfOpen("failure", func(int64) line { return line{"startedAt": int64(0), "message": "dropped"} })
	output.openWith(readyLine("port-map"))
	output.write("snapshot", func(int64) line { return line{"reason": "heartbeat"} })
	output.writeIfOpen("failure", func(int64) line { return line{"startedAt": int64(1), "message": "kept"} })

	if !strings.Contains(stderr.String(), "before ready") || strings.Contains(stderr.String()+stdout.String(), "dropped") {
		t.Fatalf("stderr %q stdout %q", stderr.String(), stdout.String())
	}
	var kinds []string
	previousT := int64(-1)
	for _, encoded := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		var object map[string]any
		if err := json.Unmarshal([]byte(encoded), &object); err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, object["type"].(string))
		lineT := int64(object["t"].(float64))
		if lineT < previousT {
			t.Fatalf("t went back to %d after %d", lineT, previousT)
		}
		previousT = lineT
	}
	if strings.Join(kinds, ",") != "ready,snapshot,failure" {
		t.Fatalf("stdout lines %v", kinds)
	}
	var ready map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(stdout.String(), "\n", 2)[0]), &ready)
	if ready["contract"] != float64(5) || ready["client"] != "go" || ready["shape"] != "port-map" {
		t.Fatalf("ready %v", ready)
	}
}

// scriptedFailure is a client error to script a failed put or get.
func scriptedFailure() as.Error {
	_, err := as.NewPortMapAddressTranslator(nil)
	if err == nil {
		panic("NewPortMapAddressTranslator accepted a nil map")
	}
	return err
}

func TestLongestUnownedIncludesAnOpenStretchAndRestartsOnReset(t *testing.T) {
	var monitor routingMonitor
	steps := []struct {
		t       int64
		unowned int
		longest int64
	}{
		{100, 0, 0},
		{110, 5, 0},
		{150, 5, 40},
		{170, 0, 60},
		{200, 3, 60},
		{230, 3, 60},
	}
	for _, step := range steps {
		if got := monitor.trackUnowned(step.t, step.unowned); got != step.longest {
			t.Fatalf("at %d longest = %d, want %d", step.t, got, step.longest)
		}
	}

	monitor.reset(240)
	if got := monitor.trackUnowned(250, 3); got != 10 {
		t.Fatalf("after reset at 240 with a stretch open, longest at 250 = %d, want 10", got)
	}
	if got := monitor.trackUnowned(260, 0); got != 20 {
		t.Fatalf("stretch closed at 260, longest = %d, want 20", got)
	}
}

func TestRoutingKeyChangesWithEveryRoutingField(t *testing.T) {
	base := []nodeView{{name: "A1", host: "10.0.0.1", port: 3000, active: true}}
	changes := map[string]func([]nodeView) ([]nodeView, int){
		"host":    func(views []nodeView) ([]nodeView, int) { views[0].host = "10.0.0.2"; return views, 0 },
		"port":    func(views []nodeView) ([]nodeView, int) { views[0].port = 3001; return views, 0 },
		"active":  func(views []nodeView) ([]nodeView, int) { views[0].active = false; return views, 0 },
		"unowned": func(views []nodeView) ([]nodeView, int) { return views, 1 },
		"added": func(views []nodeView) ([]nodeView, int) {
			return append(views, nodeView{name: "B1", host: "10.0.0.2", port: 3000, active: true}), 0
		},
		"removed": func([]nodeView) ([]nodeView, int) { return nil, 0 },
	}
	baseKey := routingKey(base, 0)
	for name, change := range changes {
		views, unowned := change(append([]nodeView(nil), base...))
		if routingKey(views, unowned) == baseKey {
			t.Fatalf("changing %s left the routing key unchanged", name)
		}
	}
	if routingKey(append([]nodeView(nil), base...), 0) != baseKey {
		t.Fatal("the same routing gave another key")
	}
}

func TestProbeRoundConnectsOnlyToAListeningPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listening := listener.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	addresses, connected := probeRound("127.0.0.1", listening)
	if len(addresses) != 1 || addresses[0] != "127.0.0.1" || !connected[0] {
		t.Fatalf("listening port: addresses %v connected %v", addresses, connected)
	}
	listener.Close()
	if _, connected := probeRound("127.0.0.1", listening); connected[0] {
		t.Fatal("connected to a closed port")
	}
}

// recordingRunner is a command runner whose output is captured.
func recordingRunner(t *testing.T) (*commandRunner, *bytes.Buffer) {
	t.Helper()
	var stdout bytes.Buffer
	output := newDriverOutput(&stdout, &bytes.Buffer{})
	output.openWith(readyLine("port-map"))
	stdout.Reset()
	options, err := parseDriverOptions(requiredFlags("--shape", "pass-through"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	return &commandRunner{options: options, output: output, stats: &loadStats{}, monitor: &routingMonitor{}}, &stdout
}

func outputLines(t *testing.T, stdout *bytes.Buffer) []map[string]any {
	t.Helper()
	var objects []map[string]any
	for _, encoded := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		var object map[string]any
		if err := json.Unmarshal([]byte(encoded), &object); err != nil {
			t.Fatalf("line %q: %v", encoded, err)
		}
		objects = append(objects, object)
	}
	return objects
}

func TestMalformedAndUnknownCommandsGetAnErrorAndAreIgnored(t *testing.T) {
	runner, stdout := recordingRunner(t)
	runner.watchDone = make(chan struct{})
	commands := map[string]string{
		"":                           "malformed command: ",
		"serv x":                     "unknown command: serv x",
		"serve":                      "malformed command: serve",
		"quit now":                   "malformed command: quit now",
		"reset all":                  "malformed command: reset all",
		"large mtu 4 8":              "malformed command: large mtu 4 8",
		"watch 127.0.0.1 A1":         "malformed command: watch 127.0.0.1 A1",
		"watch 127.0.0.1:0 A1":       "malformed command: watch 127.0.0.1:0 A1",
		"watch 127.0.0.1:3000 A1":    "a watch is already running: watch 127.0.0.1:3000 A1",
		"watch 127.0.0.1:3000 A1 B2": "malformed command: watch 127.0.0.1:3000 A1 B2",
	}
	for command, message := range commands {
		stdout.Reset()
		if !runner.handle(command) {
			t.Fatalf("%q ended the driver", command)
		}
		replies := outputLines(t, stdout)
		if len(replies) != 1 || replies[0]["type"] != "error" || replies[0]["message"] != message {
			t.Fatalf("%q replied %v, want one error %q", command, replies, message)
		}
	}
	if runner.handle("quit") {
		t.Fatal("quit did not end the driver")
	}
}

func TestResetRestartsMaxLatencyAndReplies(t *testing.T) {
	runner, stdout := recordingRunner(t)
	runner.stats.recordSuccess(0, 400*time.Millisecond)
	runner.handle("reset")

	replies := outputLines(t, stdout)
	if len(replies) != 1 || replies[0]["type"] != "reset" {
		t.Fatalf("replies %v, want one reset", replies)
	}
	object := line{}
	runner.stats.addTo(object)
	if object["maxLatencyMs"] != int64(0) {
		t.Fatalf("maxLatencyMs after reset = %v", object["maxLatencyMs"])
	}
}

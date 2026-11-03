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

package dns

import (
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const timeoutMargin = 200 * time.Millisecond

type logCapture struct {
	msgs []string
}

func (capture *logCapture) Printf(format string, v ...any) {
	capture.msgs = append(capture.msgs, fmt.Sprintf(format, v...))
}

func (capture *logCapture) messages() []string {
	return append([]string(nil), capture.msgs...)
}

func captureLogs(t *testing.T, level logger.LogPriority) *logCapture {
	t.Helper()
	capture := &logCapture{}
	logger.Logger.SetLogger(capture)
	logger.Logger.SetLevel(level)
	t.Cleanup(func() {
		logger.Logger.SetLevel(logger.OFF)
		logger.Logger.SetLogger(log.New(io.Discard, "", 0))
	})
	return capture
}

func lookup(t *testing.T, server *stubDNSServer, cfg Config) ([]Candidate, []string, error) {
	t.Helper()
	logs := captureLogs(t, logger.WARNING)
	cfg.Servers = []string{server.Address()}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	resolver, err := NewResolver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hosts, lookupErr := resolver.Lookup(resolver.Config().Timeout)
	return hosts, logs.messages(), lookupErr
}

func TestAAndAAAARecords(t *testing.T) {
	server := newStubDNSServer(t)
	server.a("seeds.example.com", "10.0.0.9", "10.0.0.2").aaaa("seeds.example.com", "2001:db8::1")

	hosts, warnings, err := lookup(t, server, Config{Names: []string{"seeds.example.com"}, Port: 3100})
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{
		{Address: "10.0.0.2", Port: 3100},
		{Address: "10.0.0.9", Port: 3100},
		{Address: "2001:db8::1", Port: 3100},
	}
	if !candidatesEqual(hosts, want) {
		t.Fatalf("got %#v want %#v", hosts, want)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings %v", warnings)
	}

	again, _, err := lookup(t, server, Config{Names: []string{"seeds.example.com"}, Port: 3100})
	if err != nil {
		t.Fatal(err)
	}
	if !candidatesEqual(again, hosts) {
		t.Fatalf("second lookup %#v want %#v", again, hosts)
	}
}

func TestDefaultPortIs3000(t *testing.T) {
	server := newStubDNSServer(t)
	server.a("seeds.example.com", "10.0.0.1")

	resolver, err := NewResolver(Config{Names: []string{"seeds.example.com"}, Servers: []string{server.Address()}})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.Config().Port != DefaultPort {
		t.Fatalf("port %d", resolver.Config().Port)
	}
	if resolver.Config().Timeout != DefaultTimeout {
		t.Fatalf("timeout %s", resolver.Config().Timeout)
	}
	if resolver.Config().RecordType != RecordTypeA {
		t.Fatalf("record type %d", resolver.Config().RecordType)
	}

	hosts, err := resolver.Lookup(resolver.Config().Timeout)
	if err != nil {
		t.Fatal(err)
	}
	if !candidatesEqual(hosts, []Candidate{{Address: "10.0.0.1", Port: 3000}}) {
		t.Fatalf("got %#v", hosts)
	}
}

func TestMultipleNamesInOrderAndDeduplicated(t *testing.T) {
	server := newStubDNSServer(t)
	server.a("b.example.com", "10.0.0.2", "10.0.0.3")
	server.a("a.example.com", "10.0.0.1", "10.0.0.2")

	hosts, _, err := lookup(t, server, Config{Names: []string{"b.example.com", "a.example.com."}})
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{
		{Address: "10.0.0.2", Port: 3000},
		{Address: "10.0.0.3", Port: 3000},
		{Address: "10.0.0.1", Port: 3000},
	}
	if !candidatesEqual(hosts, want) {
		t.Fatalf("got %#v want %#v", hosts, want)
	}
}

func TestSRVTargetsResolvedInRefresh(t *testing.T) {
	server := newStubDNSServer(t)
	server.srv("_aerospike._tcp.example.com", 20, 5, 3002, "node2.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 9, 3003, "node3.example.com")
	server.a("node1.example.com", "10.0.1.1")
	server.a("node2.example.com", "10.0.1.2")
	server.a("node3.example.com", "10.0.1.3")

	hosts, _, err := lookup(t, server, Config{
		Names:      []string{"_aerospike._tcp.example.com"},
		RecordType: RecordTypeSRV,
		Port:       4000,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{
		{Address: "10.0.1.3", Port: 3003},
		{Address: "10.0.1.1", Port: 3001},
		{Address: "10.0.1.2", Port: 3002},
	}
	if !candidatesEqual(hosts, want) {
		t.Fatalf("got %#v want %#v queries %v", hosts, want, server.Queries())
	}
}

func TestSRVTargetsFromFollowUpQueries(t *testing.T) {
	server := newStubDNSServer(t)
	server.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
	server.a("node1.example.com", "10.0.1.1")
	server.aaaa("node2.example.com", "2001:db8::2")

	hosts, _, err := lookup(t, server, Config{
		Names:      []string{"_aerospike._tcp.example.com"},
		RecordType: RecordTypeSRV,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{
		{Address: "10.0.1.1", Port: 3001},
		{Address: "2001:db8::2", Port: 3002},
	}
	if !candidatesEqual(hosts, want) {
		t.Fatalf("got %#v want %#v queries %v", hosts, want, server.Queries())
	}
	joined := strings.Join(server.Queries(), " ")
	if !strings.Contains(joined, "node1.example.com./"+itoa(stubTypeA)) {
		t.Fatalf("missing A follow-up in %v", server.Queries())
	}
	if !strings.Contains(joined, "node2.example.com./"+itoa(stubTypeAAAA)) {
		t.Fatalf("missing AAAA follow-up in %v", server.Queries())
	}
}

func TestSRVTargetThatDoesNotResolveIsDropped(t *testing.T) {
	server := newStubDNSServer(t)
	server.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3002, "gone.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3003, ".")
	server.a("node1.example.com", "10.0.1.1")

	hosts, warnings, err := lookup(t, server, Config{
		Names:      []string{"_aerospike._tcp.example.com"},
		RecordType: RecordTypeSRV,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !candidatesEqual(hosts, []Candidate{{Address: "10.0.1.1", Port: 3001}}) {
		t.Fatalf("got %#v", hosts)
	}
	if len(warnings) != 1 || warnings[0] != "DNS seed name _aerospike._tcp.example.com SRV target gone.example.com. dropped: NXDOMAIN" {
		t.Fatalf("warnings %v", warnings)
	}
}

func TestPartialFailureReturnsResolvedNamesAndWarns(t *testing.T) {
	server := newStubDNSServer(t)
	server.a("good.example.com", "10.0.0.1")
	server.rcode("bad.example.com", stubTypeA, stubSERVFAIL)
	server.rcode("bad.example.com", stubTypeAAAA, stubSERVFAIL)

	hosts, warnings, err := lookup(t, server, Config{
		Names: []string{"missing.example.com", "good.example.com", "bad.example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !candidatesEqual(hosts, []Candidate{{Address: "10.0.0.1", Port: 3000}}) {
		t.Fatalf("got %#v", hosts)
	}
	if len(warnings) != 2 ||
		warnings[0] != "DNS seed name missing.example.com failed: NXDOMAIN" ||
		warnings[1] != "DNS seed name bad.example.com failed: SERVFAIL" {
		t.Fatalf("warnings %v", warnings)
	}
}

func TestEveryNameFailingThrowsNamingEach(t *testing.T) {
	server := newStubDNSServer(t)
	server.rcode("bad.example.com", stubTypeA, stubSERVFAIL)
	server.rcode("bad.example.com", stubTypeAAAA, stubSERVFAIL)
	server.srv("other.example.com", 1, 1, 1, "x.example.com")

	_, _, err := lookup(t, server, Config{
		Names: []string{"missing.example.com", "bad.example.com", "other.example.com"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	want := "DNS seed discovery failed for every name: missing.example.com: NXDOMAIN; bad.example.com: SERVFAIL; other.example.com: NXDOMAIN"
	if err.Error() != want {
		t.Fatalf("got %q want %q", err.Error(), want)
	}
}

func TestNODATAOnAAndAAAAIsReportedAsNXDOMAIN(t *testing.T) {
	// Go nuance (GO-TEST-PLAN.md). The name exists as SRV only, so A/AAAA are
	// NODATA. Java JNDI reports empty answer. net.Resolver reports NXDOMAIN.
	server := newStubDNSServer(t)
	server.srv("other.example.com", 1, 1, 1, "x.example.com")

	_, _, err := lookup(t, server, Config{Names: []string{"other.example.com"}})
	if err == nil {
		t.Fatal("expected error")
	}
	want := "DNS seed discovery failed for every name: other.example.com: NXDOMAIN"
	if err.Error() != want {
		t.Fatalf("got %q want %q", err.Error(), want)
	}
}

func TestUnansweredQuerySharesOneRefreshDeadline(t *testing.T) {
	server, err := startStubDNSServer(true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })

	timeout := 80 * time.Millisecond
	resolver, err := NewResolver(Config{
		Names:   []string{"a.example.com", "b.example.com"},
		Timeout: timeout,
		Servers: []string{server.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		begin := time.Now()
		_, lookupErr := resolver.Lookup(resolver.Config().Timeout)
		elapsed := time.Since(begin)
		if lookupErr == nil {
			t.Fatal("expected error")
		}
		if elapsed > timeout+timeoutMargin {
			t.Fatalf("elapsed %s timeout %s", elapsed, timeout)
		}
		want := "DNS seed discovery failed for every name: a.example.com: timeout; b.example.com: timeout"
		if lookupErr.Error() != want {
			t.Fatalf("got %q want %q", lookupErr.Error(), want)
		}
	}
}

func TestUnansweredSRVTargetsShareOneBudget(t *testing.T) {
	server := newStubDNSServer(t)
	server.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
	server.srv("_aerospike._tcp.example.com", 10, 1, 3003, "node3.example.com")
	for _, target := range []string{"node1.example.com", "node2.example.com", "node3.example.com"} {
		server.rcode(target, stubTypeA, stubNoAnswer)
		server.rcode(target, stubTypeAAAA, stubNoAnswer)
	}

	timeout := 60 * time.Millisecond
	logs := captureLogs(t, logger.WARNING)
	resolver, err := NewResolver(Config{
		Names:      []string{"_aerospike._tcp.example.com"},
		RecordType: RecordTypeSRV,
		Timeout:    timeout,
		Servers:    []string{server.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	begin := time.Now()
	_, lookupErr := resolver.Lookup(resolver.Config().Timeout)
	elapsed := time.Since(begin)
	if lookupErr == nil {
		t.Fatal("expected error")
	}
	if elapsed > timeout+timeoutMargin {
		t.Fatalf("elapsed %s timeout %s", elapsed, timeout)
	}
	want := "DNS seed discovery failed for every name: _aerospike._tcp.example.com: no SRV target resolved"
	if lookupErr.Error() != want {
		t.Fatalf("got %q want %q", lookupErr.Error(), want)
	}
	warnings := logs.messages()
	if len(warnings) != 3 {
		t.Fatalf("warnings %v", warnings)
	}
	for _, warning := range warnings {
		if !strings.HasSuffix(warning, "dropped: timeout") {
			t.Fatalf("warning %q", warning)
		}
	}
}

func TestTruncatedUDPWithHangingTCPStaysInsideRefreshDeadline(t *testing.T) {
	// Go nuance (GO-TEST-PLAN.md). One truncated server plus hanging TCP.
	// Java splits the budget across a silent server and a truncated one.
	server, err := startStubDNSServer(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	server.a("a.example.com", "10.0.0.1").truncate().hangTCP()

	timeout := 200 * time.Millisecond
	resolver, err := NewResolver(Config{
		Names:   []string{"a.example.com"},
		Timeout: timeout,
		Servers: []string{server.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	begin := time.Now()
	_, lookupErr := resolver.Lookup(resolver.Config().Timeout)
	elapsed := time.Since(begin)
	if lookupErr == nil {
		t.Fatal("expected error")
	}
	if elapsed > timeout+timeoutMargin {
		t.Fatalf("elapsed %s timeout %s", elapsed, timeout)
	}
	if len(server.Queries()) == 0 {
		t.Fatal("expected a UDP query")
	}
}

func silentStubDNSServer(t *testing.T) *stubDNSServer {
	t.Helper()
	server, err := startStubDNSServer(true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}

func TestSilentFirstServerFailsOverToTheNext(t *testing.T) {
	silent := silentStubDNSServer(t)
	good := newStubDNSServer(t)
	good.a("a.example.com", "10.0.0.1")

	timeout := 400 * time.Millisecond
	resolver, err := NewResolver(Config{
		Names:   []string{"a.example.com"},
		Timeout: timeout,
		Servers: []string{silent.Address(), good.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	begin := time.Now()
	hosts, lookupErr := resolver.Lookup(timeout)
	elapsed := time.Since(begin)
	if lookupErr != nil {
		t.Fatal(lookupErr)
	}
	if !candidatesEqual(hosts, []Candidate{{Address: "10.0.0.1", Port: DefaultPort}}) {
		t.Fatalf("got %#v", hosts)
	}
	if elapsed > timeout+timeoutMargin {
		t.Fatalf("elapsed %s timeout %s", elapsed, timeout)
	}
	if len(silent.Queries()) == 0 || len(good.Queries()) == 0 {
		t.Fatalf("silent %v good %v", silent.Queries(), good.Queries())
	}
}

func TestEverySilentServerIsAskedInsideOneTimeout(t *testing.T) {
	first := silentStubDNSServer(t)
	second := silentStubDNSServer(t)

	timeout := 120 * time.Millisecond
	resolver, err := NewResolver(Config{
		Names:   []string{"a.example.com"},
		Timeout: timeout,
		Servers: []string{first.Address(), second.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	begin := time.Now()
	_, lookupErr := resolver.Lookup(timeout)
	elapsed := time.Since(begin)
	if lookupErr == nil {
		t.Fatal("expected error")
	}
	if elapsed > timeout+timeoutMargin {
		t.Fatalf("elapsed %s timeout %s", elapsed, timeout)
	}
	want := "DNS seed discovery failed for every name: a.example.com: timeout"
	if lookupErr.Error() != want {
		t.Fatalf("got %q want %q", lookupErr.Error(), want)
	}
	if len(first.Queries()) == 0 || len(second.Queries()) == 0 {
		t.Fatalf("first %v second %v", first.Queries(), second.Queries())
	}
}

func TestNXDOMAINFromTheFirstServerIsTheAnswer(t *testing.T) {
	first := newStubDNSServer(t)
	second := newStubDNSServer(t)
	second.a("a.example.com", "10.0.0.1")

	resolver, err := NewResolver(Config{
		Names:   []string{"a.example.com"},
		Timeout: time.Second,
		Servers: []string{first.Address(), second.Address()},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, lookupErr := resolver.Lookup(time.Second)
	want := "DNS seed discovery failed for every name: a.example.com: NXDOMAIN"
	if lookupErr == nil || lookupErr.Error() != want {
		t.Fatalf("got %v want %q", lookupErr, want)
	}
	if len(second.Queries()) != 0 {
		t.Fatalf("second server asked %v", second.Queries())
	}
}

func TestConstructionDoesNotProbeSystemResolvers(t *testing.T) {
	// Go nuance (GO-TEST-PLAN.md). Java probes system resolvers at build when
	// servers is empty. Go constructs and uses the system resolvers on lookup.
	resolver, err := NewResolver(Config{Names: []string{"seeds.example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resolver.Config().Servers) != 0 {
		t.Fatalf("servers %v", resolver.Config().Servers)
	}
}

func TestInvalidConfigRejectedAtConstruction(t *testing.T) {
	_, err := NewResolver(Config{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "at least one name is required") {
		t.Fatalf("got %q", err.Error())
	}
}

func candidatesEqual(got, want []Candidate) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

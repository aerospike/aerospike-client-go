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
	"strings"
	"testing"
	"time"
)

func TestNormalizeAppliesZeroDefaults(t *testing.T) {
	cfg, dial, err := normalize(Config{Names: []string{"seeds.example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != DefaultPort || cfg.Timeout != DefaultTimeout || cfg.RecordType != RecordTypeA {
		t.Fatalf("got port=%d timeout=%s recordType=%d", cfg.Port, cfg.Timeout, cfg.RecordType)
	}
	if len(cfg.Servers) != 0 || len(dial) != 0 {
		t.Fatalf("empty Servers must leave system resolvers, got servers=%v dial=%v", cfg.Servers, dial)
	}
}

func TestNormalizeKeepsConfiguredValues(t *testing.T) {
	cfg, dial, err := normalize(Config{
		Names:      []string{"seeds-a.example.com", "seeds-b.example.com."},
		RecordType: RecordTypeSRV,
		Port:       3100,
		Timeout:    50 * time.Millisecond,
		Servers:    []string{"10.0.0.1", "[::1]:5353"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(cfg.Names, []string{"seeds-a.example.com", "seeds-b.example.com."}) {
		t.Fatalf("names %v", cfg.Names)
	}
	if cfg.RecordType != RecordTypeSRV || cfg.Port != 3100 || cfg.Timeout != 50*time.Millisecond {
		t.Fatalf("got %#v", cfg)
	}
	if !equalStrings(cfg.Servers, []string{"10.0.0.1", "[::1]:5353"}) {
		t.Fatalf("Servers must keep the caller strings, got %v", cfg.Servers)
	}
	if !equalStrings(dial, []string{"10.0.0.1:53", "[::1]:5353"}) {
		t.Fatalf("dial must be host:port, got %v", dial)
	}
}

func TestNormalizeRequiresAName(t *testing.T) {
	mustReject(t, Config{}, "at least one name is required")
}

func TestNormalizeRejectsEmptyDotAndSpacedNames(t *testing.T) {
	mustReject(t, Config{Names: []string{"a.example.com", ""}}, "Invalid DNS seed provider name: ''")
	mustReject(t, Config{Names: []string{"a.example.com", "."}}, "Invalid DNS seed provider name: '.'")
	mustReject(t, Config{Names: []string{"a.example.com", "bad name"}}, "Invalid DNS seed provider name: 'bad name'")
}

func TestNormalizeRejectsDuplicateNames(t *testing.T) {
	mustReject(t, Config{Names: []string{"a.example.com", "A.example.com"}}, "duplicate")
	mustReject(t, Config{Names: []string{"a.example.com", "a.example.com."}}, "duplicate name 'a.example.com.'")
}

func TestNormalizeRejectsUnknownRecordType(t *testing.T) {
	mustReject(t, Config{Names: []string{"seeds.example.com"}, RecordType: RecordType(9)}, "Invalid DNS seed provider recordType")
}

func TestNormalizeRejectsOutOfRangePortAndTimeout(t *testing.T) {
	mustReject(t, Config{Names: []string{"seeds.example.com"}, Port: -1}, "Invalid DNS seed provider port")
	mustReject(t, Config{Names: []string{"seeds.example.com"}, Port: 65536}, "Invalid DNS seed provider port: 65536")
	mustReject(t, Config{Names: []string{"seeds.example.com"}, Timeout: -time.Second}, "Invalid DNS seed provider timeout")
}

func TestNormalizeRejectsNonIPServer(t *testing.T) {
	mustReject(t, Config{Names: []string{"seeds.example.com"}, Servers: []string{"dns.example.com"}}, "expected an IP address")
}

func TestParseServerAcceptsIPLiterals(t *testing.T) {
	assertDial := func(in, want string) {
		t.Helper()
		got, err := parseServer(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got != want {
			t.Fatalf("%q: dial %q want %q", in, got, want)
		}
	}

	assertDial("10.0.0.1", "10.0.0.1:53")
	assertDial("10.0.0.2:5353", "10.0.0.2:5353")
	assertDial("2001:db8::1", "[2001:db8::1]:53")
	assertDial("[2001:db8::2]:5353", "[2001:db8::2]:5353")
	assertDial("[::1]", "[::1]:53")
}

func TestParseServerRejectsHostnamesAndBadPorts(t *testing.T) {
	assertRejected := func(in, want string) {
		t.Helper()
		_, err := parseServer(in)
		if err == nil {
			t.Fatalf("expected error for %q", in)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%q: got %q want substring %q", in, err.Error(), want)
		}
	}

	assertRejected("", "Invalid DNS seed provider server: null")
	assertRejected("dns.example.com", "expected an IP address")
	assertRejected("10.0.0.256", "Invalid DNS seed provider server: '10.0.0.256'")
	assertRejected("10.0.0.1:x", "invalid port")
	assertRejected("10.0.0.1:70000", "invalid port")
	assertRejected("[2001:db8::1", "Invalid DNS seed provider server: '[2001:db8::1'")
	assertRejected("[::1]foo", "Invalid DNS seed provider server: '[::1]foo'")
	assertRejected("2001:db8::zz", "expected an IP address")
}

func mustReject(t *testing.T, cfg Config, want string) {
	t.Helper()
	_, _, err := normalize(cfg)
	if err == nil {
		t.Fatalf("expected error for %#v", cfg)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("got %q want substring %q", err.Error(), want)
	}
}

func equalStrings(got, want []string) bool {
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

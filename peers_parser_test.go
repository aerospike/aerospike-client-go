// Copyright 2014-2022 Aerospike, Inc.
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

package aerospike

// Tests for ClientPolicy.IpMap translation (CLIENT-5133: the field regressed to
// dead code in 2021, plus the NLB "host:port" extension).
//
// They live in the internal `aerospike` package because peerListParser,
// ParseHost, ipMap, peer.hosts, parsePeers and Client.cluster are unexported.
//
// Two layers:
//
//  1. Contract specs — pin the translation *semantics* against the real
//     server wire grammar. The fixtures below are captured verbatim from a
//     live 8.1 node so the tests exercise the exact strings the client parses:
//       peers-clear-std   -> "0,3000,[]"          (gen,defPort,[peers])
//       service-clear-std -> "10.88.0.7:3100"     (node's own advertised addr)
//     Note the node advertises its *container* IP 10.88.0.7, which the host
//     cannot reach — it connects via 127.0.0.1:3100. That is exactly the case
//     IpMap exists for, so the fixtures double as a realistic mapping scenario.
//
//  2. Wiring guard — TestIpMapWiredIntoPeerDiscovery drives the real
//     parsePeers() entry point and asserts the policy's IpMap actually reaches
//     the parser. This is the spec that fails on the 2021-regressed code (where
//     the map was defined but never plumbed in); the contract specs alone would
//     pass against that code because they inject the map directly.

import (
	"flag"
	"strconv"
	"testing"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ClientPolicy.IpMap translation", func() {

	// ---- 1a. ParseHost: the leaf translation, exercised directly. ----
	gg.Describe("ParseHost", func() {

		gg.It("leaves the address unchanged when the map is nil", func() {
			p := peerListParser{ipMap: nil}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("leaves the address unchanged when the host has no entry", func() {
			p := peerListParser{ipMap: map[string]string{"9.9.9.9": "1.1.1.1"}}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("ignores an empty mapping value", func() {
			p := peerListParser{ipMap: map[string]string{"10.88.0.7": ""}}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("replaces the host only and preserves the advertised port (Java/C parity)", func() {
			p := peerListParser{ipMap: map[string]string{"10.88.0.7": "127.0.0.1"}}
			assertParseHost(p, "10.88.0.7:3100", "127.0.0.1", 3100)
		})

		gg.It("replaces both host and port when the value carries a port (NLB extension)", func() {
			p := peerListParser{ipMap: map[string]string{"10.88.0.7": "127.0.0.1:3100"}}
			assertParseHost(p, "10.88.0.7:3100", "127.0.0.1", 3100)
		})

		gg.It("keys on the advertised host WITHOUT its port", func() {
			// The map key is the bare host. An entry that includes the port must
			// NOT match — this is the contract that lets one internal host map
			// regardless of the port it was advertised on.
			p := peerListParser{ipMap: map[string]string{"10.88.0.7:3100": "127.0.0.1:9999"}}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100) // unchanged
		})

		gg.It("applies the map after falling back to the default port", func() {
			// When the advertised address carries no port, defPort is used first,
			// then a host-only mapping preserves that resolved port.
			defPort := int64(3000)
			p := peerListParser{defPort: &defPort, ipMap: map[string]string{"10.88.0.7": "127.0.0.1"}}
			assertParseHost(p, "10.88.0.7", "127.0.0.1", 3000)
		})

		gg.It("keys an IPv6 advertised address on the bracket-stripped host", func() {
			p := peerListParser{ipMap: map[string]string{"fe80::1": "127.0.0.1:3100"}}
			assertParseHost(p, "[fe80::1]:3000", "127.0.0.1", 3100)
		})

		gg.It("maps several internal hosts behind one NLB name to distinct ports", func() {
			p := peerListParser{ipMap: map[string]string{
				"10.0.0.1": "nlb.example.com:4001",
				"10.0.0.2": "nlb.example.com:4002",
			}}
			assertParseHost(p, "10.0.0.1:3000", "nlb.example.com", 4001)
			assertParseHost(p, "10.0.0.2:3000", "nlb.example.com", 4002)
		})

		gg.It("treats a value with no colon as a bare host, preserving the port", func() {
			p := peerListParser{ipMap: map[string]string{"10.88.0.7": "reachable.internal"}}
			assertParseHost(p, "10.88.0.7:3100", "reachable.internal", 3100)
		})

		gg.It("treats a value whose colon-suffix is not a port as a whole host", func() {
			// e.g. a bracketed IPv6 literal: the text after the last ':' ("1]") is
			// not numeric, so the entire value is used as the host, port preserved.
			p := peerListParser{ipMap: map[string]string{"10.88.0.7": "[fe80::1]"}}
			assertParseHost(p, "10.88.0.7:3100", "[fe80::1]", 3100)
		})
	})

	// ---- 1b. Parse: the full peers-discovery path (peers_parser.go). ----
	gg.Describe("Parse (peers-discovery path)", func() {

		gg.It("parses the real empty single-node fixture without translating anything", func() {
			p := peerListParser{
				buf:   []byte("0,3000,[]"), // captured: peers-clear-std on a single node
				ipMap: map[string]string{"10.88.0.7": "127.0.0.1:3100"},
			}
			gm.Expect(p.Parse()).ToNot(gm.HaveOccurred())
			gm.Expect(p.generation()).To(gm.Equal(int64(0)))
			gm.Expect(p.peers).To(gm.BeEmpty())
		})

		gg.It("translates only the mapped hosts in a multi-node peer list, leaving others intact", func() {
			// gen,defPort,[ [node,tls,[host,...]], ... ] — same grammar as the real
			// fixture, populated as a real multi-node cluster would advertise.
			buf := "7,3000," +
				"[[BB9050011AC4202,,[10.88.0.7:3100]]," + // mapped
				"[BB9050022AC4202,,[10.88.0.8:3100,10.88.0.9:3100]]," + // first host mapped, second not
				"[BB9050033AC4202,,[192.168.5.5:3200]]]" // no entry -> untouched
			p := peerListParser{
				buf: []byte(buf),
				ipMap: map[string]string{
					"10.88.0.7": "127.0.0.1:3100",
					"10.88.0.8": "127.0.0.1:3200",
				},
			}

			gm.Expect(p.Parse()).ToNot(gm.HaveOccurred())
			gm.Expect(p.generation()).To(gm.Equal(int64(7)))
			gm.Expect(p.peers).To(gm.HaveLen(3))

			// node 0: single mapped host
			gm.Expect(hostNames(p.peers[0].hosts)).To(gm.Equal([]string{"127.0.0.1"}))
			gm.Expect(p.peers[0].hosts[0].Port).To(gm.Equal(3100))

			// node 1: first host mapped (and re-ported), second host untouched
			gm.Expect(hostNames(p.peers[1].hosts)).To(gm.Equal([]string{"127.0.0.1", "10.88.0.9"}))
			gm.Expect(p.peers[1].hosts[0].Port).To(gm.Equal(3200))
			gm.Expect(p.peers[1].hosts[1].Port).To(gm.Equal(3100))

			// node 2: no entry -> verbatim
			gm.Expect(hostNames(p.peers[2].hosts)).To(gm.Equal([]string{"192.168.5.5"}))
			gm.Expect(p.peers[2].hosts[0].Port).To(gm.Equal(3200))
		})

		gg.It("leaves every peer address verbatim when no map is configured", func() {
			p := peerListParser{buf: []byte("7,3000,[[BB9050011AC4202,,[10.88.0.7:3100]]]")}
			gm.Expect(p.Parse()).ToNot(gm.HaveOccurred())
			gm.Expect(p.peers[0].hosts[0].Name).To(gm.Equal("10.88.0.7"))
			gm.Expect(p.peers[0].hosts[0].Port).To(gm.Equal(3100))
		})
	})

	// ---- 1c. readHosts: the seed/service-address path (node_validator.go). ----
	gg.Describe("readHosts (seed/service-address path)", func() {

		gg.It("rewrites the real service-clear-std fixture to a reachable address", func() {
			// node_validator wraps the service-address string in brackets and passes
			// the seed's TLS name through, exactly as reproduced here.
			p := peerListParser{
				buf:   []byte("[10.88.0.7:3100]"), // captured: service-clear-std
				ipMap: map[string]string{"10.88.0.7": "127.0.0.1:3100"},
			}
			hosts, err := p.readHosts("cluster-tls-name")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hosts).To(gm.HaveLen(1))
			gm.Expect(hosts[0].Name).To(gm.Equal("127.0.0.1"))
			gm.Expect(hosts[0].Port).To(gm.Equal(3100))
			gm.Expect(hosts[0].TLSName).To(gm.Equal("cluster-tls-name")) // survives translation
		})

		gg.It("leaves the service address verbatim when no map is configured", func() {
			p := peerListParser{buf: []byte("[10.88.0.7:3100]")}
			hosts, err := p.readHosts("cluster-tls-name")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hosts[0].Name).To(gm.Equal("10.88.0.7"))
			gm.Expect(hosts[0].Port).To(gm.Equal(3100))
		})
	})
})

// hostNames extracts the Name of each host for order-sensitive comparison.
func hostNames(hosts []*Host) []string {
	names := make([]string, len(hosts))
	for i, h := range hosts {
		names[i] = h.Name
	}
	return names
}

// assertParseHost runs ParseHost(in) and checks the resulting name/port.
func assertParseHost(p peerListParser, in, name string, port int) {
	h, err := p.ParseHost(in)
	gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())
	gm.ExpectWithOffset(1, h.Name).To(gm.Equal(name))
	gm.ExpectWithOffset(1, h.Port).To(gm.Equal(port))
}

// ---- 2. Wiring guard (integration). ----
//
// The contract specs above inject the map straight into peerListParser, so they
// pass even against the 2021-regressed code where IpMap was never wired into
// discovery. This test closes that gap: it connects a client with an IpMap set
// and drives the REAL parsePeers() entry point, asserting the map arrives in the
// parser it builds. On the regressed code parsePeers built the parser without
// ipMap, so p.ipMap would be nil here and this test would fail.
//
// It is a plain Go test (not a Ginkgo spec) so it can self-connect using the
// suite's process-global -h/-p/-U/-P flags, which are not reachable from this
// internal package as variables. It skips cleanly if no cluster is available.
func TestIpMapWiredIntoPeerDiscovery(t *testing.T) {
	host := lookupFlag("h", "127.0.0.1")
	port, _ := strconv.Atoi(lookupFlag("p", "3000"))

	policy := NewClientPolicy()
	if u := lookupFlag("U", ""); u != "" {
		policy.User = u
		policy.Password = lookupFlag("P", "")
	}
	wanted := map[string]string{"10.88.0.7": "127.0.0.1:3100"}
	policy.IpMap = wanted

	client, err := NewClientWithPolicyAndHost(policy, NewHost(host, port))
	if err != nil {
		t.Skipf("IpMap wiring guard skipped: no reachable cluster at %s:%d (%v)", host, port, err)
		return
	}
	defer client.Close()

	nodes := client.cluster.GetNodes()
	if len(nodes) == 0 {
		t.Skip("IpMap wiring guard skipped: cluster reported no nodes")
		return
	}

	p, perr := parsePeers(client.cluster, nodes[0])
	if perr != nil {
		t.Fatalf("parsePeers failed: %v", perr)
	}

	// The core regression guard: discovery must hand the policy's IpMap to the
	// parser. (The peer list itself is empty on a single node — that is fine;
	// what matters is that the map was threaded through.)
	if got, ok := p.ipMap["10.88.0.7"]; !ok || got != wanted["10.88.0.7"] {
		t.Fatalf("parsePeers did not wire ClientPolicy.IpMap into the parser: got %v, want %v", p.ipMap, wanted)
	}
}

// lookupFlag returns the value of a process-global test flag, or def if unset.
func lookupFlag(name, def string) string {
	if f := flag.Lookup(name); f != nil {
		if v := f.Value.String(); v != "" {
			return v
		}
	}
	return def
}

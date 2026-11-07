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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("ClientPolicy.IpMap translation", gg.Label("discovery"), func() {

	// ---- 1a. ParseHost: the leaf translation, exercised directly. ----
	gg.Describe("ParseHost", func() {

		gg.It("leaves the address unchanged when the map is nil", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(nil))}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("leaves the address unchanged when the host has no entry", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"9.9.9.9": "1.1.1.1"}))}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("ignores an empty mapping value", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": ""}))}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100)
		})

		gg.It("replaces the host only and preserves the advertised port (Java/C parity)", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "127.0.0.1"}))}
			assertParseHost(p, "10.88.0.7:3100", "127.0.0.1", 3100)
		})

		gg.It("replaces both host and port when the value carries a port (NLB extension)", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "127.0.0.1:3100"}))}
			assertParseHost(p, "10.88.0.7:3100", "127.0.0.1", 3100)
		})

		gg.It("keys on the advertised host WITHOUT its port", func() {
			// The map key is the bare host. An entry that includes the port must
			// NOT match — this is the contract that lets one internal host map
			// regardless of the port it was advertised on.
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7:3100": "127.0.0.1:9999"}))}
			assertParseHost(p, "10.88.0.7:3100", "10.88.0.7", 3100) // unchanged
		})

		gg.It("applies the map after falling back to the default port", func() {
			// When the advertised address carries no port, defPort is used first,
			// then a host-only mapping preserves that resolved port.
			defPort := int64(3000)
			p := peerListParser{defPort: &defPort, cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "127.0.0.1"}))}
			assertParseHost(p, "10.88.0.7", "127.0.0.1", 3000)
		})

		gg.It("keys an IPv6 advertised address on the bracket-stripped host", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"fe80::1": "127.0.0.1:3100"}))}
			assertParseHost(p, "[fe80::1]:3000", "127.0.0.1", 3100)
		})

		gg.It("maps several internal hosts behind one NLB name to distinct ports", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{
				"10.0.0.1": "nlb.example.com:4001",
				"10.0.0.2": "nlb.example.com:4002",
			}))}
			assertParseHost(p, "10.0.0.1:3000", "nlb.example.com", 4001)
			assertParseHost(p, "10.0.0.2:3000", "nlb.example.com", 4002)
		})

		gg.It("treats a value with no colon as a bare host, preserving the port", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "reachable.internal"}))}
			assertParseHost(p, "10.88.0.7:3100", "reachable.internal", 3100)
		})

		gg.It("treats a value whose colon-suffix is not a port as a whole host", func() {
			// e.g. a bracketed IPv6 literal: the text after the last ':' ("1]") is
			// not numeric, so the entire value is used as the host, port preserved.
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "[fe80::1]"}))}
			assertParseHost(p, "10.88.0.7:3100", "[fe80::1]", 3100)
		})

		gg.It("treats an IPv6 literal value ending in digits as a whole host, preserving the port", func() {
			// The trailing "::10" group is hex digits of the address, not a port.
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "2001:db8:ffff::10"}))}
			assertParseHost(p, "10.88.0.7:3100", "2001:db8:ffff::10", 3100)
		})

		gg.It("treats an IPv6 literal value ending in a hex letter as a whole host, preserving the port", func() {
			p := peerListParser{cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "2001:db8::a"}))}
			assertParseHost(p, "10.88.0.7:3100", "2001:db8::a", 3100)
		})
	})

	// ---- 1b. Parse: the full peers-discovery path (peers_parser.go). ----
	gg.Describe("Parse (peers-discovery path)", func() {

		gg.It("parses the real empty single-node fixture without translating anything", func() {
			p := peerListParser{
				buf:     []byte("0,3000,[]"), // captured: peers-clear-std on a single node
				cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "127.0.0.1:3100"})),
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
				cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{
					"10.88.0.7": "127.0.0.1:3100",
					"10.88.0.8": "127.0.0.1:3200",
				})),
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
				buf:     []byte("[10.88.0.7:3100]"), // captured: service-clear-std
				cluster: translationCluster(NewStaticMapAddressTranslator(map[string]string{"10.88.0.7": "127.0.0.1:3100"})),
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

type recordingAddressTranslator struct {
	endpoints []Endpoint
}

func (translator *recordingAddressTranslator) Translate(advertised Endpoint) (Host, bool) {
	translator.endpoints = append(translator.endpoints, advertised)
	return Host{}, false
}

var _ = gg.Describe("Peers parsing with a translator", gg.Label("discovery"), func() {

	const twoPeerReply = "7,3000,[[BB9010011AC4202,,[10.88.0.7]],[BB9020011AC4202,peer-2.internal,[10.88.0.8:3200]]]"

	gg.It("hands the translator each advertised endpoint with its peer identity", func() {
		recorder := &recordingAddressTranslator{}
		parser := peerListParser{
			buf:     []byte(twoPeerReply),
			cluster: translationClusterAlternate(recorder),
		}

		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())

		gm.Expect(recorder.endpoints).To(gm.HaveLen(2))
		gm.Expect(recorder.endpoints[0]).To(gm.Equal(Endpoint{
			NodeName: "BB9010011AC4202", Host: "10.88.0.7", Port: 3000, SourceList: SourceListAlternate}))
		gm.Expect(recorder.endpoints[1]).To(gm.Equal(Endpoint{
			NodeName: "BB9020011AC4202", TLSName: "peer-2.internal", Host: "10.88.0.8", Port: 3200, SourceList: SourceListAlternate}))
	})

	gg.It("keeps the peer and drops only the missed address under strict handling", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"10.88.0.7:3000": "gateway.example.com:30001"})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		policy.TranslationOnMiss = TranslationMissStrict
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		parser := peerListParser{
			buf:     []byte(twoPeerReply),
			cluster: cluster,
		}

		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(parser.peers).To(gm.HaveLen(2))

		gm.Expect(parser.peers[0].hosts).To(gm.HaveLen(1))
		gm.Expect(parser.peers[0].hosts[0].Name).To(gm.Equal("gateway.example.com"))
		gm.Expect(parser.peers[0].hosts[0].Port).To(gm.Equal(30001))

		gm.Expect(parser.peers[1].hosts).To(gm.BeEmpty())
	})

	gg.It("keeps the advertised address on a pass_through miss and warns once", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"10.88.0.7:3000": "gateway.example.com:30001"})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		logs := captureWarnLogs()
		parser := peerListParser{
			buf:     []byte(twoPeerReply),
			cluster: cluster,
		}

		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(parser.peers[1].hosts).To(gm.HaveLen(1))
		gm.Expect(parser.peers[1].hosts[0].Name).To(gm.Equal("10.88.0.8"))
		gm.Expect(parser.peers[1].hosts[0].Port).To(gm.Equal(3200))
		gm.Expect(logs.msgs).To(gm.HaveLen(1))
		gm.Expect(logs.msgs[0]).To(gm.And(
			gm.ContainSubstring("BB9020011AC4202"),
			gm.ContainSubstring("10.88.0.8:3200"),
			gm.ContainSubstring("Connecting to the advertised address"),
		))
	})

	gg.It("warns once per node and advertised address on a pass_through miss, keeps that address, and warns again after a move", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{
			"10.0.0.1:3000": "lb.example.com:31001",
			"10.0.0.2:3001": "lb.example.com:31002",
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		policy.UseServicesAlternate = true
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)

		logs := captureWarnLogs()
		alt := "5,3000,[[BB9A,,[203.0.113.1:30001]],[BB9B,,[203.0.113.2:30002]]]"

		parser := peerListParser{buf: []byte(alt), cluster: cluster, sourceList: SourceListAlternate}
		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(parser.peers[0].hosts[0].Name).To(gm.Equal("203.0.113.1"))
		gm.Expect(parser.peers[0].hosts[0].Port).To(gm.Equal(30001))
		gm.Expect(parser.peers[1].hosts[0].Name).To(gm.Equal("203.0.113.2"))
		gm.Expect(parser.peers[1].hosts[0].Port).To(gm.Equal(30002))
		gm.Expect(logs.msgs).To(gm.HaveLen(2))
		gm.Expect(logs.msgs[0]).To(gm.And(
			gm.ContainSubstring("port-map"),
			gm.ContainSubstring("BB9A"),
			gm.ContainSubstring("203.0.113.1:30001"),
			gm.ContainSubstring("in the alternate list"),
			gm.ContainSubstring("Connecting to the advertised address"),
		))
		gm.Expect(logs.msgs[1]).To(gm.And(
			gm.ContainSubstring("port-map"),
			gm.ContainSubstring("BB9B"),
			gm.ContainSubstring("203.0.113.2:30002"),
			gm.ContainSubstring("in the alternate list"),
			gm.ContainSubstring("Connecting to the advertised address"),
		))

		again := peerListParser{buf: []byte(alt), cluster: cluster, sourceList: SourceListAlternate}
		gm.Expect(again.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(logs.msgs).To(gm.HaveLen(2))

		moved := strings.ReplaceAll(alt, "203.0.113.2:30002", "203.0.113.2:30003")
		changed := peerListParser{buf: []byte(moved), cluster: cluster, sourceList: SourceListAlternate}
		gm.Expect(changed.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(changed.peers[1].hosts[0].Port).To(gm.Equal(30003))
		gm.Expect(logs.msgs).To(gm.HaveLen(3))
		gm.Expect(logs.msgs[2]).To(gm.And(
			gm.ContainSubstring("BB9B"),
			gm.ContainSubstring("203.0.113.2:30003"),
		))
	})

	gg.It("does not warn on a pass_through miss when translate is called without the cluster miss map", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{
			"10.0.0.1:3000": "lb.example.com:31001",
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		logs := captureWarnLogs()
		host, err := translate(translator, TranslationMissPassThrough, Endpoint{
			NodeName: "BB9A", Host: "203.0.113.1", Port: 30001, SourceList: SourceListAlternate,
		}, nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(host.Name).To(gm.Equal("203.0.113.1"))
		gm.Expect(logs.msgs).To(gm.BeEmpty())
	})

	gg.It("does not warn when a user-set translator finds a mapping for the advertised address", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"203.0.113.1:30001": "lb.example.com:31001"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		logs := captureWarnLogs()

		parser := peerListParser{buf: []byte("5,3000,[[BB9A,,[203.0.113.1:30001]]]"), cluster: cluster}
		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(parser.peers[0].hosts[0].Name).To(gm.Equal("lb.example.com"))
		gm.Expect(parser.peers[0].hosts[0].Port).To(gm.Equal(31001))
		gm.Expect(logs.msgs).To(gm.BeEmpty())
	})

	gg.It("drops a strict miss and logs the mapping error on every tend, not the pass_through warning", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.0.9:3000": "lb.example.com:31009"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		policy.TranslationOnMiss = TranslationMissStrict
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		logs := captureWarnLogs()
		peers := "5,3000,[[BB9A,,[203.0.113.1:30001]]]"

		for range 2 {
			parser := peerListParser{buf: []byte(peers), cluster: cluster}
			gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
			gm.Expect(parser.peers[0].hosts).To(gm.BeEmpty())
		}
		gm.Expect(logs.msgs).To(gm.HaveLen(2))
		for _, msg := range logs.msgs {
			gm.Expect(msg).To(gm.ContainSubstring("Add a mapping"))
			gm.Expect(msg).ToNot(gm.ContainSubstring("Connecting to the advertised address"))
		}
	})

	gg.It("warns again when the same node advertises a different address, including when it changes back", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.0.9:3000": "lb.example.com:31009"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		logs := captureWarnLogs()

		for _, address := range []string{"203.0.113.1:30001", "203.0.113.1:30002", "203.0.113.1:30001", "203.0.113.1:30001"} {
			parser := peerListParser{buf: []byte("5,3000,[[BB9A,,[" + address + "]]]"), cluster: cluster}
			gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		}
		gm.Expect(logs.msgs).To(gm.HaveLen(3))
		for i, address := range []string{"203.0.113.1:30001", "203.0.113.1:30002", "203.0.113.1:30001"} {
			gm.Expect(logs.msgs[i]).To(gm.And(
				gm.ContainSubstring("BB9A"),
				gm.ContainSubstring(address),
				gm.ContainSubstring("Connecting to the advertised address"),
			))
		}
	})

	gg.It("warns once per node when concurrent tends miss the same advertised addresses", func() {
		translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.0.9:3000": "lb.example.com:31009"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		policy := NewClientPolicy()
		policy.AddressTranslator = translator
		stored := new(atomic.Pointer[ClientPolicy])
		stored.Store(policy)
		cluster := &Cluster{clientPolicy: stored}
		cluster.setAddressTranslator(policy)
		logs := captureWarnLogs()
		const workers, tends = 8, 50
		shared := Endpoint{NodeName: "BBFF", Host: "203.0.113.255", Port: 30255}

		var wg sync.WaitGroup
		for worker := range workers {
			wg.Add(1)
			go func() {
				defer gg.GinkgoRecover()
				defer wg.Done()
				own := Endpoint{NodeName: fmt.Sprintf("BB%02X", worker), Host: "203.0.113.1", Port: 30001 + worker}
				for range tends {
					for _, advertised := range []Endpoint{own, shared} {
						host, err := cluster.translate(advertised.NodeName, advertised.TLSName, advertised.Host, advertised.Port)
						gm.Expect(err).ToNot(gm.HaveOccurred())
						gm.Expect(host.Port).To(gm.Equal(advertised.Port))
					}
				}
			}()
		}
		wg.Wait()

		gm.Expect(logs.msgs).To(gm.HaveLen(workers + 1))
		gm.Expect(logs.msgs).To(gm.ContainElement(gm.ContainSubstring("node BBFF")))
	})

	gg.DescribeTable("a pass_through miss warns only for a user-set translator",
		func(policyFn func() *ClientPolicy, warnings int) {
			policy := policyFn()
			stored := new(atomic.Pointer[ClientPolicy])
			stored.Store(policy)
			cluster := &Cluster{clientPolicy: stored}
			cluster.setAddressTranslator(policy)
			logs := captureWarnLogs()

			parser := peerListParser{buf: []byte("5,3000,[[BB9A,,[203.0.113.1:30001]]]"), cluster: cluster}
			gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
			gm.Expect(parser.peers[0].hosts[0].Name).To(gm.Equal("203.0.113.1"))
			gm.Expect(parser.peers[0].hosts[0].Port).To(gm.Equal(30001))
			gm.Expect(logs.msgs).To(gm.HaveLen(warnings))
			for _, msg := range logs.msgs {
				gm.Expect(msg).To(gm.And(
					gm.ContainSubstring("port-map"),
					gm.ContainSubstring("node BB9A"),
					gm.ContainSubstring("203.0.113.1:30001"),
					gm.ContainSubstring("in the standard list"),
					gm.ContainSubstring("Connecting to the advertised address"),
				))
			}
		},
		gg.Entry("default policy with no AddressTranslator stays silent", func() *ClientPolicy { return NewClientPolicy() }, 0),
		gg.Entry("IpMap without the advertised address stays silent", func() *ClientPolicy {
			policy := NewClientPolicy()
			policy.IpMap = map[string]string{"10.0.0.9": "198.51.100.9"}
			return policy
		}, 0),
		gg.Entry("empty IpMap stays silent", func() *ClientPolicy {
			policy := NewClientPolicy()
			policy.IpMap = map[string]string{}
			return policy
		}, 0),
		gg.Entry("AddressTranslator set in code warns once", func() *ClientPolicy {
			translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.0.9:3000": "lb.example.com:31009"})
			gm.Expect(err).ToNot(gm.HaveOccurred())
			policy := NewClientPolicy()
			policy.AddressTranslator = translator
			return policy
		}, 1),
		gg.Entry("YAML translation block warns once", func() *ClientPolicy {
			return staticPolicyFromYaml(`
static:
  client:
    translation:
      provider: port-map
      port_map:
        10.0.0.9:3000: lb.example.com:31009
`)
		}, 1),
	)

	gg.It("names a peer whose alternate address list is empty", func() {
		parser := peerListParser{
			buf:        []byte("7,3000,[[BB9010011AC4202,,[10.88.0.7]],[BB9020011AC4202,,[]]]"),
			sourceList: SourceListAlternate,
		}

		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())
		gm.Expect(parser.missingAlternates).To(gm.Equal([]string{"BB9020011AC4202"}))
	})

	gg.It("names a peer missing from the alternate list entirely", func() {
		parser := peerListParser{
			buf:        []byte("7,3000,[[BB9010011AC4202,,[10.88.0.7]]]"),
			sourceList: SourceListAlternate,
		}
		gm.Expect(parser.Parse()).ToNot(gm.HaveOccurred())

		parser.addMissingAlternates("7,3000,[[BB9010011AC4202,,[172.1.1.1]],[BB9030011AC4202,,[172.1.1.2]]]")

		gm.Expect(parser.missingAlternates).To(gm.Equal([]string{"BB9030011AC4202"}))
	})
})

func translationCluster(translator AddressTranslator) *Cluster {
	policy := NewClientPolicy()
	policy.AddressTranslator = translator
	stored := new(atomic.Pointer[ClientPolicy])
	stored.Store(policy)
	cluster := &Cluster{clientPolicy: stored}
	cluster.setAddressTranslator(policy)
	return cluster
}

func translationClusterAlternate(translator AddressTranslator) *Cluster {
	cluster := translationCluster(translator)
	cluster.clientPolicy.Load().UseServicesAlternate = true
	return cluster
}

type warnLogCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (capture *warnLogCapture) Printf(format string, v ...any) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.msgs = append(capture.msgs, fmt.Sprintf(format, v...))
}

func captureWarnLogs() *warnLogCapture {
	capture := &warnLogCapture{}
	previous := logger.Logger.Logger
	logger.Logger.SetLogger(capture)
	logger.Logger.SetLevel(logger.WARNING)
	gg.DeferCleanup(func() {
		logger.Logger.SetLogger(previous)
		if lookupFlag("debug", "false") == "true" {
			logger.Logger.SetLevel(logger.DEBUG)
		} else {
			logger.Logger.SetLevel(logger.OFF)
		}
	})
	return capture
}

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
	if p.cluster == nil || p.cluster.addressTranslator == nil {
		t.Fatal("parsePeers did not wire an address translator into the parser")
	}
	translated, found := p.cluster.addressTranslator.Translate(Endpoint{Host: "10.88.0.7", Port: 3000})
	if !found || translated.Name+":"+strconv.Itoa(translated.Port) != wanted["10.88.0.7"] {
		t.Fatalf("parsePeers did not wire ClientPolicy.IpMap into the parser: got %v (found %v), want %v", translated, found, wanted)
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

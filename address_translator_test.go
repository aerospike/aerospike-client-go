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

package aerospike

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Address translators", gg.Label("discovery"), func() {

	// The static-map translator carries the IpMap contract. Every IpMap
	// value shape is pinned here with its expected address; a miss with the
	// default pass-through handling keeps the advertised address unchanged.
	gg.Describe("static-map keeps the IpMap contract", func() {

		type scenario struct {
			name         string
			addressMap   map[string]string
			advertised   string
			expectedHost string
			expectedPort int
		}

		scenarios := []scenario{
			{"nil map", nil, "10.88.0.7:3100", "10.88.0.7", 3100},
			{"host with no entry", map[string]string{"9.9.9.9": "1.1.1.1"}, "10.88.0.7:3100", "10.88.0.7", 3100},
			{"empty mapping value", map[string]string{"10.88.0.7": ""}, "10.88.0.7:3100", "10.88.0.7", 3100},
			{"host-only value", map[string]string{"10.88.0.7": "127.0.0.1"}, "10.88.0.7:3100", "127.0.0.1", 3100},
			{"host and port value", map[string]string{"10.88.0.7": "127.0.0.1:3100"}, "10.88.0.7:3100", "127.0.0.1", 3100},
			{"key carrying a port never matches", map[string]string{"10.88.0.7:3100": "127.0.0.1:9999"}, "10.88.0.7:3100", "10.88.0.7", 3100},
			{"IPv6 advertised address keyed without brackets", map[string]string{"fe80::1": "127.0.0.1:3100"}, "[fe80::1]:3000", "127.0.0.1", 3100},
			{"one endpoint name with distinct ports", map[string]string{"10.0.0.1": "nlb.example.com:4001"}, "10.0.0.1:3000", "nlb.example.com", 4001},
			{"bare host value without a colon", map[string]string{"10.88.0.7": "reachable.internal"}, "10.88.0.7:3100", "reachable.internal", 3100},
			{"bracketed IPv6 value", map[string]string{"10.88.0.7": "[fe80::1]"}, "10.88.0.7:3100", "[fe80::1]", 3100},
			{"IPv6 literal value ending in digits", map[string]string{"10.88.0.7": "2001:db8:ffff::10"}, "10.88.0.7:3100", "2001:db8:ffff::10", 3100},
			{"IPv6 literal value ending in a hex letter", map[string]string{"10.88.0.7": "2001:db8::a"}, "10.88.0.7:3100", "2001:db8::a", 3100},
		}

		for _, each := range scenarios {
			current := each
			gg.It("resolves "+current.name, func() {
				plainParser := peerListParser{}
				advertised, err := plainParser.ParseHost(current.advertised)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				endpoint := Endpoint{NodeName: "BB9000000000001", Host: advertised.Name, Port: advertised.Port}
				translated, found := NewStaticMapAddressTranslator(current.addressMap).Translate(endpoint)
				if !found {
					translated = Host{Name: advertised.Name, Port: advertised.Port}
				}

				gm.Expect(translated.Name).To(gm.Equal(current.expectedHost))
				gm.Expect(translated.Port).To(gm.Equal(current.expectedPort))
			})
		}

		gg.It("keeps the advertised TLS name on a hit", func() {
			translator := NewStaticMapAddressTranslator(map[string]string{"10.0.0.1": "nlb.example.com:4001"})
			endpoint := Endpoint{NodeName: "BB9000000000001", TLSName: "node-1.cluster.internal", Host: "10.0.0.1", Port: 3000}

			translated, found := translator.Translate(endpoint)

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("nlb.example.com"))
			gm.Expect(translated.Port).To(gm.Equal(4001))
			gm.Expect(translated.TLSName).To(gm.Equal("node-1.cluster.internal"))
		})
	})

	gg.Describe("port-map", func() {

		gg.It("maps an advertised host and port to a different host and port", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.1.10:3000": "gateway.example.com:30001"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			translated, found := translator.Translate(Endpoint{NodeName: "A", Host: "10.0.1.10", Port: 3000})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("gateway.example.com"))
			gm.Expect(translated.Port).To(gm.Equal(30001))
		})

		gg.It("maps by node name when no host and port entry matches", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{"BB9000000000001": "gateway.example.com:30002"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			translated, found := translator.Translate(Endpoint{NodeName: "BB9000000000001", Host: "10.0.1.11", Port: 3000})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("gateway.example.com"))
			gm.Expect(translated.Port).To(gm.Equal(30002))
		})

		gg.It("prefers the host and port entry over the node name entry", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{
				"10.0.1.10:3000":  "by-endpoint.example.com:30001",
				"BB9000000000001": "by-node.example.com:30002",
			})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			translated, found := translator.Translate(Endpoint{NodeName: "BB9000000000001", Host: "10.0.1.10", Port: 3000})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("by-endpoint.example.com"))
		})

		gg.It("handles bracketed IPv6 hosts in keys and values", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{"[2001:db8::1]:3000": "[2001:db8:ffff::10]:30001"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			translated, found := translator.Translate(Endpoint{NodeName: "A", Host: "2001:db8::1", Port: 3000})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("2001:db8:ffff::10"))
			gm.Expect(translated.Port).To(gm.Equal(30001))
		})

		gg.It("misses an endpoint with no matching entry", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.1.10:3000": "gateway.example.com:30001"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, found := translator.Translate(Endpoint{NodeName: "B", Host: "10.0.1.99", Port: 3000})

			gm.Expect(found).To(gm.BeFalse())
		})

		gg.It("keeps the advertised TLS name on a hit", func() {
			translator, err := NewPortMapAddressTranslator(map[string]string{"10.0.1.10:3000": "gateway.example.com:30001"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			translated, found := translator.Translate(Endpoint{NodeName: "A", TLSName: "node-a.cluster.internal", Host: "10.0.1.10", Port: 3000})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.TLSName).To(gm.Equal("node-a.cluster.internal"))
		})

		gg.DescribeTable("rejects an invalid map at construction",
			func(portMap map[string]string, reason string) {
				_, err := NewPortMapAddressTranslator(portMap)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Error()).To(gm.ContainSubstring(reason))
			},
			gg.Entry("nil map", nil, "map is nil"),
			gg.Entry("empty key", map[string]string{"": "gateway.example.com:30001"}, "key is empty"),
			gg.Entry("value without a port", map[string]string{"10.0.1.10:3000": "gateway.example.com"}, "is not host:port"),
			gg.Entry("unbracketed IPv6 value", map[string]string{"10.0.1.10:3000": "2001:db8::1:30001"}, "not in brackets"),
			gg.Entry("unparseable port", map[string]string{"10.0.1.10:3000": "gateway.example.com:none"}, "has an invalid port"),
			gg.Entry("port out of range", map[string]string{"10.0.1.10:3000": "gateway.example.com:70000"}, "out of range"),
			gg.Entry("empty host in value", map[string]string{"10.0.1.10:3000": ":30001"}, "has an empty host"),
		)

		gg.It("rejects two keys naming the same advertised endpoint", func() {
			// The bracketed and bare spellings parse to the same endpoint.
			_, err := NewPortMapAddressTranslator(map[string]string{
				"10.0.1.10:3000":   "gateway.example.com:30001",
				"[10.0.1.10]:3000": "gateway.example.com:30002",
			})

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("duplicates another key"))
		})
	})

	gg.Describe("pass-through", func() {

		gg.It("uses every advertised address unchanged", func() {
			translated, found := NewPassThroughAddressTranslator().Translate(
				Endpoint{NodeName: "A", TLSName: "node-a.cluster.internal", Host: "as-endpoint.test", Port: 30001})

			gm.Expect(found).To(gm.BeTrue())
			gm.Expect(translated.Name).To(gm.Equal("as-endpoint.test"))
			gm.Expect(translated.Port).To(gm.Equal(30001))
			gm.Expect(translated.TLSName).To(gm.Equal("node-a.cluster.internal"))
		})
	})

	gg.Describe("translation on the hot path", func() {

		gg.It("allocates nothing per call", func() {
			staticMap := NewStaticMapAddressTranslator(map[string]string{"10.0.0.1": "nlb.example.com:4001"})
			portMap, err := NewPortMapAddressTranslator(map[string]string{"10.0.1.10:3000": "gateway.example.com:30001"})
			gm.Expect(err).ToNot(gm.HaveOccurred())
			passThrough := NewPassThroughAddressTranslator()

			hit := Endpoint{NodeName: "A", Host: "10.0.0.1", Port: 3000}
			miss := Endpoint{NodeName: "B", Host: "10.9.9.9", Port: 3000}
			portMapped := Endpoint{NodeName: "A", Host: "10.0.1.10", Port: 3000}

			gm.Expect(testing.AllocsPerRun(1000, func() { staticMap.Translate(hit) })).To(gm.BeZero())
			gm.Expect(testing.AllocsPerRun(1000, func() { staticMap.Translate(miss) })).To(gm.BeZero())
			gm.Expect(testing.AllocsPerRun(1000, func() { portMap.Translate(portMapped) })).To(gm.BeZero())
			gm.Expect(testing.AllocsPerRun(1000, func() { passThrough.Translate(hit) })).To(gm.BeZero())
		})
	})
})

// Translate is in-memory and its p99 stays under 1 ms. Timed outside the
// discovery Ginkgo suite because that suite runs with -race.

const (
	translateLatencyWarmup        = 200_000
	translateLatencyRounds        = 7
	translateLatencyCallsPerRound = 20_000
	translateLatencyTarget        = time.Millisecond
)

var translateLatencyEndpoints = []Endpoint{
	{NodeName: "BB9A", Host: "10.0.0.1", Port: 3000, SourceList: SourceListStandard},
	{NodeName: "BB9B", TLSName: "tls1", Host: "10.0.0.2", Port: 3000, SourceList: SourceListStandard},
	{NodeName: "BB9C", Host: "10.0.0.3", Port: 3000, SourceList: SourceListAlternate},
	{NodeName: "BB9D", Host: "2001:db8::1", Port: 3000, SourceList: SourceListStandard},
}

func TestTranslateLatency(t *testing.T) {
	t.Run("static_map", func(t *testing.T) {
		measureTranslateLatency(t, "static-map", NewStaticMapAddressTranslator(map[string]string{
			"10.0.0.1": "192.168.1.1",
			"10.0.0.2": "192.168.1.2",
		}), 2)
	})

	t.Run("port_map", func(t *testing.T) {
		translator, err := NewPortMapAddressTranslator(map[string]string{
			"10.0.0.1:3000":      "lb.example.com:30001",
			"[2001:db8::1]:3000": "lb.example.com:30002",
			"BB9B":               "lb.example.com:30003",
		})
		if err != nil {
			t.Fatal(err)
		}
		measureTranslateLatency(t, "port-map", translator, 3)
	})

	t.Run("pass_through", func(t *testing.T) {
		measureTranslateLatency(t, "pass-through", NewPassThroughAddressTranslator(), 4)
	})

	t.Run("spin_fails", func(t *testing.T) {
		rounds := make([][]time.Duration, translateLatencyRounds)
		for i := range rounds {
			rounds[i] = make([]time.Duration, 100)
			timeTranslateCalls(spinningAddressTranslator{spin: 2 * time.Millisecond}, rounds[i])
		}
		_, err := translateLatencyP99("spin-2ms", rounds)
		if err == nil {
			t.Fatal("spin-2ms should exceed 1 ms p99")
		}
		if !strings.HasPrefix(err.Error(), "spin-2ms median round p99 ") {
			t.Fatal(err)
		}
	})
}

func measureTranslateLatency(t *testing.T, name string, translator AddressTranslator, hitsPerCycle int) {
	t.Helper()
	hits := timeTranslateCalls(translator, make([]time.Duration, translateLatencyWarmup))
	rounds := make([][]time.Duration, translateLatencyRounds)
	for i := range rounds {
		rounds[i] = make([]time.Duration, translateLatencyCallsPerRound)
		hits += timeTranslateCalls(translator, rounds[i])
	}
	total := translateLatencyWarmup + translateLatencyRounds*translateLatencyCallsPerRound
	wantHits := total / len(translateLatencyEndpoints) * hitsPerCycle
	if hits != wantHits {
		t.Fatalf("%s hits %d, want %d", name, hits, wantHits)
	}
	p99, err := translateLatencyP99(name, rounds)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("translate() %s: median round p99 %d ns", name, p99)
}

func timeTranslateCalls(translator AddressTranslator, samples []time.Duration) int {
	hits := 0
	for i := range samples {
		advertised := translateLatencyEndpoints[i%len(translateLatencyEndpoints)]
		begin := time.Now()
		_, ok := translator.Translate(advertised)
		samples[i] = time.Since(begin)
		if ok {
			hits++
		}
	}
	return hits
}

func translateLatencyP99(name string, rounds [][]time.Duration) (time.Duration, error) {
	p99s := make([]time.Duration, len(rounds))
	for i, samples := range rounds {
		sorted := slices.Clone(samples)
		slices.Sort(sorted)
		p99s[i] = sorted[len(sorted)*99/100]
	}
	listed := fmt.Sprint(p99s)
	slices.Sort(p99s)
	p99 := p99s[len(p99s)/2]
	if p99 >= translateLatencyTarget {
		return p99, fmt.Errorf("%s median round p99 %d ns, round p99s %s ns", name, p99, listed)
	}
	return p99, nil
}

type spinningAddressTranslator struct {
	spin time.Duration
}

func (translator spinningAddressTranslator) Translate(Endpoint) (Host, bool) {
	end := time.Now().Add(translator.spin)
	for time.Now().Before(end) {
	}
	return Host{}, false
}

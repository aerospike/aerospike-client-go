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
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// DNS seed refresher tests cover the Java DnsSeedRefresherTest intent through
// newSeedRefresher and a loopback A-record server. Timeout-versus-deadline
// construction is already in seed_refresher_test.go.
var _ = gg.Describe("DNS seed refresher", gg.Label("discovery"), func() {
	var server *aRecordDNSServer

	gg.BeforeEach(func() {
		var err error
		server, err = startARecordDNSServer()
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		if server != nil {
			server.close()
		}
	})

	gg.It("names the provider dns", func() {
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Servers: []string{server.address()},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(seedProviderDisplayName(provider)).To(gm.Equal(seedProviderNameDns))
		provider.Close()
	})

	gg.It("fails start with SEED_DISCOVERY_FAILED when every name is NXDOMAIN and there are no constructor seeds", func() {
		refresher, _ := newDnsTendRefresher(server, DnsSeedCandidateProviderConfig{}, nil)

		seeds, err := refresher.start()

		gm.Expect(seeds).To(gm.BeNil())
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Seed provider dns failed and no seed host is left"))
		gm.Expect(err.Error()).To(gm.ContainSubstring("DNS seed discovery failed for every name: a.example.com: NXDOMAIN; b.example.com: NXDOMAIN"))
	})

	gg.It("starts from constructor seeds when dns init fails", func() {
		staticSeeds := []*Host{NewHost("127.0.0.1", 1)}
		refresher, _ := newDnsTendRefresher(server, DnsSeedCandidateProviderConfig{}, staticSeeds)

		seeds, err := refresher.start()

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
	})

	gg.It("fails start with SEED_DISCOVERY_FAILED in thread mode when every name is NXDOMAIN and there are no constructor seeds", func() {
		refresher, _ := newDnsThreadRefresher(server, DnsSeedCandidateProviderConfig{}, nil)
		defer refresher.close()

		seeds, err := refresher.start()

		gm.Expect(seeds).To(gm.BeNil())
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Seed provider dns failed and no seed host is left"))
		gm.Expect(err.Error()).To(gm.ContainSubstring("DNS seed discovery failed for every name: a.example.com: NXDOMAIN; b.example.com: NXDOMAIN"))
	})

	gg.It("starts from constructor seeds in thread mode when dns init fails", func() {
		staticSeeds := []*Host{NewHost("127.0.0.1", 1)}
		refresher, _ := newDnsThreadRefresher(server, DnsSeedCandidateProviderConfig{}, staticSeeds)
		defer refresher.close()

		seeds, err := refresher.start()

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
	})

	gg.It("constructs in thread mode when timeout_ms exceeds refresh_timeout_ms", func() {
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Servers: []string{server.address()},
			Timeout: 400 * time.Millisecond,
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond

		_, verr := newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(verr).ToNot(gm.HaveOccurred())
		provider.Close()
	})

	gg.It("starts within refresh_timeout_ms when dns timeout_ms is larger", func() {
		silent, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer silent.Close()

		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Servers: []string{silent.LocalAddr().String()},
			Timeout: 400 * time.Millisecond,
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond
		refresher, verr := newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(verr).ToNot(gm.HaveOccurred())
		defer refresher.close()

		began := time.Now()
		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(time.Since(began)).To(gm.BeNumerically("<", 250*time.Millisecond))
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
	})

	gg.It("bounds the first thread-mode lookup by refresh_timeout_ms, not timeout_ms", func() {
		server.a("a.example.com", "10.0.0.2").slow(200 * time.Millisecond)
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Servers: []string{server.address()},
			Timeout: 80 * time.Millisecond,
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 2 * time.Second
		refresher, verr := newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(verr).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1", "10.0.0.2:3000"}))
	})

	gg.It("uses timeout_ms on later thread-mode lookups, not refresh_timeout_ms", func() {
		server.a("a.example.com", "10.0.0.2")
		provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Servers: []string{server.address()},
			Timeout: 80 * time.Millisecond,
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 2 * time.Second
		recorder := &publishRecorder{}
		refresher, verr := newSeedRefresher(policy, nil, recorder.publish)
		gm.Expect(verr).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.2:3000"}))

		server.clear()
		server.a("a.example.com", "10.0.0.3").slow(200 * time.Millisecond)
		refresher.requestUrgentRefresh()
		gm.Consistently(func() int { return recorder.count() }, "500ms", "20ms").Should(gm.BeZero())
	})

	gg.It("bounds the first tend-mode lookup by timeout_ms, not refresh_timeout_ms", func() {
		server.a("a.example.com", "10.0.0.2").slow(200 * time.Millisecond)
		staticSeeds := []*Host{NewHost("127.0.0.1", 1)}
		refresher, _ := newDnsTendRefresher(server, DnsSeedCandidateProviderConfig{
			Names:   []string{"a.example.com"},
			Timeout: 80 * time.Millisecond,
		}, staticSeeds)

		began := time.Now()
		seeds, err := refresher.start()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(time.Since(began)).To(gm.BeNumerically("<", 190*time.Millisecond))
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
	})

	gg.It("returns IP literals from a successful init lookup", func() {
		server.a("a.example.com", "10.0.0.2")
		staticSeeds := []*Host{NewHost("127.0.0.1", 1)}
		refresher, _ := newDnsTendRefresher(server, DnsSeedCandidateProviderConfig{
			Names: []string{"a.example.com"},
			Port:  2,
		}, staticSeeds)

		seeds, err := refresher.start()

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1", "10.0.0.2:2"}))
		for _, host := range seeds {
			gm.Expect(host.Name).ToNot(gm.Equal("a.example.com"))
			gm.Expect(net.ParseIP(host.Name)).ToNot(gm.BeNil())
		}
	})

	gg.It("keeps the previous seed list when every name becomes NXDOMAIN and recovers on the next good lookup", func() {
		server.a("a.example.com", "10.0.0.2").a("b.example.com", "10.0.0.3")
		refresher, recorder := newDnsTendRefresher(server, DnsSeedCandidateProviderConfig{}, nil)

		seeds, err := refresher.start()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.2:3000", "10.0.0.3:3000"}))
		gm.Expect(recorder.count()).To(gm.BeZero())

		server.clear()
		refresher.tendTick(1)

		gm.Expect(recorder.count()).To(gm.BeZero())
		gm.Expect(refresher.overrunCount).To(gm.Equal(0))
		gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())

		server.a("a.example.com", "10.0.0.4")
		refresher.tendTick(2)

		gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"10.0.0.4:3000"}))
		gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())
	})
})

func newDnsTendRefresher(server *aRecordDNSServer, config DnsSeedCandidateProviderConfig, staticSeeds []*Host) (*seedRefresher, *publishRecorder) {
	return newDnsRefresher(server, config, staticSeeds, DiscoveryExecutionTend)
}

func newDnsThreadRefresher(server *aRecordDNSServer, config DnsSeedCandidateProviderConfig, staticSeeds []*Host) (*seedRefresher, *publishRecorder) {
	return newDnsRefresher(server, config, staticSeeds, DiscoveryExecutionThread)
}

func newDnsRefresher(server *aRecordDNSServer, config DnsSeedCandidateProviderConfig, staticSeeds []*Host, mode DiscoveryExecutionMode) (*seedRefresher, *publishRecorder) {
	if len(config.Names) == 0 {
		config.Names = []string{"a.example.com", "b.example.com"}
	}
	if len(config.Servers) == 0 {
		config.Servers = []string{server.address()}
	}

	provider, err := NewDnsSeedCandidateProvider(config)
	gm.Expect(err).ToNot(gm.HaveOccurred())

	policy := NewClientPolicy()
	policy.SeedProvider = provider
	policy.DiscoveryExecutionMode = mode
	if mode == DiscoveryExecutionTend {
		policy.DiscoveryRefreshInterval = time.Second
		policy.TendInterval = time.Second
	} else {
		policy.DiscoveryRefreshInterval = time.Hour
	}

	recorder := &publishRecorder{}
	refresher, err := newSeedRefresher(policy, staticSeeds, recorder.publish)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return refresher, recorder
}

const (
	aRecordTypeA    = 1
	aRecordNOERROR  = 0
	aRecordNXDOMAIN = 3
)

// aRecordDNSServer is a loopback UDP DNS server that answers A queries.
// Unknown names return NXDOMAIN. AAAA for a name that has A records is
// NOERROR with no answers.
type aRecordDNSServer struct {
	conn    *net.UDPConn
	lock    sync.Mutex
	answers map[string][][]byte
	delay   time.Duration
	done    chan struct{}
}

func startARecordDNSServer() (*aRecordDNSServer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	server := &aRecordDNSServer{
		conn:    conn,
		answers: map[string][][]byte{},
		done:    make(chan struct{}),
	}
	go server.serve()
	return server, nil
}

func (server *aRecordDNSServer) address() string {
	return server.conn.LocalAddr().String()
}

func (server *aRecordDNSServer) a(name string, addresses ...string) *aRecordDNSServer {
	for _, address := range addresses {
		ip := net.ParseIP(address).To4()
		rr := append(encodeDNSName(name), stubAMeta(ip)...)
		key := aRecordKey(name, aRecordTypeA)
		server.lock.Lock()
		server.answers[key] = append(server.answers[key], rr)
		server.lock.Unlock()
	}
	return server
}

// slow delays every later answer by delay.
func (server *aRecordDNSServer) slow(delay time.Duration) *aRecordDNSServer {
	server.lock.Lock()
	server.delay = delay
	server.lock.Unlock()
	return server
}

func (server *aRecordDNSServer) clear() {
	server.lock.Lock()
	server.answers = map[string][][]byte{}
	server.lock.Unlock()
}

func (server *aRecordDNSServer) close() {
	_ = server.conn.Close()
	<-server.done
}

func (server *aRecordDNSServer) serve() {
	defer close(server.done)
	buf := make([]byte, 2048)
	for {
		n, remote, err := server.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		msg := append([]byte(nil), buf[:n]...)
		qname, qtype, qstart, qend, ok := parseARecordQuestion(msg)
		if !ok {
			continue
		}
		key := aRecordKey(qname, qtype)
		server.lock.Lock()
		answers := append([][]byte(nil), server.answers[key]...)
		rcode := aRecordNOERROR
		if len(answers) == 0 && !server.hasNameLocked(qname) {
			rcode = aRecordNXDOMAIN
		}
		delay := server.delay
		server.lock.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}

		header := make([]byte, 12)
		copy(header[0:2], msg[0:2])
		flags := uint16(0x8400 | 0x0080 | rcode)
		if len(msg) >= 4 {
			flags |= binary.BigEndian.Uint16(msg[2:4]) & 0x0100
		}
		binary.BigEndian.PutUint16(header[2:4], flags)
		binary.BigEndian.PutUint16(header[4:6], 1)
		if rcode != aRecordNOERROR {
			answers = nil
		}
		binary.BigEndian.PutUint16(header[6:8], uint16(len(answers)))

		out := append(header, msg[qstart:qend]...)
		for _, rr := range answers {
			out = append(out, rr...)
		}
		_, _ = server.conn.WriteToUDP(out, remote)
	}
}

func (server *aRecordDNSServer) hasNameLocked(name string) bool {
	prefix := aRecordKey(name, 0)
	prefix = prefix[:strings.LastIndexByte(prefix, '/')+1]
	for key := range server.answers {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func aRecordKey(name string, qtype int) string {
	n := strings.ToLower(name)
	if !strings.HasSuffix(n, ".") {
		n += "."
	}
	return n + "/" + strconv.Itoa(qtype)
}

func stubAMeta(ip net.IP) []byte {
	meta := make([]byte, 10)
	binary.BigEndian.PutUint16(meta[0:2], aRecordTypeA)
	binary.BigEndian.PutUint16(meta[2:4], 1)
	binary.BigEndian.PutUint32(meta[4:8], 60)
	binary.BigEndian.PutUint16(meta[8:10], uint16(len(ip)))
	return append(meta, ip...)
}

func encodeDNSName(name string) []byte {
	var out []byte
	for _, label := range strings.Split(name, ".") {
		if label == "" {
			continue
		}
		out = append(out, byte(len(label)))
		out = append(out, label...)
	}
	return append(out, 0)
}

func parseARecordQuestion(msg []byte) (qname string, qtype, qstart, qend int, ok bool) {
	if len(msg) < 12 {
		return "", 0, 0, 0, false
	}
	name, offset, parsed := decodeDNSName(msg, 12)
	if !parsed || offset+4 > len(msg) {
		return "", 0, 0, 0, false
	}
	return name, int(binary.BigEndian.Uint16(msg[offset : offset+2])), 12, offset + 4, true
}

func decodeDNSName(msg []byte, offset int) (string, int, bool) {
	var labels []string
	jumped := false
	end := offset
	for hops := 0; hops < 20; hops++ {
		if offset >= len(msg) {
			return "", 0, false
		}
		length := int(msg[offset])
		if length == 0 {
			offset++
			if !jumped {
				end = offset
			}
			return strings.Join(labels, ".") + ".", end, true
		}
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(msg) {
				return "", 0, false
			}
			if !jumped {
				end = offset + 2
				jumped = true
			}
			offset = int(binary.BigEndian.Uint16(msg[offset:offset+2]) & 0x3FFF)
			continue
		}
		offset++
		if offset+length > len(msg) {
			return "", 0, false
		}
		labels = append(labels, string(msg[offset:offset+length]))
		offset += length
	}
	return "", 0, false
}

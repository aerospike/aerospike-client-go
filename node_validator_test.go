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
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

var _ = gg.Describe("Seed validation against a scripted server", gg.Label("discovery"), func() {

	var server *fakeInfoServer

	gg.BeforeEach(func() {
		var err error
		server, err = newFakeInfoServer()
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		server.close()
	})

	newValidationCluster := func(policy *ClientPolicy) *Cluster {
		policy.Timeout = 2 * time.Second
		return validationCluster(policy)
	}

	gg.It("fails a seed with no alternate address when UseServicesAlternate is set", func() {
		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "")

		validator := &nodeValidator{}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("UseServicesAlternate"))
		gm.Expect(err.Error()).To(gm.ContainSubstring("FAKE000000000001"))
	})

	gg.It("keeps the seed itself when its advertised service address is a strict miss", func() {
		translator, terr := NewPortMapAddressTranslator(map[string]string{"10.9.9.9:999": "unused.example.com:1"})
		gm.Expect(terr).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		policy.AddressTranslator = translator
		policy.TranslationOnMiss = TranslationMissStrict
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "10.88.0.7:3100")

		validator := &nodeValidator{}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(validator.name).To(gm.Equal("FAKE000000000001"))
		gm.Expect(validator.primaryHost.Name).To(gm.Equal(server.host().Name))
		gm.Expect(validator.primaryHost.Port).To(gm.Equal(server.host().Port))
	})

	gg.It("replaces a load balancer seed with its translated advertised address", func() {
		serverAddress := server.host()
		translator, terr := NewPortMapAddressTranslator(map[string]string{
			"10.88.0.7:3100": serverAddress.Name + ":" + strconv.Itoa(serverAddress.Port),
		})
		gm.Expect(terr).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		policy.AddressTranslator = translator
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "10.88.0.7:3100")

		validator := &nodeValidator{detectLoadBalancer: true}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(validator.primaryHost.Name).To(gm.Equal(serverAddress.Name))
		gm.Expect(validator.primaryHost.Port).To(gm.Equal(serverAddress.Port))
	})
})

// fakeInfoServer is a loopback TCP server speaking the info protocol. Tests
// script its answers per info command without a real cluster.
type fakeInfoServer struct {
	listener net.Listener

	repliesLock sync.Mutex
	replies     map[string]string
}

func newFakeInfoServer() (*fakeInfoServer, error) {
	return newFakeInfoServerOn("127.0.0.1:0")
}

func newFakeInfoServerOn(address string) (*fakeInfoServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	server := &fakeInfoServer{
		listener: listener,
		replies: map[string]string{
			"partition-generation": "1",
			"peers-generation":     "1",
			"build":                "8.1.0.0",
		},
	}
	go server.serve()
	return server, nil
}

func (server *fakeInfoServer) host() *Host {
	address := server.listener.Addr().(*net.TCPAddr)
	return NewHost(address.IP.String(), address.Port)
}

func (server *fakeInfoServer) setReply(command, reply string) {
	server.repliesLock.Lock()
	server.replies[command] = reply
	server.repliesLock.Unlock()
}

func (server *fakeInfoServer) reply(command string) (string, bool) {
	server.repliesLock.Lock()
	defer server.repliesLock.Unlock()
	reply, exists := server.replies[command]
	return reply, exists
}

func (server *fakeInfoServer) close() {
	server.listener.Close()
}

func (server *fakeInfoServer) serve() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		go server.serveConnection(conn)
	}
}

func (server *fakeInfoServer) serveConnection(conn net.Conn) {
	defer conn.Close()

	for {
		var header types.MessageHeader
		if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
			return
		}

		body := make([]byte, header.Length())
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		var response bytes.Buffer
		for _, command := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if command == "" {
				continue
			}
			if reply, exists := server.reply(command); exists {
				response.WriteString(command + "\t" + reply + "\n")
			} else if command == "node" {
				response.WriteString("node\tFAKE000000000001\n")
			}
		}

		message := types.NewMessage(types.MSG_INFO, response.Bytes())
		serialized, err := message.Serialize()
		if err != nil {
			return
		}
		if _, err := conn.Write(serialized); err != nil {
			return
		}
	}
}

func validationCluster(policy *ClientPolicy) *Cluster {
	policyPointer := &atomic.Pointer[ClientPolicy]{}
	policyPointer.Store(policy)
	translator := policy.AddressTranslator
	if translator == nil {
		translator = NewStaticMapAddressTranslator(policy.IpMap)
	}
	return &Cluster{
		clientPolicy:      policyPointer,
		infoPolicy:        InfoPolicy{Timeout: policy.Timeout},
		addressTranslator: translator,
		translatorName:    translatorDisplayName(translator),
		translationOnMiss: policy.TranslationOnMiss,
	}
}

func withLookupHost(t *testing.T, lookup func(string) ([]string, error)) {
	t.Helper()
	saved := lookupHost
	lookupHost = lookup
	t.Cleanup(func() { lookupHost = saved })
}

func withProbeDial(t *testing.T, dial func(network, address string, timeout time.Duration) (net.Conn, error)) {
	t.Helper()
	saved := probeDial
	probeDial = dial
	t.Cleanup(func() { probeDial = saved })
}

func startFakeInfoServer(t *testing.T) *fakeInfoServer {
	t.Helper()
	server, err := newFakeInfoServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.close)
	return server
}

func TestValidateNodeDialsResolvedAddressesTogether(t *testing.T) {
	server := startFakeInfoServer(t)
	live := server.host()
	server.setReply("service-clear-std", net.JoinHostPort(live.Name, strconv.Itoa(live.Port)))

	withLookupHost(t, func(string) ([]string, error) {
		return []string{"192.0.2.1", live.Name}, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 2 * time.Second
	policy.TendInterval = time.Second
	started := time.Now()
	validator := &nodeValidator{}
	err := validator.validateNode(validationCluster(policy), NewHost("db.example.internal", live.Port))
	elapsed := time.Since(started)

	if err != nil {
		t.Fatal(err)
	}
	if validator.primaryHost == nil || validator.primaryHost.Name != live.Name || validator.primaryHost.Port != live.Port {
		t.Fatalf("primary host = %v, want %s", validator.primaryHost, live)
	}
	if validator.name != "FAKE000000000001" {
		t.Fatalf("node name = %s", validator.name)
	}
	// A serial dial would spend Timeout/2 on 192.0.2.1 before reaching the live address.
	if elapsed >= policy.Timeout/2 {
		t.Fatalf("validateNode took %s, serial dial of the dead address is %s", elapsed, policy.Timeout/2)
	}
}

func TestValidateNodeKeepsAdvertisedHostnameOnTheResolvedAddress(t *testing.T) {
	server := startFakeInfoServer(t)
	live := server.host()
	const hostname = "db.example.invalid"
	server.setReply("service-clear-std", net.JoinHostPort(hostname, strconv.Itoa(live.Port)))

	withLookupHost(t, func(string) ([]string, error) {
		return []string{"192.0.2.1", live.Name}, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 2 * time.Second
	policy.TendInterval = time.Second
	host := NewHost(hostname, live.Port)
	host.TLSName = "db-tls"
	started := time.Now()
	validator := &nodeValidator{}
	err := validator.validateNode(validationCluster(policy), host)
	elapsed := time.Since(started)

	if err != nil {
		t.Fatal(err)
	}
	if validator.primaryHost == nil || validator.primaryHost.Name != hostname || validator.primaryHost.Port != live.Port {
		t.Fatalf("primary host = %v, want %s:%d", validator.primaryHost, hostname, live.Port)
	}
	if validator.primaryHost.TLSName != host.TLSName {
		t.Fatalf("primary host TLS name = %q, want %q", validator.primaryHost.TLSName, host.TLSName)
	}
	resolved := validator.resolvedHost
	if resolved == nil || resolved.Name != live.Name || resolved.Port != live.Port {
		t.Fatalf("resolved host = %v, want %s", resolved, live)
	}
	if resolved.TLSName != host.TLSName {
		t.Fatalf("resolved host TLS name = %q, want %q", resolved.TLSName, host.TLSName)
	}
	// Dialing the dead address or the hostname again costs Timeout/2.
	if elapsed >= policy.Timeout/2 {
		t.Fatalf("validateNode took %s, one extra dial is %s", elapsed, policy.Timeout/2)
	}
}

func TestValidateNodeOnASingleAddressHostnameKeepsTheAddress(t *testing.T) {
	server := startFakeInfoServer(t)
	live := server.host()
	const hostname = "db.example.invalid"
	server.setReply("service-clear-std", net.JoinHostPort(hostname, strconv.Itoa(live.Port)))

	withLookupHost(t, func(string) ([]string, error) {
		return []string{live.Name}, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 2 * time.Second
	validator := &nodeValidator{}
	if err := validator.validateNode(validationCluster(policy), NewHost(hostname, live.Port)); err != nil {
		t.Fatal(err)
	}
	if validator.primaryHost == nil || validator.primaryHost.Name != live.Name || validator.primaryHost.Port != live.Port {
		t.Fatalf("primary host = %v, want %s", validator.primaryHost, live)
	}
	if validator.resolvedHost != nil {
		t.Fatalf("resolved host = %v, want nil", validator.resolvedHost)
	}
}

func TestValidateNodeFallsBackWhenTheFirstAddressToAcceptIsNotReady(t *testing.T) {
	second := secondLoopback()
	if second == nil {
		t.Skip("127.0.0.2 cannot be bound locally")
	}
	ready := startFakeInfoServer(t)
	port := ready.host().Port
	starting, err := newFakeInfoServerOn(net.JoinHostPort(second.String(), strconv.Itoa(port)))
	if err != nil {
		t.Skipf("port %d is taken on %s: %s", port, second, err)
	}
	t.Cleanup(starting.close)
	starting.setReply("partition-generation", "-1")

	withLookupHost(t, func(string) ([]string, error) {
		return []string{second.String(), ready.host().Name}, nil
	})
	withProbeDial(t, func(network, address string, timeout time.Duration) (net.Conn, error) {
		if strings.HasPrefix(address, ready.host().Name+":") {
			time.Sleep(50 * time.Millisecond)
		}
		return net.DialTimeout(network, address, timeout)
	})

	policy := NewClientPolicy()
	policy.Timeout = 2 * time.Second
	policy.TendInterval = time.Second
	validator := &nodeValidator{}
	if err := validator.validateNode(validationCluster(policy), NewHost("db.example.internal", port)); err != nil {
		t.Fatal(err)
	}
	if validator.primaryHost == nil || validator.primaryHost.Name != ready.host().Name {
		t.Fatalf("primary host = %v, want %s", validator.primaryHost, ready.host())
	}
}

func TestValidateNodeGivesUpEarlyWhenEveryAddressRefuses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	withLookupHost(t, func(string) ([]string, error) {
		return []string{"127.0.0.1", "127.0.0.1"}, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.TendInterval = 2 * time.Second
	started := time.Now()
	verr := (&nodeValidator{}).validateNode(validationCluster(policy), NewHost("db.example.invalid", port))
	elapsed := time.Since(started)

	if verr == nil || !verr.Matches(types.TIMEOUT) {
		t.Fatalf("error = %v, want TIMEOUT", verr)
	}
	if elapsed >= policy.TendInterval/4 {
		t.Fatalf("gave up in %s, every address refused at once", elapsed)
	}
}

func TestValidateNodeGivesUpOnceWhenNoAddressAccepts(t *testing.T) {
	withLookupHost(t, func(string) ([]string, error) {
		return []string{"192.0.2.1", "198.51.100.1"}, nil
	})
	var dials atomic.Int32
	withProbeDial(t, func(_, _ string, timeout time.Duration) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(timeout)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	})

	policy := NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.TendInterval = 300 * time.Millisecond
	started := time.Now()
	err := (&nodeValidator{}).validateNode(validationCluster(policy), NewHost("db.example.internal", 31091))
	elapsed := time.Since(started)

	if err == nil || !err.Matches(types.TIMEOUT) {
		t.Fatalf("error = %v, want TIMEOUT", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dialed %d times, want each address once", got)
	}
	// Serial dials of two silent addresses take 2 x TendInterval.
	if elapsed < policy.TendInterval || elapsed >= 2*policy.TendInterval {
		t.Fatalf("gave up in %s, cap is %s", elapsed, policy.TendInterval)
	}
}

func TestConnectFirstAliasSkipsNilPolicy(t *testing.T) {
	validator := &nodeValidator{aliases: []*Host{NewHost("192.0.2.1", 31091), NewHost("198.51.100.1", 31091)}}
	candidates, err := validator.connectFirstAlias(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || len(validator.aliases) != 2 {
		t.Fatalf("candidates = %d, aliases = %d, want 2 each", len(candidates), len(validator.aliases))
	}
}

func TestValidateNodeOnAnIPDoesNotResolve(t *testing.T) {
	server := startFakeInfoServer(t)

	withLookupHost(t, func(string) ([]string, error) {
		t.Fatal("lookupHost called for an IP address")
		return nil, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 2 * time.Second
	validator := &nodeValidator{}
	if err := validator.validateNode(validationCluster(policy), server.host()); err != nil {
		t.Fatal(err)
	}
	if validator.primaryHost == nil || validator.primaryHost.Name != server.host().Name {
		t.Fatalf("primary host = %v, want %s", validator.primaryHost, server.host())
	}
}

func TestMoveFailedNodeLeavesANodeDialingASingleAddressHostname(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			conn.Close()
		}
	}()
	address := listener.Addr().(*net.TCPAddr)

	const hostname = "db.example.invalid"
	withLookupHost(t, func(string) ([]string, error) {
		return []string{address.IP.String()}, nil
	})

	policy := NewClientPolicy()
	policy.Timeout = 200 * time.Millisecond
	node := &Node{name: "BB9A000000000001", host: NewHost(hostname, address.Port), lookupName: hostname}
	node.failures.Set(1)
	node.active.Set(true)
	peers := newPeers(1, 1)
	var partMap iatomic.Guard[partitionMap]
	validationCluster(policy).moveFailedNode(peers, node, &partMap)

	time.Sleep(50 * time.Millisecond)
	if got := accepted.Load(); got != 0 {
		t.Fatalf("validated %d times, the node already follows its hostname", got)
	}
	if peers.containsNodeToRemove(node) {
		t.Fatal("node was replaced")
	}
}

func TestMoveFailedNodeGivesUpAfterOneBoundWhenNoOtherAddressAccepts(t *testing.T) {
	const hostname = "db.example.internal"
	withLookupHost(t, func(string) ([]string, error) {
		return []string{"127.0.0.1", "192.0.2.1", "198.51.100.1"}, nil
	})
	var dials atomic.Int32
	withProbeDial(t, func(_, _ string, timeout time.Duration) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(timeout)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	})

	policy := NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.TendInterval = 300 * time.Millisecond
	node := &Node{name: "BB9A000000000001", host: NewHost(hostname, 31091), lookupName: hostname, resolvedHost: NewHost("127.0.0.1", 31091)}
	node.failures.Set(1)
	node.active.Set(true)
	peers := newPeers(1, 1)
	var partMap iatomic.Guard[partitionMap]
	started := time.Now()
	validationCluster(policy).moveFailedNode(peers, node, &partMap)
	elapsed := time.Since(started)

	if got := dials.Load(); got != 2 {
		t.Fatalf("dialed %d times, want each other address once", got)
	}
	// Validating each silent address in turn takes 2 x Timeout/2.
	if elapsed < policy.TendInterval || elapsed >= 2*policy.TendInterval {
		t.Fatalf("gave up in %s, cap is %s", elapsed, policy.TendInterval)
	}
	if peers.containsNodeToRemove(node) {
		t.Fatal("node was replaced")
	}
}

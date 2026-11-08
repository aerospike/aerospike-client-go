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
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

var _ = gg.Describe("Node address move", gg.Label("discovery"), func() {
	const (
		tendInterval = 250 * time.Millisecond
		moveBound    = 20 * tendInterval
		nodeA        = "BB9A000000000001"
		nodeB        = "BB9B000000000001"
		nodeC        = "BB9C000000000001"
		namespace    = "test"
	)

	type stayCase int
	const (
		differentNode stayCase = iota
		ipLiteral
		singleAddress
		noFailures
	)

	var (
		first  = net.ParseIP("127.0.0.1")
		second net.IP

		servers    []*stubNodeServer
		client     *Client
		origLookup = lookupHost
		lookups    map[string]int
		lookupMu   sync.Mutex
		hosts      map[string][]string
	)

	gg.BeforeEach(func() {
		second = secondLoopback()
		if second == nil {
			gg.Skip("127.0.0.2 cannot be bound locally")
		}

		servers = nil
		client = nil
		lookups = map[string]int{}
		hosts = map[string][]string{}
		lookupHost = func(name string) ([]string, error) {
			lookupMu.Lock()
			lookups[name]++
			addrs := hosts[name]
			lookupMu.Unlock()
			if addrs != nil {
				out := make([]string, len(addrs))
				copy(out, addrs)
				return out, nil
			}
			return origLookup(name)
		}
	})

	gg.AfterEach(func() {
		lookupHost = origLookup
		if client != nil {
			client.Close()
		}
		for _, server := range servers {
			server.close()
		}
	})

	registerHostname := func(prefix string, addresses ...net.IP) string {
		name := fmt.Sprintf("%s-%d.move.test", prefix, time.Now().UnixNano())
		addrs := make([]string, len(addresses))
		for i, address := range addresses {
			addrs[i] = address.String()
		}
		lookupMu.Lock()
		hosts[name] = addrs
		lookupMu.Unlock()
		return name
	}

	lookupCount := func(name string) int {
		lookupMu.Lock()
		defer lookupMu.Unlock()
		return lookups[name]
	}

	startServer := func(ip net.IP, port int, name string) *stubNodeServer {
		server, err := newStubNodeServer(ip, port, name)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		servers = append(servers, server)
		return server
	}

	connect := func(hostname string, port int) *Client {
		policy := NewClientPolicy()
		policy.Timeout = 500 * time.Millisecond
		policy.TendInterval = tendInterval
		opened, err := NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client = opened
		return opened
	}

	connectSeedOnly := func(hostname string, port int) *Client {
		policy := NewClientPolicy()
		policy.Timeout = 500 * time.Millisecond
		policy.TendInterval = tendInterval
		policy.SeedOnlyCluster = true
		opened, err := NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client = opened
		return opened
	}

	otherNodeStays := func(stay stayCase) {
		hostA := registerHostname("a", first, second)
		var hostB string
		if stay == singleAddress {
			hostB = registerHostname("b", first)
		} else {
			hostB = registerHostname("b", first, second)
		}
		portA := freePortOn(first, second)
		portB := freePortOn(first, second)
		peerB := hostB
		if stay == ipLiteral {
			peerB = first.String()
		}

		a1 := startServer(first, portA, nodeA)
		b1 := startServer(first, portB, nodeB)
		b2Name := nodeB
		if stay == differentNode {
			b2Name = nodeC
		}

		peersA := "1,3000,[[" + nodeB + ",,[" + peerB + ":" + strconv.Itoa(portB) + "]]]"
		a1.setPeers(peersA)
		b1.setPeers("1,3000,[[" + nodeA + ",,[" + hostA + ":" + strconv.Itoa(portA) + "]]]")

		opened := connect(hostA, portA)
		gm.Eventually(func() int { return len(opened.GetNodes()) }, 10*time.Second, 20*time.Millisecond).Should(gm.Equal(2))
		oldB := nodeNamed(opened, nodeB)
		gm.Expect(oldB).ToNot(gm.BeNil())
		gm.Expect(oldB.host.Name).To(gm.Equal(first.String()))

		a2 := startServer(second, portA, nodeA)
		if stay == noFailures {
			a2.setPeers(peersA)
		} else {
			a2.setPeers("1,3000,[]")
		}
		startServer(second, portB, b2Name)

		lookupsB := lookupCount(hostB)

		a1.close()
		if stay != noFailures {
			b1.close()
		}

		gm.Eventually(func() bool {
			return nodeOnAddress(opened, nodeA, second)
		}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())

		if stay == noFailures {
			time.Sleep(10 * tendInterval)
			gm.Expect(oldB.IsActive()).To(gm.BeTrue())
			gm.Expect(nodeNamed(opened, nodeB)).To(gm.Equal(oldB))
			gm.Expect(lookupCount(hostB)).To(gm.Equal(lookupsB))
		} else {
			gm.Eventually(func() *Node {
				return nodeNamed(opened, nodeB)
			}, 10*time.Second, 20*time.Millisecond).Should(gm.BeNil())
		}

		if stay == ipLiteral {
			gm.Expect(nodeOnAddress(opened, nodeB, second)).To(gm.BeFalse())
		}

		record, err := opened.Get(nil, mustKey(namespace, "set", 1))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record).ToNot(gm.BeNil())
		gm.Expect(nodeOnAddress(opened, nodeB, second)).To(gm.BeFalse())
	}

	gg.It("moves a failing node to another address of its hostname", func() {
		hostname := registerHostname("move", first, second)
		port := freePortOn(first, second)
		live := startServer(first, port, nodeA)

		opened := connectSeedOnly(hostname, port)
		old := nodeNamed(opened, nodeA)
		gm.Expect(old).ToNot(gm.BeNil())
		gm.Expect(old.host.Name).To(gm.Equal(first.String()))
		gm.Expect(old.lookupName).To(gm.Equal(hostname))

		startServer(second, port, nodeA)
		begin := time.Now()
		live.close()
		gm.Eventually(func() bool {
			return nodeOnAddress(opened, nodeA, second)
		}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())
		gm.Expect(time.Since(begin)).To(gm.BeNumerically("<", moveBound))

		gm.Eventually(func() bool {
			return nodeOwnsPartitions(opened, nodeA, second)
		}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())

		record, err := opened.Get(nil, mustKey(namespace, "set", 1))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record).ToNot(gm.BeNil())
		gm.Expect(opened.GetNodes()).To(gm.HaveLen(1))
		gm.Expect(old.IsActive()).To(gm.BeFalse())
	})

	gg.It("moves every node when they all sit on the dead address", func() {
		hostname := registerHostname("zone", first, second)
		names := []string{nodeA, nodeB, nodeC}
		ports := make([]int, len(names))
		var dead []*stubNodeServer

		for i, name := range names {
			ports[i] = freePortOn(first, second)
			dead = append(dead, startServer(first, ports[i], name))
		}

		for i := range names {
			peers := peersFor(hostname, names, ports, i)
			for _, server := range servers {
				if server.port() == ports[i] {
					server.setPeers(peers)
				}
			}
		}

		opened := connect(hostname, ports[0])
		gm.Eventually(func() int { return len(opened.GetNodes()) }, 10*time.Second, 20*time.Millisecond).Should(gm.Equal(len(names)))
		for _, name := range names {
			gm.Expect(nodeOnAddress(opened, name, first)).To(gm.BeTrue())
		}

		for i, name := range names {
			standby := startServer(second, ports[i], name)
			standby.setPeers(peersFor(hostname, names, ports, i))
		}

		begin := time.Now()
		for _, server := range dead {
			server.close()
		}
		for _, name := range names {
			gm.Eventually(func() bool {
				return nodeOnAddress(opened, name, second)
			}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())
		}
		gm.Expect(time.Since(begin)).To(gm.BeNumerically("<", moveBound))
		gm.Eventually(func() int { return len(opened.GetNodes()) }, 10*time.Second, 20*time.Millisecond).Should(gm.Equal(len(names)))

		record, err := opened.Get(nil, mustKey(namespace, "set", 1))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record).ToNot(gm.BeNil())
	})

	gg.It("does not move onto an address that is a different node", func() {
		otherNodeStays(differentNode)
	})

	gg.It("does not re-resolve a node whose advertised address is an IP literal", func() {
		otherNodeStays(ipLiteral)
	})

	gg.It("does not move when the hostname has one address", func() {
		otherNodeStays(singleAddress)
	})

	gg.It("does not look up a hostname for a node that has no failures", func() {
		otherNodeStays(noFailures)
	})
})

func peersFor(hostname string, names []string, ports []int, skip int) string {
	var b strings.Builder
	b.WriteString("1,3000,[")
	wrote := false
	for i, name := range names {
		if i == skip {
			continue
		}
		if wrote {
			b.WriteByte(',')
		}
		b.WriteString("[" + name + ",,[" + hostname + ":" + strconv.Itoa(ports[i]) + "]]")
		wrote = true
	}
	b.WriteByte(']')
	return b.String()
}

func nodeNamed(client *Client, name string) *Node {
	for _, node := range client.GetNodes() {
		if node.GetName() == name {
			return node
		}
	}
	return nil
}

func nodeOnAddress(client *Client, name string, ip net.IP) bool {
	node := nodeNamed(client, name)
	return node != nil && node.IsActive() && node.host.Name == ip.String()
}

func nodeOwnsPartitions(client *Client, name string, ip net.IP) bool {
	node := nodeNamed(client, name)
	if node == nil || !node.IsActive() || node.host.Name != ip.String() {
		return false
	}
	partMap := client.cluster.getPartitions()
	partitions := partMap["test"]
	if partitions == nil || len(partitions.Replicas) == 0 {
		return false
	}
	for _, owner := range partitions.Replicas[0] {
		if owner == nil || owner.GetName() != name || owner.host.Name != ip.String() {
			return false
		}
	}
	return true
}

func mustKey(namespace, set string, value int) *Key {
	key, err := NewKey(namespace, set, value)
	if err != nil {
		panic(err)
	}
	return key
}

func secondLoopback() net.IP {
	ip := net.ParseIP("127.0.0.2")
	if canBindIP(ip) {
		return ip
	}
	return nil
}

func canBindIP(ip net.IP) bool {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

func freePortOn(first, second net.IP) int {
	for range 100 {
		listener, err := net.Listen("tcp", net.JoinHostPort(second.String(), "0"))
		if err != nil {
			continue
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		probe, err := net.Listen("tcp", net.JoinHostPort(first.String(), strconv.Itoa(port)))
		if err != nil {
			continue
		}
		probe.Close()
		return port
	}
	panic("no port free on both 127.0.0.1 and 127.0.0.2")
}

type stubNodeServer struct {
	listener net.Listener
	nodeName string
	replicas string

	mu    sync.Mutex
	peers string
	conns []net.Conn
}

func newStubNodeServer(ip net.IP, port int, nodeName string) (*stubNodeServer, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	bitmap := bytes.Repeat([]byte{0xff}, 4096/8)
	server := &stubNodeServer{
		listener: listener,
		nodeName: nodeName,
		replicas: "test:0,1," + base64.StdEncoding.EncodeToString(bitmap) + ";",
		peers:    "1,3000,[]",
	}
	go server.serve()
	return server, nil
}

func (server *stubNodeServer) port() int {
	return server.listener.Addr().(*net.TCPAddr).Port
}

func (server *stubNodeServer) setPeers(peers string) {
	server.mu.Lock()
	server.peers = peers
	server.mu.Unlock()
}

func (server *stubNodeServer) close() {
	server.listener.Close()
	server.mu.Lock()
	conns := server.conns
	server.conns = nil
	server.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

func (server *stubNodeServer) serve() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.mu.Lock()
		server.conns = append(server.conns, conn)
		server.mu.Unlock()
		go server.serveConnection(conn)
	}
}

func (server *stubNodeServer) serveConnection(conn net.Conn) {
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

		var message *types.Message
		if header.Type == uint8(types.MSG_MESSAGE) {
			payload := make([]byte, 22)
			payload[0] = 22
			message = types.NewMessage(types.MSG_MESSAGE, payload)
		} else {
			message = types.NewMessage(types.MSG_INFO, server.info(body))
		}
		response, err := message.Serialize()
		if err != nil {
			return
		}

		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}

func (server *stubNodeServer) info(request []byte) []byte {
	server.mu.Lock()
	peers := server.peers
	server.mu.Unlock()

	var out bytes.Buffer
	for _, command := range strings.Split(strings.TrimSpace(string(request)), "\n") {
		if command == "" {
			continue
		}
		if value, ok := server.infoValue(command, peers); ok {
			out.WriteString(command + "\t" + value + "\n")
		}
	}
	return out.Bytes()
}

func (server *stubNodeServer) infoValue(command, peers string) (string, bool) {
	switch command {
	case "node":
		return server.nodeName, true
	case "partition-generation", "peers-generation":
		return "1", true
	case "build":
		return "8.1.0.0", true
	case "replicas":
		return server.replicas, true
	case "peers-clear-std":
		return peers, true
	default:
		return "", false
	}
}

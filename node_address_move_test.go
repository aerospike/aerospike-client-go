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
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
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

		servers     []*stubNodeServer
		client      *Client
		origLookup  = lookupHost
		lookups     map[string]int
		lookupMu    sync.Mutex
		hosts       map[string][]string
		lookupDelay time.Duration
	)

	gg.BeforeEach(func() {
		second = secondLoopback()
		if second == nil && canBindIP(net.IPv6loopback) {
			second = net.IPv6loopback
		}
		if second == nil {
			gg.Skip("neither 127.0.0.2 nor ::1 can be bound locally")
		}

		servers = nil
		client = nil
		lookups = map[string]int{}
		hosts = map[string][]string{}
		lookupDelay = 0
		lookupHost = func(name string) ([]string, error) {
			lookupMu.Lock()
			lookups[name]++
			addrs := hosts[name]
			delay := lookupDelay
			lookupMu.Unlock()
			if addrs != nil {
				time.Sleep(delay)
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

	setAddresses := func(name string, addresses ...net.IP) {
		addrs := make([]string, len(addresses))
		for i, address := range addresses {
			addrs[i] = address.String()
		}
		lookupMu.Lock()
		hosts[name] = addrs
		lookupMu.Unlock()
	}

	registerHostname := func(prefix string, addresses ...net.IP) string {
		name := fmt.Sprintf("%s-%d.move.test", prefix, time.Now().UnixNano())
		setAddresses(name, addresses...)
		return name
	}

	setLookupDelay := func(delay time.Duration) {
		lookupMu.Lock()
		lookupDelay = delay
		lookupMu.Unlock()
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

	// connectWith opens a client with the given Timeout and TendInterval. With
	// TendInterval of an hour the spec drives every tend with cluster.tend().
	connectWith := func(hostname string, port int, timeout, interval time.Duration) *Client {
		policy := NewClientPolicy()
		policy.Timeout = timeout
		policy.TendInterval = interval
		opened, err := NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client = opened
		return opened
	}

	failTends := func(opened *Client, node *Node, from, to int) {
		for tend := from; tend <= to; tend++ {
			_ = opened.cluster.tend()
			gm.Expect(nodeNamed(opened, node.name)).To(gm.BeIdenticalTo(node), "tend %d replaced the node", tend)
			gm.Expect(node.failures.Get()).To(gm.Equal(tend), "tend %d", tend)
		}
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
			// Only the node closed last owns partitions, so a tend between the
			// closes keeps every node.
			if i < len(names)-1 {
				dead[i].setOwnsPartitions(false)
			}
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
			if i < len(names)-1 {
				standby.setOwnsPartitions(false)
			}
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

		gm.Eventually(func() bool {
			return nodeOwnsPartitions(opened, names[len(names)-1], second)
		}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())

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

	gg.It("removes rather than moves a node whose hostname has only the failed address", func() {
		otherNodeStays(singleAddress)
	})

	gg.It("does not look up a hostname for a node that has no failures", func() {
		otherNodeStays(noFailures)
	})

	gg.Context("bounded move", func() {
		var third net.IP

		gg.BeforeEach(func() {
			third = thirdAddress(first, second)
		})

		requireThird := func() {
			if third == nil {
				gg.Skip("no third local address to bind")
			}
		}

		gg.It("does not move a failing node before its third failed tend", func() {
			hostname := registerHostname("gate", first, second)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)
			gm.Expect(old).ToNot(gm.BeNil())

			startServer(second, port, nodeA)
			lookups := lookupCount(hostname)
			live.close()

			failTends(opened, old, 1, moveFailedTends-1)
			gm.Expect(lookupCount(hostname)).To(gm.Equal(lookups))

			_ = opened.cluster.tend()
			gm.Expect(nodeOnAddress(opened, nodeA, second)).To(gm.BeTrue())
			gm.Expect(old.IsActive()).To(gm.BeFalse())
		})

		gg.It("does not move a node that recovers before its third failed tend", func() {
			hostname := registerHostname("recover", first, second)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)
			startServer(second, port, nodeA)

			live.close()
			failTends(opened, old, 1, moveFailedTends-1)

			live = startServer(first, port, nodeA)
			_ = opened.cluster.tend()
			gm.Expect(nodeNamed(opened, nodeA)).To(gm.BeIdenticalTo(old))
			gm.Expect(old.failures.Get()).To(gm.Equal(0))

			live.close()
			failTends(opened, old, 1, moveFailedTends-1)
			gm.Expect(nodeOnAddress(opened, nodeA, first)).To(gm.BeTrue())
		})

		gg.It("clears the last tried address when the node recovers", func() {
			hostname := registerHostname("reset", first, second)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)

			live.close()
			failTends(opened, old, 1, moveFailedTends)
			gm.Expect(old.moveAddress).To(gm.Equal(second.String()))

			startServer(first, port, nodeA)
			_ = opened.cluster.tend()
			gm.Expect(nodeNamed(opened, nodeA)).To(gm.BeIdenticalTo(old))
			gm.Expect(old.failures.Get()).To(gm.Equal(0))
			gm.Expect(old.moveAddress).To(gm.BeEmpty())
		})

		gg.It("tries one address per tend", func() {
			requireThird()
			hostname := registerHostname("one", first, second, third)
			port := freePortOn(first, second, third)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)

			startServer(third, port, nodeA)
			live.close()

			failTends(opened, old, 1, moveFailedTends)
			gm.Expect(old.moveAddress).To(gm.Equal(second.String()))

			_ = opened.cluster.tend()
			gm.Expect(nodeOnAddress(opened, nodeA, third)).To(gm.BeTrue())
			gm.Expect(old.IsActive()).To(gm.BeFalse())
		})

		gg.It("starts after the last tried address when DNS order changes", func() {
			requireThird()
			hostname := registerHostname("order", first, second, third)
			port := freePortOn(first, second, third)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)

			live.close()
			failTends(opened, old, 1, moveFailedTends)
			gm.Expect(old.moveAddress).To(gm.Equal(second.String()))

			// second is now at the index after the one it had, so a stored
			// index would try second again.
			setAddresses(hostname, third, first, second)
			failTends(opened, old, moveFailedTends+1, moveFailedTends+1)
			gm.Expect(old.moveAddress).To(gm.Equal(third.String()))
		})

		gg.It("bounds a silent address by half the tend interval", func() {
			requireThird()
			hostname := registerHostname("silent", first, second, third)
			port := freePortOn(first, second, third)
			live := startServer(first, port, nodeA)
			const timeout = 6 * time.Second
			opened := connectWith(hostname, port, timeout, 200*time.Millisecond)

			startServer(second, port, nodeA).setSilent()
			startServer(third, port, nodeA)

			begin := time.Now()
			live.close()
			gm.Eventually(func() bool {
				return nodeOnAddress(opened, nodeA, third)
			}, 10*time.Second, 5*time.Millisecond).Should(gm.BeTrue())
			gm.Expect(time.Since(begin)).To(gm.BeNumerically("<", timeout/2))
		})

		gg.It("does not move onto an address that answers as another node", func() {
			hostname := registerHostname("other", first, second)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)

			startServer(second, port, nodeC)
			live.close()

			failTends(opened, old, 1, moveFailedTends+1)
			gm.Expect(old.moveAddress).To(gm.Equal(second.String()))
			gm.Expect(nodeNamed(opened, nodeC)).To(gm.BeNil())
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(1))
		})

		gg.It("does not move onto an address a healthy node owns", func() {
			hostA := registerHostname("owned", first, second)
			port := freePortOn(first, second)
			a1 := startServer(first, port, nodeA)
			a1.setPeers("1,3000,[[" + nodeB + ",,[" + second.String() + ":" + strconv.Itoa(port) + "]]]")
			b := startServer(second, port, nodeB)
			b.setPeers("1,3000,[[" + nodeA + ",,[" + hostA + ":" + strconv.Itoa(port) + "]]]")
			opened := connectWith(hostA, port, 500*time.Millisecond, time.Hour)
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(2))
			old := nodeNamed(opened, nodeA)
			liveB := nodeNamed(opened, nodeB)

			// B's tend connection stays B; the move candidate's new
			// connection to the same address answers as A.
			b.answerNewConnectionsAs(nodeA)
			a1.close()

			failTends(opened, old, 1, moveFailedTends)
			gm.Expect(old.moveAddress).To(gm.Equal(second.String()))
			gm.Expect(nodeNamed(opened, nodeB)).To(gm.BeIdenticalTo(liveB))
			gm.Expect(liveB.failures.Get()).To(gm.Equal(0))
		})

		gg.It("does not move a node advertised by its hostname through the peer path sooner", func() {
			// The failing node must not be on loopback: peerExists keeps a
			// localhost node without looking at the peer hosts.
			lan := nonLoopbackIPv4()
			if lan == nil {
				gg.Skip("no non-loopback IPv4 address to bind")
			}
			hostA := registerHostname("peer", lan, second)
			portA := freePortOn(lan, second)
			portB := freePortOn(lan, second)
			peersOfA := "1,3000,[[" + nodeB + ",,[" + lan.String() + ":" + strconv.Itoa(portB) + "]]]"

			a1 := startServer(lan, portA, nodeA)
			a1.setPeers(peersOfA)
			startServer(lan, portB, nodeB).setPeers("1,3000,[[" + nodeA + ",,[" + hostA + ":" + strconv.Itoa(portA) + "]]]")
			opened := connectWith(hostA, portA, 500*time.Millisecond, time.Hour)
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(2))
			old := nodeNamed(opened, nodeA)
			gm.Expect(old.host.IsLocalhost()).To(gm.BeFalse())

			startServer(second, portA, nodeA).setPeers(peersOfA)
			a1.close()

			failTends(opened, old, 1, moveFailedTends-1)

			_ = opened.cluster.tend()
			gm.Expect(nodeOnAddress(opened, nodeA, second)).To(gm.BeTrue())
			gm.Expect(old.IsActive()).To(gm.BeFalse())
		})

		gg.It("moves a node advertised by its hostname when the hostname now resolves to one new address", func() {
			// The failing node must not be on loopback: peerExists keeps a
			// localhost node without looking at the peer hosts.
			lan := nonLoopbackIPv4()
			if lan == nil {
				gg.Skip("no non-loopback IPv4 address to bind")
			}
			hostB := registerHostname("single", lan)
			portA := freePortOn(first, second)
			portB := freePortOn(lan, second)
			peersOfA := "1,3000,[[" + nodeB + ",,[" + hostB + ":" + strconv.Itoa(portB) + "]]]"
			peersOfB := "1,3000,[[" + nodeA + ",,[" + first.String() + ":" + strconv.Itoa(portA) + "]]]"

			startServer(first, portA, nodeA).setPeers(peersOfA)
			b1 := startServer(lan, portB, nodeB)
			b1.setPeers(peersOfB)
			opened := connectWith(first.String(), portA, 500*time.Millisecond, time.Hour)
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(2))
			old := nodeNamed(opened, nodeB)
			gm.Expect(old.lookupName).To(gm.Equal(hostB))
			gm.Expect(old.host.IsLocalhost()).To(gm.BeFalse())

			startServer(second, portB, nodeB).setPeers(peersOfB)
			setAddresses(hostB, second)
			b1.close()

			failTends(opened, old, 1, moveFailedTends-1)

			_ = opened.cluster.tend()
			gm.Expect(nodeOnAddress(opened, nodeB, second)).To(gm.BeTrue())
			gm.Expect(old.IsActive()).To(gm.BeFalse())
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(2))
			gm.Expect(nodeNamed(opened, nodeB).lookupName).To(gm.Equal(hostB))
		})

		gg.It("does not replace a node with itself when its hostname still resolves only to the failed address", func() {
			hostname := registerHostname("same", first)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)
			gm.Expect(old.lookupName).To(gm.Equal(hostname))

			lookups := lookupCount(hostname)
			live.close()

			failTends(opened, old, 1, moveFailedTends+1)
			gm.Expect(lookupCount(hostname)).To(gm.Equal(lookups+2), "tends 3 and 4 look up the hostname")
			gm.Expect(old.moveAddress).To(gm.BeEmpty())
			gm.Expect(opened.GetNodes()).To(gm.HaveLen(1))
		})

		gg.It("looks up the hostname on the tend goroutine", func() {
			const delay = 300 * time.Millisecond
			hostname := registerHostname("slow", first, second)
			port := freePortOn(first, second)
			live := startServer(first, port, nodeA)
			opened := connectWith(hostname, port, 500*time.Millisecond, time.Hour)
			old := nodeNamed(opened, nodeA)

			startServer(second, port, nodeA)
			setLookupDelay(delay)
			live.close()

			for tend := 1; tend < moveFailedTends; tend++ {
				begin := time.Now()
				_ = opened.cluster.tend()
				gm.Expect(time.Since(begin)).To(gm.BeNumerically("<", delay), "tend %d", tend)
			}

			begin := time.Now()
			_ = opened.cluster.tend()
			gm.Expect(time.Since(begin)).To(gm.BeNumerically(">=", delay))
			gm.Expect(nodeOnAddress(opened, nodeA, second)).To(gm.BeTrue())
			gm.Expect(old.IsActive()).To(gm.BeFalse())
		})
	})

	gg.Context("staggered connect for new peers", func() {
		var port int

		gg.BeforeEach(func() {
			port = freePortOn(first, second)
		})

		// openCluster connects to a seed on its own port, so the servers on
		// port only see the validation under test. Timeout and interval set
		// the stagger bound.
		openCluster := func(timeout, interval time.Duration) *Cluster {
			seed := startServer(first, freePortOn(first), nodeA)
			return connectWith(first.String(), seed.port(), timeout, interval).cluster
		}

		validate := func(cluster *Cluster, hostname string) (*nodeValidator, Error, time.Duration) {
			nv := &nodeValidator{}
			begin := time.Now()
			err := nv.validateNode(cluster, NewHost(hostname, port))
			return nv, err, time.Since(begin)
		}

		requireHung := func(n int) []net.IP {
			hung := hungAddresses(n)
			if hung == nil {
				gg.Skip("TEST-NET addresses do not hang on this network")
			}
			return hung
		}

		// runningConnects counts the connects started by connectFirst that
		// have not returned yet.
		runningConnects := func() int {
			stacks := make([]byte, 1<<20)
			stacks = stacks[:runtime.Stack(stacks, true)]
			return bytes.Count(stacks, []byte("connectFirst.func"))
		}

		gg.It("connects only to the first address when it answers", func() {
			cluster := openCluster(2*time.Second, time.Hour)
			live := startServer(first, port, nodeB)
			other := startServer(second, port, nodeB)

			nv, err, _ := validate(cluster, registerHostname("first", first, second))

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(nv.primaryHost.Name).To(gm.Equal(first.String()))
			accepts, _ := live.acceptStats()
			gm.Expect(accepts).To(gm.Equal(2), "one probe and one validation connection")
			accepts, _ = other.acceptStats()
			gm.Expect(accepts).To(gm.Equal(0))
		})

		gg.It("starts the next address after the stagger delay when an address hangs", func() {
			hung := requireHung(1)
			cluster := openCluster(2*time.Second, time.Hour)
			startServer(first, port, nodeB)

			nv, err, elapsed := validate(cluster, registerHostname("hung", hung[0], first))

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(nv.primaryHost.Name).To(gm.Equal(first.String()))
			gm.Expect(elapsed).To(gm.BeNumerically(">=", staggerDelay))
			gm.Expect(elapsed).To(gm.BeNumerically("<", time.Second))
		})

		gg.It("stops the connects still running once an address answers", func() {
			hung := requireHung(1)
			cluster := openCluster(2*time.Second, time.Hour)
			startServer(first, port, nodeB)

			_, err, _ := validate(cluster, registerHostname("cancelled", hung[0], first))

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Eventually(runningConnects, time.Second, 20*time.Millisecond).Should(gm.BeZero())
		})

		gg.It("stops the connects still running once the bound passes", func() {
			hung := requireHung(2)
			const bound = 200 * time.Millisecond
			cluster := openCluster(2*bound, time.Hour)

			_, err, _ := validate(cluster, registerHostname("expired", hung[0], hung[1]))

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Eventually(runningConnects, time.Second, 20*time.Millisecond).Should(gm.BeZero())
		})

		gg.It("returns a hung connect as soon as it is cancelled", func() {
			hung := requireHung(1)
			const cancelAfter = 100 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(cancelAfter, cancel)

			var dialer net.Dialer
			begin := time.Now()
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(hung[0].String(), strconv.Itoa(port)))
			elapsed := time.Since(begin)
			if conn != nil {
				conn.Close()
			}

			gg.AddReportEntry("cancelled connect", fmt.Sprintf("returned after %s: %v", elapsed, err))
			gm.Expect(errors.Is(err, context.Canceled)).To(gm.BeTrue(), "%v", err)
			gm.Expect(elapsed).To(gm.BeNumerically(">=", cancelAfter))
			gm.Expect(elapsed).To(gm.BeNumerically("<", cancelAfter+200*time.Millisecond))
		})

		gg.It("returns a hung connect at its deadline", func() {
			hung := requireHung(1)
			const deadline = 200 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()

			var dialer net.Dialer
			begin := time.Now()
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(hung[0].String(), strconv.Itoa(port)))
			elapsed := time.Since(begin)
			if conn != nil {
				conn.Close()
			}

			gg.AddReportEntry("connect past its deadline", fmt.Sprintf("returned after %s: %v", elapsed, err))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.(net.Error).Timeout()).To(gm.BeTrue(), "%v", err)
			gm.Expect(elapsed).To(gm.BeNumerically(">=", deadline))
			gm.Expect(elapsed).To(gm.BeNumerically("<", deadline+200*time.Millisecond))
		})

		gg.It("starts the next address at once when an address refuses", func() {
			cluster := openCluster(2*time.Second, time.Hour)
			startServer(first, port, nodeB)

			nv, err, elapsed := validate(cluster, registerHostname("refused", second, first))

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(nv.primaryHost.Name).To(gm.Equal(first.String()))
			gm.Expect(elapsed).To(gm.BeNumerically("<", staggerDelay))
		})

		gg.It("starts every address within the bound", func() {
			hung := requireHung(3)
			const bound = 200 * time.Millisecond
			cluster := openCluster(2*bound, time.Hour)
			live := startServer(first, port, nodeB)

			begin := time.Now()
			_, err, _ := validate(cluster, registerHostname("last", hung[0], hung[1], hung[2], first))

			gm.Expect(err).ToNot(gm.HaveOccurred())
			_, accepted := live.acceptStats()
			gm.Expect(accepted.Sub(begin)).To(gm.BeNumerically(">=", 3*bound/4), "the fourth address starts after three delays of bound/4")
			gm.Expect(accepted.Sub(begin)).To(gm.BeNumerically("<", bound))
		})

		gg.It("fails within half the timeout when every address hangs", func() {
			hung := requireHung(2)
			const bound = 200 * time.Millisecond
			cluster := openCluster(2*bound, time.Hour)
			hostname := registerHostname("allhung", hung[0], hung[1])

			_, err, elapsed := validate(cluster, hostname)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.(*AerospikeError).ResultCode).To(gm.Equal(types.TIMEOUT))
			gm.Expect(err.Error()).To(gm.ContainSubstring(fmt.Sprintf("No connection to %s completed for 2 addresses within %s", NewHost(hostname, port), bound)))
			gm.Expect(elapsed).To(gm.BeNumerically(">=", bound))
			gm.Expect(elapsed).To(gm.BeNumerically("<", bound+300*time.Millisecond))
		})

		gg.It("fails within the tend interval when it is below half the timeout", func() {
			hung := requireHung(2)
			const interval = 200 * time.Millisecond
			cluster := openCluster(4*time.Second, interval)

			_, err, elapsed := validate(cluster, registerHostname("interval", hung[0], hung[1]))

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(elapsed).To(gm.BeNumerically(">=", interval))
			gm.Expect(elapsed).To(gm.BeNumerically("<", interval+300*time.Millisecond))
		})

		gg.It("connects a hostname with one address as before", func() {
			hung := requireHung(1)
			const timeout = time.Second
			cluster := openCluster(timeout, 100*time.Millisecond)

			_, err, elapsed := validate(cluster, registerHostname("single", hung[0]))

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).ToNot(gm.ContainSubstring("No connection to"))
			gm.Expect(elapsed).To(gm.BeNumerically(">=", timeout/2), "one address keeps the Timeout/2 dial, not the stagger bound")
		})

		gg.It("connects an IP literal as before", func() {
			hung := requireHung(1)
			const timeout = time.Second
			cluster := openCluster(timeout, 100*time.Millisecond)

			_, err, elapsed := validate(cluster, hung[0].String())

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).ToNot(gm.ContainSubstring("No connection to"))
			gm.Expect(elapsed).To(gm.BeNumerically(">=", timeout/2), "an IP literal keeps the Timeout/2 dial, not the stagger bound")
		})

		gg.It("still adds every node of a seed hostname with several addresses", func() {
			cluster := openCluster(2*time.Second, time.Hour)
			startServer(first, port, nodeB)
			startServer(second, port, nodeC)

			nv := &nodeValidator{}
			nodesToAdd := nodesToAddT{}
			err := nv.seedNodes(cluster, NewHost(registerHostname("seed", first, second), port), nodesToAdd)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(nodesToAdd).To(gm.HaveKey(nodeB))
			gm.Expect(nodesToAdd).To(gm.HaveKey(nodeC))
		})
	})
})

// thirdAddress returns a local address other than first and second that can
// be bound, or nil.
func thirdAddress(first, second net.IP) net.IP {
	for _, ip := range []net.IP{net.IPv6loopback, nonLoopbackIPv4()} {
		if ip != nil && !ip.Equal(first) && !ip.Equal(second) && canBindIP(ip) {
			return ip
		}
	}
	return nil
}

func nonLoopbackIPv4() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.To4() == nil || !ipNet.IP.IsGlobalUnicast() {
			continue
		}
		if canBindIP(ipNet.IP) {
			return ipNet.IP.To4()
		}
	}
	return nil
}

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

// hungAddresses returns n TEST-NET addresses whose TCP connect does not
// complete, or nil when the network answers them at once.
func hungAddresses(n int) []net.IP {
	ips := make([]net.IP, n)
	for i := range ips {
		ips[i] = net.IPv4(192, 0, 2, byte(i+1)).To4()
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ips[0].String(), "3000"), 50*time.Millisecond)
	if err == nil {
		conn.Close()
		return nil
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		return nil
	}
	return ips
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

func freePortOn(ips ...net.IP) int {
	for range 100 {
		listener, err := net.Listen("tcp", net.JoinHostPort(ips[len(ips)-1].String(), "0"))
		if err != nil {
			continue
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		if portFreeOn(port, ips[:len(ips)-1]) {
			return port
		}
	}
	panic(fmt.Sprintf("no port free on every address of %v", ips))
}

func portFreeOn(port int, ips []net.IP) bool {
	for _, ip := range ips {
		probe, err := net.Listen("tcp", net.JoinHostPort(ip.String(), strconv.Itoa(port)))
		if err != nil {
			return false
		}
		probe.Close()
	}
	return true
}

type stubNodeServer struct {
	listener net.Listener
	nodeName string
	replicas string

	mu          sync.Mutex
	peers       string
	conns       []net.Conn
	silent      bool
	accepts     int
	firstAccept time.Time
}

func newStubNodeServer(ip net.IP, port int, nodeName string) (*stubNodeServer, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	server := &stubNodeServer{
		listener: listener,
		nodeName: nodeName,
		replicas: stubReplicas(true),
		peers:    "1,3000,[]",
	}
	go server.serve()
	return server, nil
}

func (server *stubNodeServer) port() int {
	return server.listener.Addr().(*net.TCPAddr).Port
}

// stubReplicas is a replicas reply that claims every partition of the test
// namespace, or none.
func stubReplicas(ownsPartitions bool) string {
	fill := byte(0x00)
	if ownsPartitions {
		fill = 0xff
	}
	bitmap := bytes.Repeat([]byte{fill}, 4096/8)
	return "test:0,1," + base64.StdEncoding.EncodeToString(bitmap) + ";"
}

func (server *stubNodeServer) setOwnsPartitions(ownsPartitions bool) {
	server.mu.Lock()
	server.replicas = stubReplicas(ownsPartitions)
	server.mu.Unlock()
}

func (server *stubNodeServer) setPeers(peers string) {
	server.mu.Lock()
	server.peers = peers
	server.mu.Unlock()
}

// setSilent makes the server accept connections and never answer, like a
// node that hangs after the TCP handshake.
func (server *stubNodeServer) setSilent() {
	server.mu.Lock()
	server.silent = true
	server.mu.Unlock()
}

// answerNewConnectionsAs makes connections accepted from now on report
// nodeName, while open connections keep the old name: two nodes behind one
// address.
func (server *stubNodeServer) answerNewConnectionsAs(nodeName string) {
	server.mu.Lock()
	server.nodeName = nodeName
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
		if server.accepts == 0 {
			server.firstAccept = time.Now()
		}
		server.accepts++
		server.mu.Unlock()
		go server.serveConnection(conn)
	}
}

// acceptStats returns how many connections the server accepted and when it
// accepted the first.
func (server *stubNodeServer) acceptStats() (int, time.Time) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.accepts, server.firstAccept
}

func (server *stubNodeServer) serveConnection(conn net.Conn) {
	defer conn.Close()

	server.mu.Lock()
	silent := server.silent
	nodeName := server.nodeName
	server.mu.Unlock()
	if silent {
		_, _ = io.Copy(io.Discard, conn)
		return
	}

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
			message = types.NewMessage(types.MSG_INFO, server.info(body, nodeName))
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

func (server *stubNodeServer) info(request []byte, nodeName string) []byte {
	server.mu.Lock()
	peers := server.peers
	replicas := server.replicas
	server.mu.Unlock()

	var out bytes.Buffer
	for _, command := range strings.Split(strings.TrimSpace(string(request)), "\n") {
		if command == "" {
			continue
		}

		if value, ok := server.infoValue(command, peers, replicas, nodeName); ok {
			out.WriteString(command + "\t" + value + "\n")
		}
	}
	return out.Bytes()
}

func (server *stubNodeServer) infoValue(command, peers, replicas, nodeName string) (string, bool) {
	switch command {
	case "node":
		return nodeName, true
	case "partition-generation", "peers-generation":
		return "1", true
	case "build":
		return "8.1.0.0", true
	case "replicas":
		return replicas, true
	case "peers-clear-std":
		return peers, true
	default:
		return "", false
	}
}

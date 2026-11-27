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
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

var _ = gg.Describe("Peer address collisions", gg.Label("discovery"), func() {
	const (
		seedName       = "BB9E000000000001"
		liveName       = "BB9E000000000002"
		newcomerName   = "BB9E000000000003"
		otherName      = "BB9E000000000004"
		firstPeerName  = "BB9E000000000005"
		secondPeerName = "BB9E000000000006"
	)

	var (
		loopback   = net.ParseIP("127.0.0.1")
		origLookup = lookupHost
		lookupMu   sync.Mutex
		hosts      map[string][]string
	)

	gg.BeforeEach(func() {
		hosts = map[string][]string{}
		lookupHost = func(name string) ([]string, error) {
			lookupMu.Lock()
			addrs, exists := hosts[name]
			lookupMu.Unlock()
			if exists {
				return append([]string(nil), addrs...), nil
			}
			return origLookup(name)
		}
		gg.DeferCleanup(func() { lookupHost = origLookup })
	})

	registerHostname := func(prefix string, address net.IP) string {
		name := prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".collide.test"
		lookupMu.Lock()
		hosts[name] = []string{address.String()}
		lookupMu.Unlock()
		return name
	}

	startServer := func(port int, name string) *stubNodeServer {
		server, err := newStubNodeServer(loopback, port, name)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.DeferCleanup(server.close)
		return server
	}

	peerList := func(entries ...string) string {
		return "1,3000,[" + strings.Join(entries, ",") + "]"
	}

	peerEntry := func(name, host string, port int) string {
		return "[" + name + ",,[" + host + ":" + strconv.Itoa(port) + "]]"
	}

	connect := func(seed *stubNodeServer) *Client {
		policy := NewClientPolicy()
		policy.Timeout = time.Second
		policy.TendInterval = time.Hour
		client, err := NewClientWithPolicyAndHost(policy, NewHost(loopback.String(), seed.port()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.DeferCleanup(client.Close)
		return client
	}

	collisionsFor := func(capture *warnLogCapture, name string) []string {
		var found []string
		for _, message := range capture.snapshot() {
			if strings.Contains(message, "TRANSLATION_COLLISION") && strings.Contains(message, name) {
				found = append(found, message)
			}
		}
		return found
	}

	expectAliasLogged := func(capture *warnLogCapture, aliasName, nodeName string) {
		gm.Expect(collisionsFor(capture, aliasName)).To(gm.ContainElement(gm.ContainSubstring(nodeName)),
			"the peer that reached another node should be logged as a collision naming both")
		gm.Expect(capture.snapshot()).ToNot(gm.ContainElement(gm.ContainSubstring("both validated as node")),
			"the peer advertised as the node must not be logged as a name clash")
	}

	clusterWithLiveNode := func(active bool, failures int) *Cluster {
		liveNode := &Node{name: liveName, host: NewHost("gateway.example.com", 30001)}
		liveNode.active.Set(active)
		liveNode.failures.Set(failures)
		cluster := &Cluster{translatorName: "pass-through", nodesMap: *sm.New[string, *Node](4)}
		cluster.nodes.Set([]*Node{liveNode})
		cluster.nodesMap.Set(liveNode.name, liveNode)
		return cluster
	}

	newcomerOnLiveAddress := func() *peers {
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{
			{nodeName: newcomerName, hosts: []*Host{NewHost("gateway.example.com", 30001)}},
		})
		return tendPeers
	}

	gg.It("keeps a healthy live node's address against a newcomer", func() {
		cluster := clusterWithLiveNode(true, 0)
		tendPeers := newcomerOnLiveAddress()
		newcomer := tendPeers.peers()[0]

		gm.Expect(cluster.sharedOwner(tendPeers, newcomer, newcomer.hosts[0])).To(gm.Equal(liveName), "a newcomer on a healthy live node's address must skip it")
	})

	gg.It("does not let a failing live node own its address against a newcomer", func() {
		cluster := clusterWithLiveNode(true, 1)
		tendPeers := newcomerOnLiveAddress()
		newcomer := tendPeers.peers()[0]

		gm.Expect(cluster.sharedOwner(tendPeers, newcomer, newcomer.hosts[0])).To(gm.BeEmpty(), "a live node with tend failures must not own its address")
	})

	gg.It("adds every node when each also advertises one shared address", func() {
		seed := startServer(0, seedName)
		first := startServer(0, firstPeerName)
		second := startServer(0, secondPeerName)
		const bridge = "172.31.255.1:3000"
		entry := func(name string, server *stubNodeServer) string {
			return "[" + name + ",,[" + bridge + "," + loopback.String() + ":" + strconv.Itoa(server.port()) + "]]"
		}
		seed.setPeers(peerList(entry(firstPeerName, first), entry(secondPeerName, second)))
		first.setPeers(peerList(entry(seedName, seed), entry(secondPeerName, second)))
		second.setPeers(peerList(entry(seedName, seed), entry(firstPeerName, first)))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		for range 2 {
			_ = client.cluster.tend()
		}

		gm.Expect(client.GetNodes()).To(gm.HaveLen(3), "each peer should join through its own address")
		gm.Expect(nodeNamed(client, firstPeerName).host.Port).To(gm.Equal(first.port()))
		gm.Expect(nodeNamed(client, secondPeerName).host.Port).To(gm.Equal(second.port()))
		gm.Expect(capture.snapshot()).ToNot(gm.ContainElement(gm.ContainSubstring("TRANSLATION_COLLISION")))
		gm.Expect(capture.snapshot()).ToNot(gm.ContainElement(gm.ContainSubstring("Skip address")),
			"an untranslated shared address is skipped at info level")
	})

	gg.It("keeps a healthy node on its address and does not add a newcomer advertised there", func() {
		seed := startServer(0, seedName)
		live := startServer(0, liveName)
		seed.setPeers(peerList(peerEntry(liveName, loopback.String(), live.port())))
		live.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		gm.Expect(nodeNamed(client, liveName)).ToNot(gm.BeNil(), "the live node should join before the newcomer is advertised")

		seed.setPeers(peerList(
			peerEntry(liveName, loopback.String(), live.port()),
			peerEntry(newcomerName, loopback.String(), live.port()),
		))
		for range 2 {
			_ = client.cluster.tend()
		}

		gm.Expect(nodeNamed(client, newcomerName)).To(gm.BeNil(), "the newcomer must not join on the healthy node's address in any tend")
		liveNode := nodeNamed(client, liveName)
		gm.Expect(liveNode).ToNot(gm.BeNil(), "the healthy node must stay in the cluster")
		gm.Expect(liveNode.IsActive()).To(gm.BeTrue(), "the healthy node must stay active")
		gm.Expect(collisionsFor(capture, newcomerName)).ToNot(gm.BeEmpty(), "the rejected newcomer should be logged as a collision")
	})

	gg.It("adds a newcomer on the address of a node that failed in the same tend", func() {
		seed := startServer(0, seedName)
		live := startServer(0, liveName)
		port := live.port()
		seed.setPeers(peerList(peerEntry(liveName, loopback.String(), port)))
		live.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		failed := nodeNamed(client, liveName)
		gm.Expect(failed).ToNot(gm.BeNil(), "the live node should join before it fails")

		live.close()
		newcomer := startServer(port, newcomerName)
		newcomer.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		seed.setPeers(peerList(peerEntry(newcomerName, loopback.String(), port)))

		_ = client.cluster.tend()

		gm.Expect(failed.failures.Get()).To(gm.BeNumerically(">", 0), "the old node should have failed this tend")
		var refreshFailures []string
		for _, message := range capture.snapshot() {
			if strings.Contains(message, "refresh failed") && strings.Contains(message, liveName) {
				refreshFailures = append(refreshFailures, message)
			}
		}
		gm.Expect(refreshFailures).ToNot(gm.BeEmpty(), "the old node's failed refresh should be logged")
		gm.Expect(strings.Join(refreshFailures, "\n")).ToNot(gm.ContainSubstring("Node name has changed"),
			"the old node must fail on the network and stay active, not go inactive on a name change")
		gm.Expect(nodeNamed(client, newcomerName)).ToNot(gm.BeNil(), "the newcomer should join in the tend in which the old node failed")
	})

	gg.It("adds neither of two peers that validate as the same node", func() {
		seed := startServer(0, seedName)
		other := startServer(0, otherName)
		other.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		first := registerHostname("first", loopback)
		second := registerHostname("second", loopback)
		seed.setPeers(peerList(
			peerEntry(firstPeerName, first, other.port()),
			peerEntry(secondPeerName, second, other.port()),
		))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		for range 2 {
			_ = client.cluster.tend()
		}

		gm.Expect(nodeNamed(client, otherName)).To(gm.BeNil(), "neither peer may add the node both validated as")
		gm.Expect(client.GetNodes()).To(gm.HaveLen(1), "only the seed should be a node")
		gm.Expect(collisionsFor(capture, firstPeerName)).ToNot(gm.BeEmpty(), "the first peer should be logged as a collision")
		gm.Expect(collisionsFor(capture, secondPeerName)).ToNot(gm.BeEmpty(), "the second peer should be logged as a collision")
	})

	gg.It("adds neither of two peers on different addresses that validate as the same node", func() {
		seed := startServer(0, seedName)
		firstServer := startServer(0, otherName)
		secondServer := startServer(0, otherName)
		firstServer.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		secondServer.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		seed.setPeers(peerList(
			peerEntry(firstPeerName, loopback.String(), firstServer.port()),
			peerEntry(secondPeerName, loopback.String(), secondServer.port()),
		))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		for range 2 {
			_ = client.cluster.tend()
		}

		gm.Expect(nodeNamed(client, otherName)).To(gm.BeNil(), "neither peer may add the node both validated as")
		gm.Expect(client.GetNodes()).To(gm.HaveLen(1), "only the seed should be a node")
		var sameName []string
		for _, message := range capture.snapshot() {
			if strings.Contains(message, "both validated as node "+otherName) {
				sameName = append(sameName, message)
			}
		}
		gm.Expect(strings.Join(sameName, "\n")).To(gm.ContainSubstring(firstPeerName), "the first peer should be logged as a name clash")
		gm.Expect(strings.Join(sameName, "\n")).To(gm.ContainSubstring(secondPeerName), "the second peer should be logged as a name clash")
	})

	gg.It("adds the peer advertised as the node when another peer validates as it on the same address", func() {
		seed := startServer(0, seedName)
		other := startServer(0, otherName)
		other.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		own := registerHostname("own", loopback)
		alias := registerHostname("alias", loopback)
		seed.setPeers(peerList(
			peerEntry(otherName, own, other.port()),
			peerEntry(secondPeerName, alias, other.port()),
		))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		for range 2 {
			_ = client.cluster.tend()
		}

		gm.Expect(nodeNamed(client, otherName)).ToNot(gm.BeNil(), "the peer advertised as the node should add it")
		gm.Expect(nodeNamed(client, secondPeerName)).To(gm.BeNil(), "the peer that only reached that node must not add a node")
		gm.Expect(client.GetNodes()).To(gm.HaveLen(2), "only the seed and the node should be nodes")
		expectAliasLogged(capture, secondPeerName, otherName)
	})

	gg.It("adds the peer advertised as the node when another peer validates as it on a different address", func() {
		seed := startServer(0, seedName)
		ownServer := startServer(0, otherName)
		aliasServer := startServer(0, otherName)
		ownServer.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		aliasServer.setPeers(peerList(peerEntry(seedName, loopback.String(), seed.port())))
		seed.setPeers(peerList(
			peerEntry(otherName, loopback.String(), ownServer.port()),
			peerEntry(secondPeerName, loopback.String(), aliasServer.port()),
		))

		// The logger level is not synchronised with the tend goroutine; capture before the client starts.
		capture := captureWarnLogs()
		client := connect(seed)
		for range 2 {
			_ = client.cluster.tend()
		}

		node := nodeNamed(client, otherName)
		gm.Expect(node).ToNot(gm.BeNil(), "the peer advertised as the node should add it")
		gm.Expect(node.host.Port).To(gm.Equal(ownServer.port()), "the node should be on the address advertised under its own name")
		gm.Expect(nodeNamed(client, secondPeerName)).To(gm.BeNil(), "the peer that only reached that node must not add a node")
		gm.Expect(client.GetNodes()).To(gm.HaveLen(2), "only the seed and the node should be nodes")
		expectAliasLogged(capture, secondPeerName, otherName)
	})

	gg.It("adds neither the peer advertised as a node nor a peer that validated as another node on its address", func() {
		shared := NewHost("lb.example.com", 3000)
		owner := &peer{nodeName: otherName, validated: &nodeValidator{name: otherName, primaryHost: shared}}
		alias := &peer{nodeName: secondPeerName, validated: &nodeValidator{name: otherName, primaryHost: shared}}
		stranger := &peer{nodeName: firstPeerName, validated: &nodeValidator{name: firstPeerName, primaryHost: shared}}
		cluster := &Cluster{translatorName: "pass-through", nodesMap: *sm.New[string, *Node](4)}

		capture := captureWarnLogs()
		added := cluster.addValidatedPeers(newPeers(8, 8), []*peer{owner, alias, stranger})

		gm.Expect(added).To(gm.BeEmpty(), "two node names on one address must add neither node")
		gm.Expect(collisionsFor(capture, otherName)).ToNot(gm.BeEmpty(), "the peer advertised as the node should be logged as a collision")
		gm.Expect(collisionsFor(capture, firstPeerName)).ToNot(gm.BeEmpty(), "the peer that validated as another node should be logged as a collision")
	})

	gg.Describe("when a peer address answers as another node", func() {
		const (
			tendInterval = 250 * time.Millisecond
			joinBound    = 5 * time.Second
			settle       = 12 * tendInterval
			nodeA        = "BB9A00000000000A"
			nodeB        = "BB9B00000000000B"
			nodeC        = "BB9C00000000000C"
			nodeD        = "BB9D00000000000D"
			nodeX        = "BB9E00000000000E"
		)

		var (
			servers []*stubNodeServer
			logs    *warnLogCapture
			ipMap   map[string]string
		)

		gg.BeforeEach(func() {
			servers = nil
			ipMap = nil
			logs = captureWarnLogs()
		})

		start := func(name string) *stubNodeServer {
			server := startServer(0, name)
			servers = append(servers, server)
			return server
		}

		local := func(server *stubNodeServer) string {
			return net.JoinHostPort(loopback.String(), strconv.Itoa(server.port()))
		}

		type entry struct {
			name  string
			hosts []string
		}

		advertise := func(entries ...entry) {
			for _, server := range servers {
				var others []string
				for _, e := range entries {
					if e.name != server.nodeName {
						others = append(others, "["+e.name+",,["+strings.Join(e.hosts, ",")+"]]")
					}
				}
				server.setPeers(peerList(others...))
			}
		}

		connectTending := func(seed *stubNodeServer) *Client {
			policy := NewClientPolicy()
			policy.Timeout = 500 * time.Millisecond
			policy.TendInterval = tendInterval
			policy.IpMap = ipMap
			client, err := NewClientWithPolicyAndHost(policy, NewHost(loopback.String(), seed.port()))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gg.DeferCleanup(client.Close)
			return client
		}

		names := func(client *Client) []string {
			return slices.Sorted(slices.Values(client.GetNodeNames()))
		}

		warned := func(parts ...string) bool {
			logs.mu.Lock()
			defer logs.mu.Unlock()
			for _, msg := range logs.msgs {
				all := true
				for _, part := range parts {
					if !strings.Contains(msg, part) {
						all = false
						break
					}
				}
				if all {
					return true
				}
			}
			return false
		}

		gg.It("creates both nodes when a peer lists an alias of a known node first", func() {
			a := start(nodeA)
			b := start(nodeB)
			bAlias := start(nodeB)
			d := start(nodeD)
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeD, []string{local(bAlias), local(d)}},
			)

			opened := connectTending(a)
			gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
			gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
			gm.Expect(nodeNamed(opened, nodeD).host.Port).To(gm.Equal(d.port()))
			gm.Expect(warned("Skip address")).To(gm.BeFalse(), "an untranslated shared address logs at INFO: %v", logs.msgs)
			gm.Expect(warned("TRANSLATION_COLLISION")).To(gm.BeFalse(), "warnings: %v", logs.msgs)
		})

		gg.It("stops a peer that lists a known node's socket under another name first", func() {
			a := start(nodeA)
			b := start(nodeB)
			d := start(nodeD)
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
			)

			opened := connectTending(a)
			gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB}))
			live := nodeNamed(opened, nodeB)

			bAlias := net.JoinHostPort(registerHostname("b", loopback), strconv.Itoa(b.port()))
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeD, []string{bAlias, local(d)}},
			)

			gm.Eventually(func() bool {
				return warned("TRANSLATION_COLLISION", "left node "+nodeD, bAlias+" is used by "+nodeB, "Not tried: "+local(d))
			}, joinBound, 20*time.Millisecond).Should(gm.BeTrue(), "warnings: %v", logs.msgs)
			gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB}))
			gm.Expect(nodeNamed(opened, nodeB)).To(gm.BeIdenticalTo(live))
			gm.Expect(live.IsActive()).To(gm.BeTrue())
		})

		gg.It("keeps the known node and does not create a peer whose only address answers as it", func() {
			a := start(nodeA)
			b := start(nodeB)
			bAlias := start(nodeB)
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeD, []string{local(bAlias)}},
			)

			opened := connectTending(a)
			gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB}))
			gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB}))
			gm.Expect(nodeNamed(opened, nodeA).IsActive()).To(gm.BeTrue())
			gm.Expect(nodeNamed(opened, nodeB).IsActive()).To(gm.BeTrue())
			gm.Expect(warned("TRANSLATION_COLLISION", nodeD, nodeB, local(bAlias), "Address translator static-map")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
			gm.Expect(warned("Skip address")).To(gm.BeFalse(), "an untranslated shared address logs at INFO: %v", logs.msgs)
		})

		gg.It("creates an unadvertised node under the name it answers with", func() {
			a := start(nodeA)
			x := start(nodeX)
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeD, []string{local(x)}},
			)

			opened := connectTending(a)
			gm.Eventually(func() *Node { return nodeNamed(opened, nodeX) }, joinBound, 20*time.Millisecond).
				ShouldNot(gm.BeNil())
			gm.Expect(nodeNamed(opened, nodeD)).To(gm.BeNil())
			gm.Expect(warned("Peer node `"+nodeD+"` is different than actual node `"+nodeX+"`")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
		})

		gg.It("warns when the translator produced the shared address", func() {
			a := start(nodeA)
			b := start(nodeB)
			bAlias := start(nodeB)
			d := start(nodeD)
			ipMap = map[string]string{"10.255.0.4": loopback.String()}
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeD, []string{net.JoinHostPort("10.255.0.4", strconv.Itoa(bAlias.port())), local(d)}},
			)

			opened := connectTending(a)
			gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
			gm.Expect(warned("Skip address "+local(bAlias)+" of node "+nodeD+" because node "+nodeB+" also has it. Address translator static-map")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
			gm.Expect(warned("TRANSLATION_COLLISION")).To(gm.BeFalse(), "warnings: %v", logs.msgs)
		})

		gg.It("reports a collision naming the translator when a translated peer has no address left", func() {
			a := start(nodeA)
			b := start(nodeB)
			bAlias := start(nodeB)
			ipMap = map[string]string{"10.255.0.4": loopback.String()}
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeD, []string{net.JoinHostPort("10.255.0.4", strconv.Itoa(bAlias.port()))}},
			)

			opened := connectTending(a)
			gm.Eventually(func() bool {
				return warned("TRANSLATION_COLLISION", nodeD, nodeB, local(bAlias), "Address translator static-map")
			}, joinBound, 20*time.Millisecond).Should(gm.BeTrue(), "warnings: %v", logs.msgs)
			gm.Expect(names(opened)).To(gm.Equal([]string{nodeA, nodeB}))
		})

		gg.It("keeps a collision when the socket already holds a node with another name", func() {
			a := start(nodeA)
			b := start(nodeB)
			c := start(nodeC)
			d := start(nodeD)
			lb := newSwitchProxy(loopback, local(c))
			gg.DeferCleanup(lb.close)
			lbAddr := net.JoinHostPort(loopback.String(), strconv.Itoa(lb.port()))
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeC, []string{lbAddr}},
			)

			opened := connectTending(a)
			gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
				Should(gm.Equal([]string{nodeA, nodeC}))
			live := nodeNamed(opened, nodeC)
			gm.Expect(live.host.Port).To(gm.Equal(lb.port()))

			lb.switchTo(local(b))
			advertise(
				entry{nodeA, []string{local(a)}},
				entry{nodeB, []string{local(b)}},
				entry{nodeC, []string{lbAddr}},
				entry{nodeD, []string{net.JoinHostPort(registerHostname("lb", loopback), strconv.Itoa(lb.port())), local(d)}},
			)

			gm.Eventually(func() *Node { return nodeNamed(opened, nodeB) }, joinBound, 20*time.Millisecond).
				ShouldNot(gm.BeNil())
			gm.Consistently(func() *Node { return nodeNamed(opened, nodeD) }, settle, 50*time.Millisecond).
				Should(gm.BeNil())
			gm.Expect(nodeNamed(opened, nodeC)).To(gm.BeIdenticalTo(live))
			gm.Expect(live.IsActive()).To(gm.BeTrue())
		})
	})
})

// switchProxy forwards each new connection to the current target; open connections keep theirs.
type switchProxy struct {
	listener net.Listener

	mu     sync.Mutex
	target string
	conns  []net.Conn
}

func newSwitchProxy(ip net.IP, target string) *switchProxy {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	gm.Expect(err).ToNot(gm.HaveOccurred())
	proxy := &switchProxy{listener: listener, target: target}
	go proxy.serve()
	return proxy
}

func (proxy *switchProxy) port() int {
	return proxy.listener.Addr().(*net.TCPAddr).Port
}

func (proxy *switchProxy) switchTo(target string) {
	proxy.mu.Lock()
	proxy.target = target
	proxy.mu.Unlock()
}

func (proxy *switchProxy) serve() {
	for {
		in, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		proxy.mu.Lock()
		target := proxy.target
		proxy.mu.Unlock()
		out, err := net.Dial("tcp", target)
		if err != nil {
			in.Close()
			continue
		}
		proxy.mu.Lock()
		proxy.conns = append(proxy.conns, in, out)
		proxy.mu.Unlock()
		go func() { io.Copy(out, in); out.Close() }()
		go func() { io.Copy(in, out); in.Close() }()
	}
}

func (proxy *switchProxy) close() {
	proxy.listener.Close()
	proxy.mu.Lock()
	conns := proxy.conns
	proxy.conns = nil
	proxy.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

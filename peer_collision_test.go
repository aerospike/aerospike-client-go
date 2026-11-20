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
	"net"
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
			if strings.Contains(message, "to the same address") && strings.Contains(message, name) {
				found = append(found, message)
			}
		}
		return found
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

		cluster.detectTranslationCollisions(tendPeers)

		gm.Expect(tendPeers.collided.Exists(newcomerName)).To(gm.BeTrue(), "a newcomer on a healthy live node's address must be marked collided")
	})

	gg.It("does not let a failing live node own its address against a newcomer", func() {
		cluster := clusterWithLiveNode(true, 1)
		tendPeers := newcomerOnLiveAddress()

		cluster.detectTranslationCollisions(tendPeers)

		gm.Expect(tendPeers.collided.Exists(newcomerName)).To(gm.BeFalse(), "a live node with tend failures must not own its address")
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
})

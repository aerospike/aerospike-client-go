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
	"crypto/tls"
	"sync/atomic"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

var _ = gg.Describe("Translation collision detection", gg.Label("discovery"), func() {

	newTestPeer := func(nodeName string, hosts ...*Host) *peer {
		return &peer{nodeName: nodeName, hosts: hosts}
	}

	gg.It("finds the other new peer on a shared translated address", func() {
		cluster := &Cluster{translatorName: "port-map"}
		first := newTestPeer("BB9010011AC4202", NewHost("gateway.example.com", 30001))
		second := newTestPeer("BB9020011AC4202", NewHost("gateway.example.com", 30001))
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{first, second})

		gm.Expect(cluster.sharedOwner(tendPeers, first, first.hosts[0])).To(gm.Equal("BB9020011AC4202"))
		gm.Expect(cluster.sharedOwner(tendPeers, second, second.hosts[0])).To(gm.Equal("BB9010011AC4202"))
	})

	gg.It("keeps distinct ports apart", func() {
		cluster := &Cluster{translatorName: "port-map"}
		first := newTestPeer("BB9010011AC4202", NewHost("gateway.example.com", 30001))
		second := newTestPeer("BB9020011AC4202", NewHost("gateway.example.com", 30002))
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{first, second})

		gm.Expect(cluster.sharedOwner(tendPeers, first, first.hosts[0])).To(gm.BeEmpty())
		gm.Expect(cluster.sharedOwner(tendPeers, second, second.hosts[0])).To(gm.BeEmpty())
	})

	gg.It("compares hostnames case-insensitively", func() {
		cluster := &Cluster{translatorName: "pass-through"}
		first := newTestPeer("BB9010011AC4202", NewHost("AS.Priv.Example.com", 31001))
		second := newTestPeer("BB9020011AC4202", NewHost("as.priv.example.com", 31001), NewHost("as.priv.example.com", 31002))
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{first, second})

		gm.Expect(cluster.sharedOwner(tendPeers, first, first.hosts[0])).To(gm.Equal("BB9020011AC4202"))
		gm.Expect(cluster.sharedOwner(tendPeers, second, second.hosts[1])).To(gm.BeEmpty())
	})

	gg.It("reports the live node as the owner of its address", func() {
		liveNode := &Node{name: "BB9010011AC4202", host: NewHost("gateway.example.com", 30001)}
		liveNode.active.Set(true)
		cluster := &Cluster{translatorName: "pass-through", nodesMap: *sm.New[string, *Node](4)}
		cluster.nodes.Set([]*Node{liveNode})
		cluster.nodesMap.Set(liveNode.name, liveNode)

		newcomer := newTestPeer("BB9090011AC4202", NewHost("gateway.example.com", 30001))
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{newcomer})

		gm.Expect(cluster.sharedOwner(tendPeers, newcomer, newcomer.hosts[0])).To(gm.Equal(liveNode.name))
	})

	gg.It("lists the first three shared addresses, counts the rest and lists the addresses not tried", func() {
		shared := []sharedAddress{
			{host: NewHost("h1", 3000), owner: "B"},
			{host: NewHost("h2", 3000), owner: "D"},
			{host: NewHost("h3", 3000), owner: "E"},
			{host: NewHost("h4", 3000), owner: "F"},
			{host: NewHost("h5", 3000), owner: "G"},
		}
		message := newTranslationCollision("port-map", "A", shared, nil).Error()
		gm.Expect(message).To(gm.ContainSubstring("TRANSLATION_COLLISION"))
		gm.Expect(message).To(gm.ContainSubstring("Address translator port-map left node A with no address of its own: h1:3000 is used by B, h2:3000 is used by D, h3:3000 is used by E and 2 more. Give each node its own reachable address or mapping"))
		gm.Expect(message).ToNot(gm.ContainSubstring("h4"))

		message = newTranslationCollision("static-map", "A", shared[:1], []*Host{NewHost("own.example.com", 3000), NewHost("10.0.0.9", 3000)}).Error()
		gm.Expect(message).To(gm.ContainSubstring("h1:3000 is used by B. Not tried: own.example.com:3000, 10.0.0.9:3000. Give each node"))
	})

	gg.It("adds a node advertised twice once and neither of two nodes validated on one address", func() {
		cluster := &Cluster{translatorName: "pass-through", nodesMap: *sm.New[string, *Node](4), clientPolicy: new(atomic.Pointer[ClientPolicy])}
		cluster.clientPolicy.Store(NewClientPolicy())

		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{
			newTestPeer("A", NewHost("10.0.1.10", 3000)),
			newTestPeer("A", NewHost("10.0.1.10", 3000)),
			newTestPeer("B", NewHost("10.0.1.11", 3000)),
			newTestPeer("C", NewHost("10.0.1.11", 3000)),
		})
		candidates := tendPeers.peers()
		for _, candidate := range candidates {
			candidate.validated = &nodeValidator{name: candidate.nodeName, primaryHost: candidate.hosts[0]}
		}

		added := cluster.addValidatedPeers(tendPeers, candidates)

		gm.Expect(added).To(gm.HaveLen(1))
		gm.Expect(added[0].name).To(gm.Equal("A"))
		gm.Expect(tendPeers.nodes()).To(gm.HaveKey("A"))
		gm.Expect(tendPeers.nodes()).ToNot(gm.HaveKey("B"))
		gm.Expect(tendPeers.nodes()).ToNot(gm.HaveKey("C"))
	})
})

type countingViewObserver struct {
	staticMapAddressTranslator
	notifications atomic.Int64
	lastViewCount atomic.Int64
}

func (observer *countingViewObserver) OnClusterViewChanged(viewChange int64) {
	observer.notifications.Add(1)
	observer.lastViewCount.Store(viewChange)
}

type panickingViewObserver struct {
	passThroughAddressTranslator
}

func (panickingViewObserver) OnClusterViewChanged(int64) {
	panic("observer failure")
}

var _ = gg.Describe("Cluster view change notification", gg.Label("discovery"), func() {

	newClusterWithNodes := func(observer ClusterViewChangeObserver, nodes ...*Node) *Cluster {
		cluster := &Cluster{viewChangeObserver: observer, translatorName: "static-map"}
		cluster.nodes.Set(nodes)
		return cluster
	}

	gg.It("skips the initial view and fires once per later change", func() {
		observer := &countingViewObserver{}
		nodeA := &Node{name: "A", host: NewHost("10.0.0.1", 3000)}
		cluster := newClusterWithNodes(observer, nodeA)

		cluster.notifyClusterViewChanged()
		gm.Expect(observer.notifications.Load()).To(gm.BeZero())

		cluster.notifyClusterViewChanged()
		gm.Expect(observer.notifications.Load()).To(gm.BeZero())

		nodeB := &Node{name: "B", host: NewHost("10.0.0.2", 3000)}
		cluster.nodes.Set([]*Node{nodeA, nodeB})
		cluster.notifyClusterViewChanged()
		gm.Expect(observer.notifications.Load()).To(gm.Equal(int64(1)))
		gm.Expect(observer.lastViewCount.Load()).To(gm.Equal(int64(1)))

		nodeB.host = NewHost("10.0.0.9", 3000)
		cluster.notifyClusterViewChanged()
		gm.Expect(observer.notifications.Load()).To(gm.Equal(int64(2)))
		gm.Expect(observer.lastViewCount.Load()).To(gm.Equal(int64(2)))
	})

	gg.It("survives a panicking observer", func() {
		cluster := newClusterWithNodes(panickingViewObserver{}, &Node{name: "A", host: NewHost("10.0.0.1", 3000)})

		cluster.notifyClusterViewChanged()
		cluster.nodes.Set([]*Node{})
		gm.Expect(cluster.notifyClusterViewChanged).ToNot(gm.Panic())
	})
})

var _ = gg.Describe("Seed TLS name defaults", gg.Label("discovery"), func() {

	gg.It("fills a missing TLS name from the cluster name when TLS verification is on", func() {
		policy := NewClientPolicy()
		policy.TlsConfig = &tls.Config{}
		policy.ClusterName = "prod.cluster"

		named := defaultSeedTLSNames(policy, []*Host{NewHost("found.internal", 3000)})

		gm.Expect(named).To(gm.HaveLen(1))
		gm.Expect(named[0].Name).To(gm.Equal("found.internal"))
		gm.Expect(named[0].TLSName).To(gm.Equal("prod.cluster"))
	})

	gg.It("leaves TLS names unchanged when TLS verification is off", func() {
		policy := NewClientPolicy()
		seed := NewHost("found.internal", 3000)

		named := defaultSeedTLSNames(policy, []*Host{seed})

		gm.Expect(named[0]).To(gm.BeIdenticalTo(seed))
		gm.Expect(named[0].TLSName).To(gm.BeEmpty())
	})
})

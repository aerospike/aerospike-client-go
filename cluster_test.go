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
	"sync/atomic"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

var _ = gg.Describe("Translation collision detection", func() {

	newTestPeer := func(nodeName string, hosts ...*Host) *peer {
		return &peer{nodeName: nodeName, hosts: hosts}
	}

	gg.It("marks both new peers that share one translated address", func() {
		cluster := &Cluster{translatorName: "port-map"}
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{
			newTestPeer("BB9010011AC4202", NewHost("gateway.example.com", 30001)),
			newTestPeer("BB9020011AC4202", NewHost("gateway.example.com", 30001)),
		})

		cluster.detectTranslationCollisions(tendPeers)

		gm.Expect(tendPeers.collided.Exists("BB9010011AC4202")).To(gm.BeTrue())
		gm.Expect(tendPeers.collided.Exists("BB9020011AC4202")).To(gm.BeTrue())
	})

	gg.It("keeps distinct addresses apart", func() {
		cluster := &Cluster{translatorName: "port-map"}
		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{
			newTestPeer("BB9010011AC4202", NewHost("gateway.example.com", 30001)),
			newTestPeer("BB9020011AC4202", NewHost("gateway.example.com", 30002)),
		})

		cluster.detectTranslationCollisions(tendPeers)

		gm.Expect(tendPeers.collided.Exists("BB9010011AC4202")).To(gm.BeFalse())
		gm.Expect(tendPeers.collided.Exists("BB9020011AC4202")).To(gm.BeFalse())
	})

	gg.It("keeps the live node and marks only the newcomer on a clash", func() {
		liveNode := &Node{name: "BB9010011AC4202", host: NewHost("gateway.example.com", 30001)}
		cluster := &Cluster{translatorName: "pass-through", nodesMap: *sm.New[string, *Node](4)}
		cluster.nodes.Set([]*Node{liveNode})
		cluster.nodesMap.Set(liveNode.name, liveNode)

		tendPeers := newPeers(8, 8)
		tendPeers.appendPeers([]*peer{
			newTestPeer("BB9090011AC4202", NewHost("gateway.example.com", 30001)),
		})

		cluster.detectTranslationCollisions(tendPeers)

		gm.Expect(tendPeers.collided.Exists("BB9090011AC4202")).To(gm.BeTrue())
		gm.Expect(tendPeers.collided.Exists("BB9010011AC4202")).To(gm.BeFalse())
	})

	gg.It("claims each validated address exactly once", func() {
		tendPeers := newPeers(8, 8)

		gm.Expect(tendPeers.claimAddress(hostAndPort{host: "10.0.1.10", port: 3000}, "A")).To(gm.BeEmpty())
		gm.Expect(tendPeers.claimAddress(hostAndPort{host: "10.0.1.10", port: 3000}, "A")).To(gm.BeEmpty())
		gm.Expect(tendPeers.claimAddress(hostAndPort{host: "10.0.1.10", port: 3000}, "B")).To(gm.Equal("A"))
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

var _ = gg.Describe("Cluster view change notification", func() {

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

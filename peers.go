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

import (

	"github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

type peers struct {
	_peers         sm.Map[string, *peer]
	_nodes         sm.Map[string, *Node]
	_nodesToRemove sm.Map[string, *Node]
	refreshCount   atomic.Int
	genChanged     atomic.Bool
}

// newPeers creates a new peers object
func newPeers(peerCapacity int, addCapacity int) *peers {
	return &peers{
		_peers:         *sm.New[string, *peer](peerCapacity),
		_nodes:         *sm.New[string, *Node](addCapacity),
		_nodesToRemove: *sm.New[string, *Node](addCapacity),
		genChanged:     *atomic.NewBool(true),
	}
}

// addNode adds a node to the nodes map
func (ps *peers) addNode(name string, node *Node) {
	ps._nodes.Set(name, node)
}

// nodeByName returns a node by name
func (ps *peers) nodeByName(name string) *Node {
	return ps._nodes.Get(name)
}

// appendPeers adds a list of peers to the peers map
// appendPears appends peers to the peers
func (ps *peers) appendPeers(peers []*peer) {
	for _, peer := range peers {
		ps._peers.Set(peer.nodeName, peer)
	}
}

// peers returns a copy of peers for safe iteration
func (ps *peers) peers() []*peer {
	return sm.MapAllF(&ps._peers, func(m map[string]*peer) []*peer {
		res := make([]*peer, 0, len(m))
		for _, peer := range m {
			res = append(res, peer)
		}
		return res
	})
}

// nodes returns a copy of nodes for safe iteration
func (ps *peers) nodes() map[string]*Node {
	return ps._nodes.Clone()
}

// addNodesToRemove adds a node to the removal list
func (ps *peers) addNodesToRemove(removeNode *Node) {
	ps._nodesToRemove.Set(removeNode.String(), removeNode)
}

// getNodesToRemove returns a copy of nodes to remove for safe iteration
func (ps *peers) getNodesToRemove() []*Node {
	return sm.MapAllF(&ps._nodesToRemove, func(m map[string]*Node) []*Node {
		res := make([]*Node, 0, len(m))
		for _, node := range m {
			res = append(res, node)
		}
		return res
	})
}

// containsNodeToRemove checks if a node is already marked for removal
func (ps *peers) containsNodeToRemove(node *Node) bool {
	return ps._nodesToRemove.Exists(node.String())
}

// peer is a peer of a node
type peer struct {
	nodeName    string
	tlsName     string
	hosts       []*Host
	replaceNode *Node
}

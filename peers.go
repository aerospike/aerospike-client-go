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
	"sync"

	"github.com/aerospike/aerospike-client-go/v8/internal/atomic"
)

type peers struct {
	_peers         map[string]*peer
	_hosts         map[Host]struct{}
	_nodes         map[string]*Node
	_nodesToRemove []*Node
	refreshCount   atomic.Int
	genChanged     atomic.Bool

	mutex sync.RWMutex
}

// newPeers creates a new peers object
func newPeers(peerCapacity int, addCapacity int) *peers {
	return &peers{
		_peers:         make(map[string]*peer, peerCapacity),
		_hosts:         make(map[Host]struct{}, addCapacity),
		_nodes:         make(map[string]*Node, addCapacity),
		_nodesToRemove: make([]*Node, 0),
		genChanged:     *atomic.NewBool(true),
	}
}

// todo: not used anywhere. Consider removing
// hostExists checks if a host exists in the hosts map
func (ps *peers) hostExists(host Host) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()
	_, exists := ps._hosts[host]
	return exists
}

// todo: not used anywhere. Consider removing
// addHost adds a host to the hosts map
func (ps *peers) addHost(host Host) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ps._hosts[host] = struct{}{}
}

// addNode adds a node to the nodes map
func (ps *peers) addNode(name string, node *Node) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ps._nodes[name] = node
}

// nodeByName returns a node by name
func (ps *peers) nodeByName(name string) *Node {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()
	return ps._nodes[name]
}

// appendPeers adds a list of peers to the peers map
func (ps *peers) appendPeers(peers []*peer) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	for _, peer := range peers {
		ps._peers[peer.nodeName] = peer
	}
}

// peers returns a copy of peers for safe iteration
func (ps *peers) peers() []*peer {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	res := make([]*peer, 0, len(ps._peers))
	for _, peer := range ps._peers {
		res = append(res, peer)
	}
	return res
}

// nodes returns a copy of nodes for safe iteration
func (ps *peers) nodes() map[string]*Node {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()
	return ps._nodes
}

// addNodesToRemove adds a node to the removal list
func (ps *peers) addNodesToRemove(removeNode *Node) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps._nodesToRemove = append(ps._nodesToRemove, removeNode)
}

// getNodesToRemove returns a copy of nodes to remove for safe iteration
func (ps *peers) getNodesToRemove() []*Node {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	// Return a copy to prevent race conditions
	result := make([]*Node, len(ps._nodesToRemove))
	copy(result, ps._nodesToRemove)
	return result
}

// containsNodeToRemove checks if a node is already marked for removal
func (ps *peers) containsNodeToRemove(node *Node) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for _, entry := range ps._nodesToRemove {
		if entry.Equals(node) {
			return true
		}
	}
	return false
}

// clearNodesToRemove resets the removal list (call after processing)
func (ps *peers) clearNodesToRemove() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps._nodesToRemove = ps._nodesToRemove[:0]
}

type peer struct {
	nodeName    string
	tlsName     string
	hosts       []*Host
	replaceNode *Node
}

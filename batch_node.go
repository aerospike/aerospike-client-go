// Copyright 2013-2015 Aerospike, Inc.
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
	. "github.com/aerospike/aerospike-client-go/types"
)

type batchNode struct {
	Node            *Node
	BatchNamespaces []*batchNamespace
	KeyCapacity     int
}

func newBatchNodeList(cluster *Cluster, keys []*Key) ([]*batchNode, error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "command failed because cluster is empty.")
	}

	nodeCount := len(nodes)
	keysPerNode := len(keys)/nodeCount + 10

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, nodeCount+1)

	for _, key := range keys {
		partition := NewPartitionByKey(key)

		// error not required
		node, _ := cluster.GetNode(partition)
		batchNode := findBatchNode(batchNodes, node)

		if batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, key))
		} else {
			batchNode.AddKey(key)
		}
	}
	return batchNodes, nil
}

func newBatchNode(node *Node, keyCapacity int, key *Key) *batchNode {
	return &batchNode{
		Node:            node,
		KeyCapacity:     keyCapacity,
		BatchNamespaces: []*batchNamespace{newBatchNamespace(&key.namespace, keyCapacity, key)},
	}
}

func (bn *batchNode) AddKey(key *Key) {
	batchNamespace := bn.findNamespace(&key.namespace)

	if batchNamespace == nil {
		bn.BatchNamespaces = append(bn.BatchNamespaces, newBatchNamespace(&key.namespace, bn.KeyCapacity, key))
	} else {
		batchNamespace.keys = append(batchNamespace.keys, key)
	}
}

func (bn *batchNode) findNamespace(ns *string) *batchNamespace {
	for _, batchNamespace := range bn.BatchNamespaces {
		// Note: use both pointer equality and equals.
		if batchNamespace.namespace == ns || *batchNamespace.namespace == *ns {
			return batchNamespace
		}
	}
	return nil
}

func findBatchNode(nodes []*batchNode, node *Node) *batchNode {
	for _, batchNode := range nodes {
		// Note: using pointer equality for performance.
		if batchNode.Node == node {
			return batchNode
		}
	}
	return nil
}

type batchNamespace struct {
	namespace *string
	keys      []*Key
}

func newBatchNamespace(namespace *string, capacity int, key *Key) *batchNamespace {
	return &batchNamespace{
		namespace: namespace,
		keys:      []*Key{key},
	}
}

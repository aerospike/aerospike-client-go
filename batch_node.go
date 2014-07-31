// Copyright 2013-2014 Aerospike, Inc.
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

type BatchNode struct {
	Node            *Node
	BatchNamespaces []*batchNamespace
	KeyCapacity     int
}

func NewBatchNodeList(cluster *Cluster, keys []*Key) ([]*BatchNode, error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.")
	}

	nodeCount := len(nodes)
	keysPerNode := len(keys)/nodeCount + 10

	// Split keys by server node.
	batchNodes := make([]*BatchNode, 0, nodeCount+1)

	for _, key := range keys {
		partition := NewPartitionByKey(key)

		// error not required
		node, _ := cluster.GetNode(partition)
		batchNode := findBatchNode(batchNodes, node)

		if batchNode == nil {
			batchNodes = append(batchNodes, NewBatchNode(node, keysPerNode, key))
		} else {
			batchNode.AddKey(key)
		}
	}
	return batchNodes, nil
}

func NewBatchNode(node *Node, keyCapacity int, key *Key) *BatchNode {
	return &BatchNode{
		Node:            node,
		KeyCapacity:     keyCapacity,
		BatchNamespaces: []*batchNamespace{NewBatchNamespace(key.Namespace(), keyCapacity, key)},
	}
}

func (this *BatchNode) AddKey(key *Key) {
	batchNamespace := this.findNamespace(key.Namespace())

	if batchNamespace == nil {
		this.BatchNamespaces = append(this.BatchNamespaces, NewBatchNamespace(key.Namespace(), this.KeyCapacity, key))
	} else {
		batchNamespace.keys = append(batchNamespace.keys, key)
	}
}

func (this *BatchNode) findNamespace(ns *string) *batchNamespace {
	for _, batchNamespace := range this.BatchNamespaces {
		// Note: use both pointer equality and equals.
		if batchNamespace.namespace == ns || *batchNamespace.namespace == *ns {
			return batchNamespace
		}
	}
	return nil
}

func findBatchNode(nodes []*BatchNode, node *Node) *BatchNode {
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

func NewBatchNamespace(namespace *string, capacity int, key *Key) *batchNamespace {
	return &batchNamespace{
		namespace: namespace,
		keys:      []*Key{key},
	}
}

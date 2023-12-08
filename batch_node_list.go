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

import "github.com/aerospike/aerospike-client-go/v7/types"

func newBatchNodeList(cluster *Cluster, policy *BatchPolicy, keys []*Key, records []*BatchRecord, hasWrite bool) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(keys) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	var errs Error
	for i := range keys {
		var node *Node
		var err Error
		if hasWrite {
			node, err = GetNodeBatchWrite(cluster, keys[i], replicaPolicy, nil, 0)
		} else {
			node, err = GetNodeBatchRead(cluster, keys[i], replicaPolicy, replicaPolicySC, nil, 0, 0)
		}

		if err != nil {
			if len(records) > 0 {
				records[i].Err = chainErrors(err, records[i].Err)
			} else {
				errs = chainErrors(err, errs)
			}
			// return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, i))
		} else {
			batchNode.AddKey(i)
		}
	}

	return batchNodes, errs
}

func newBatchNodeListKeys(cluster *Cluster, policy *BatchPolicy, keys []*Key, records []*BatchRecord, sequenceAP, sequenceSC int, batchSeed *batchNode, hasWrite bool) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(keys) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	var errs Error
	for _, offset := range batchSeed.offsets {
		var node *Node
		var err Error
		if hasWrite {
			node, err = GetNodeBatchWrite(cluster, keys[offset], replicaPolicy, batchSeed.Node, sequenceAP)
		} else {
			node, err = GetNodeBatchRead(cluster, keys[offset], replicaPolicy, replicaPolicySC, batchSeed.Node, sequenceAP, sequenceSC)
		}

		if err != nil {
			errs = chainErrors(err, errs)
			// return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, offset))
		} else {
			batchNode.AddKey(offset)
		}
	}
	return batchNodes, errs
}

func newBatchNodeListRecords(cluster *Cluster, policy *BatchPolicy, records []*BatchRead, sequenceAP, sequenceSC int, batchSeed *batchNode) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(batchSeed.offsets) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	for _, offset := range batchSeed.offsets {
		node, err := GetNodeBatchRead(cluster, records[offset].Key, replicaPolicy, replicaPolicySC, batchSeed.Node, sequenceAP, sequenceSC)
		if err != nil {
			return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, offset))
		} else {
			batchNode.AddKey(offset)
		}
	}
	return batchNodes, nil
}

func newBatchIndexNodeList(cluster *Cluster, policy *BatchPolicy, records []*BatchRead) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(records) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	for i := range records {
		node, err := GetNodeBatchRead(cluster, records[i].Key, replicaPolicy, replicaPolicySC, nil, 0, 0)
		if err != nil {
			return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, i))
		} else {
			batchNode.AddKey(i)
		}
	}
	return batchNodes, nil
}

func newBatchOperateNodeListIfcRetry(cluster *Cluster, policy *BatchPolicy, records []BatchRecordIfc, sequenceAP, sequenceSC int, batchSeed *batchNode) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(records) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	var errs Error
	for _, offset := range batchSeed.offsets {
		b := records[offset]

		if b.resultCode() != types.NO_RESPONSE {
			// Do not retry keys that already have a response.
			continue
		}

		var node *Node
		var err Error
		if b.isWrite() {
			node, err = GetNodeBatchWrite(cluster, b.key(), replicaPolicy, batchSeed.Node, sequenceAP)
		} else {
			node, err = GetNodeBatchRead(cluster, b.key(), replicaPolicy, replicaPolicySC, batchSeed.Node, sequenceAP, sequenceSC)
		}

		if err != nil {
			errs = chainErrors(err, errs)
			// return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, offset))
		} else {
			batchNode.AddKey(offset)
		}
	}
	return batchNodes, errs
}

func newBatchOperateNodeListIfc(cluster *Cluster, policy *BatchPolicy, records []BatchRecordIfc) ([]*batchNode, Error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(records) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	replicaPolicy := policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(policy.GetBasePolicy())

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	var errs Error
	for i := range records {
		b := records[i]
		b.prepare()

		var node *Node
		var err Error
		if b.isWrite() {
			node, err = GetNodeBatchWrite(cluster, b.key(), replicaPolicy, nil, 0)
		} else {
			node, err = GetNodeBatchRead(cluster, b.key(), replicaPolicy, replicaPolicySC, nil, 0, 0)
		}

		if err != nil {
			records[i].chainError(err)
			records[i].setError(node, err.resultCode(), false)
			// Don't interrupt the batch request because of INVALID_NAMESPACE error
			// These keys will not be sent to the server
			if !err.Matches(types.INVALID_NAMESPACE) {
				errs = chainErrors(err, errs)
			}
			continue
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, i))
		} else {
			batchNode.AddKey(i)
		}
	}
	return batchNodes, errs
}

func newGrpcBatchOperateListIfc(policy *BatchPolicy, records []BatchRecordIfc) (*batchNode, Error) {
	// Split keys by server node.
	batchNode := new(batchNode)
	for i := range records {
		b := records[i]
		b.prepare()
		batchNode.AddKey(i)
	}

	return batchNode, nil
}

func findBatchNode(nodes []*batchNode, node *Node) *batchNode {
	for i := range nodes {
		// Note: using pointer equality for performance.
		if nodes[i].Node == node {
			return nodes[i]
		}
	}
	return nil
}

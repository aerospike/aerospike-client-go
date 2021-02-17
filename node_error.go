// Copyright 2014-2021 Aerospike, Inc.
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

import "github.com/aerospike/aerospike-client-go/types"

func newNodeError(node *Node, err error) *AerospikeError {
	return &AerospikeError{
		error: err,
		Node:  node,
	}
}

func newAerospikeNodeError(node *Node, code types.ResultCode, messages ...string) *AerospikeError {
	return &AerospikeError{
		error: NewAerospikeError(code, messages...),
		Node:  node,
	}
}

func newInvalidNodeError(clusterSize int, partition *Partition) error {
	// important to check for clusterSize first, since partition may be nil sometimes
	if clusterSize == 0 {
		return NewAerospikeError(types.INVALID_NODE_ERROR, "Cluster is empty.")
	}
	return NewAerospikeError(types.INVALID_NODE_ERROR, "Node not found for partition "+partition.String()+" in partition table.")
}

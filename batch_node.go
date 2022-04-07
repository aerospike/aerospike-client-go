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

import "fmt"

type batchNode struct {
	Node    *Node
	offsets []int
}

func newBatchNode(node *Node, capacity int, offset int) *batchNode {
	res := &batchNode{
		Node:    node,
		offsets: make([]int, 1, capacity),
	}

	res.offsets[0] = offset
	return res
}

func (bn *batchNode) AddKey(offset int) {
	bn.offsets = append(bn.offsets, offset)
}

func (bn *batchNode) String() string {
	return fmt.Sprintf("Node: %s, Offsets: %v", bn.Node.String(), bn.offsets)

}

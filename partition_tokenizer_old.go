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

// Parse node partitions using old protocol. This is more code than a String.split() implementation,
// but it's faster because there are much fewer interim strings.

type PartitionTokenizerOld struct {
	replicasName string //= "replicas-write";

	sb     string
	buffer []byte
	length int
	offset int
}

func NewPartitionTokenizerOld(conn *Connection) (*PartitionTokenizerOld, error) {
	return nil, nil
}

func (this *PartitionTokenizerOld) UpdatePartition(hmap map[string]atomicNodeArray, node *Node) map[string]atomicNodeArray {
	return nil
}

func (this *PartitionTokenizerOld) getNext() (*Partition, error) {
	return nil, nil
}

func (this *PartitionTokenizerOld) getTruncatedResponse() string {
	return ""
}

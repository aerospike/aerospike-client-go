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
	Buffer "github.com/citrusleaf/go-client/utils/buffer"
)

type SingleCommand struct {
	BaseCommand

	cluster   *Cluster
	key       *Key
	partition *Partition
}

func NewSingleCommand(cluster *Cluster, key *Key) *SingleCommand {
	return &SingleCommand{
		cluster:   cluster,
		key:       key,
		partition: NewPartitionByKey(key),
	}
}

func (this *SingleCommand) getNode(ifc Command) (*Node, error) {
	return this.cluster.GetNode(this.partition)
}

func (this *SingleCommand) emptySocket(conn *Connection) error {
	// There should not be any more bytes.
	// Empty the socket to be safe.
	sz := Buffer.BytesToInt64(this.dataBuffer, 0)
	headerLength := this.dataBuffer[8]
	receiveSize := int(sz&0xFFFFFFFFFFFF) - int(headerLength)

	// Read remaining message bytes.
	if receiveSize > 0 {
		this.sizeBufferSz(receiveSize)
		conn.Read(this.dataBuffer, receiveSize)
	}
	return nil
}

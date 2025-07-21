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
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
)

func DefaultTimeout() time.Duration {
	return _DEFAULT_TIMEOUT
}

func ParseInfoErrorCode(response string) Error {
	return parseInfoErrorCode(response)
}

func (e *AerospikeError) Msg() string {
	return e.msg
}

func (clstr *Cluster) GetMasterNode(partition *Partition) (*Node, Error) {
	return partition.getMasterNode(clstr)
}

// implements GomegaStringer to avoid some of the pain points
// in formatting the code
func (nd *Node) GomegaString() string {
	return nd.String()
}

func (ptn *Partition) GetMasterNode(cluster *Cluster) (*Node, Error) {
	return ptn.getMasterNode(cluster)
}

func (ptn *Partition) GetMasterProlesNode(cluster *Cluster) (*Node, Error) {
	return ptn.getMasterProlesNode(cluster)
}

// fillMinCounts will fill the connection pool to the minimum required
// by the ClientPolicy.MinConnectionsPerNode
func (nd *Node) ConnsCount() int {
	return nd.connectionCount.Get()
}

// CloseConnections closes all the node connections
func (nd *Node) CloseConnections() {
	nd.closeConnections()
}

// PartitionForWrite returns a partition for write purposes
func ConfiguredAsStrongConsistency(client *Client, namespace string) bool {
	// Must copy hashmap reference for copy on write semantics to work.
	pmap := client.cluster.getPartitions()
	p := pmap[namespace]
	if p == nil {
		return false
	}

	return p.SCMode
}

func NewWriteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	bins []*Bin,
	binMap BinMap) (writeCommand, Error) {
	return newWriteCommand(
		cluster,
		policy,
		key,
		bins,
		binMap,
		_WRITE)
}

func (cmd *writeCommand) WriteBuffer(ifc command) Error {
	return cmd.writeBuffer(ifc)
}

func (cmd *writeCommand) Buffer() []byte {
	return cmd.dataBuffer[:cmd.dataOffset]
}

func NewDeleteCommand(cluster *Cluster, policy *WritePolicy, key *Key) (*deleteCommand, Error) {
	return newDeleteCommand(cluster, policy, key)
}

func (cmd *deleteCommand) WriteBuffer(ifc command) Error {
	return cmd.writeBuffer(ifc)
}

func (cmd *deleteCommand) Buffer() []byte {
	return cmd.dataBuffer[:cmd.dataOffset]
}

func (ctn *Connection) UpdateDeadline() (time.Time, time.Time, time.Duration, Error) {
	err := ctn.updateDeadline()
	return ctn.deadline, ctn.socketDeadline, ctn.socketTimeout, err
}

func (node *Node) ValidateErrorCount() Error {
	return node.validateErrorCount()
}

func (node *Node) IncrementErrorCount() {
	node.incrErrorCount()
}

func (clstr *Cluster) GetTendCount() int {
	return clstr.tendCount.Get()
}

func (clstr *Cluster) SetTendCount(value int) {
	clstr.tendCount.Set(value)
}

func (node *Node) GetCluster() *Cluster {
	return node.cluster
}

func (node *Node) SimulateTendAdvancement(count int) {
	for i := 0; i < count; i++ {
		node.cluster.tendCount.IncrementAndGet()
	}
}

func (node *Node) SetMaxErrorCount(value int) {
	node.maxErrorCount.Set(value)
}

func (node *Node) GetErrorCount() int {
	return node.errorCount.Get()
}

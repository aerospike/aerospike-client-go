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
	"io"
	"time"

	"github.com/aerospike/aerospike-client-go/v7/types"
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

func (k *Key) HasValueToSend() bool {
	return k.hasValueToSend()
}

func (k *Key) RemoveValue() {
	k.userKey = nil
}

func (ctn *Connection) UpdateDeadline() (time.Time, time.Time, time.Duration, Error) {
	err := ctn.updateDeadline()
	return ctn.deadline, ctn.socketDeadline, ctn.socketTimeout, err
}

// QueryReplyWire builds a header-only query reply carrying resultCode and the
// INFO3_LAST and INFO3_PARTITION_DONE flags. The server overloads generation
// as the partition id on PARTITION_DONE replies.
func QueryReplyWire(resultCode types.ResultCode, last, partitionDone bool, partitionID uint32) []byte {
	info3 := byte(0)
	if last {
		info3 |= byte(_INFO3_LAST)
	}
	if partitionDone {
		info3 |= byte(_INFO3_PARTITION_DONE)
	}
	return []byte{
		0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, // proto: version 2, type 3, size 22
		0x16,              // header length
		0x00, 0x00, info3, // info1..info3
		0x00, // unused
		byte(resultCode),
		byte(partitionID >> 24), byte(partitionID >> 16), byte(partitionID >> 8), byte(partitionID), // generation
		0x00, 0x00, 0x00, 0x00, // record ttl
		0x00, 0x00, 0x00, 0x00, // transaction ttl
		0x00, 0x00, // field count
		0x00, 0x00, // op count
	}
}

// ReplyOutcome is what a query or scan partition command did with replayed server replies.
type ReplyOutcome struct {
	Err              Error
	Records          int
	PartsUnavailable int
	PartitionRetry   bool
}

// ReplayQueryReplies feeds replies, in order, to a query partition command's
// parseResult over an in-memory connection and reports what it did with them.
// PartitionRetry is read for partitionID.
func ReplayQueryReplies(partitionID int, replies ...[]byte) ReplyOutcome {
	policy := NewQueryPolicy()
	_, tracker, nodeParts, recordset := replayFixtures(&policy.MultiPolicy)
	cmd := newQueryPartitionCommand(policy, tracker, nodeParts, NewStatement("test", "set"), recordset)
	return replayReplies(cmd, &cmd.baseMultiCommand, tracker, nodeParts, recordset, partitionID, replies)
}

// ReplayScanReplies is ReplayQueryReplies for a scan partition command.
func ReplayScanReplies(partitionID int, replies ...[]byte) ReplyOutcome {
	policy := NewScanPolicy()
	_, tracker, nodeParts, recordset := replayFixtures(&policy.MultiPolicy)
	cmd := newScanPartitionCommand(policy, tracker, nodeParts, "test", "set", nil, recordset)
	return replayReplies(cmd, &cmd.baseMultiCommand, tracker, nodeParts, recordset, partitionID, replies)
}

func replayFixtures(policy *MultiPolicy) (*Node, *partitionTracker, *nodePartitions, *Recordset) {
	node := &Node{cluster: &Cluster{}}
	node.active.Set(true)
	tracker := newPartitionTrackerForNodes(policy, []*Node{node})
	return node, tracker, newNodePartitions(node, 16), newRecordset(policy.RecordQueueSize, 1)
}

func replayReplies(cmd command, base *baseMultiCommand, tracker *partitionTracker, nodeParts *nodePartitions, recordset *Recordset, partitionID int, replies [][]byte) ReplyOutcome {
	var wire []byte
	for _, reply := range replies {
		wire = append(wire, reply...)
	}

	buf := buffPool.Get(DefaultBufferSize)
	defer buffPool.Put(buf)

	conn := &Connection{
		conn:           &bytesConn{buf: wire},
		limitReader:    &io.LimitedReader{R: nil, N: 0},
		dataBuffer:     buf,
		origDataBuffer: buf,
	}
	conn.limitReader.R = conn.conn
	base.conn = conn
	base.dataBuffer = buf

	err := base.parseResult(cmd, conn)

	return ReplyOutcome{
		Err:              err,
		Records:          len(recordset.records),
		PartsUnavailable: nodeParts.partsUnavailable,
		PartitionRetry:   tracker.partitions[partitionID].Retry,
	}
}

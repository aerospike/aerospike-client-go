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
	"bufio"
	"iter"
	"time"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type singleCommand struct {
	baseCommand

	cluster   *Cluster
	key       *Key
	partition *Partition
}

func newSingleCommand(cluster *Cluster, key *Key, partition *Partition) singleCommand {
	return singleCommand{
		baseCommand: baseCommand{},
		cluster:     cluster,
		key:         key,
		partition:   partition,
	}
}

func (cmd *singleCommand) getConnection(policy Policy) (*Connection, Error) {
	return cmd.node.getConnectionWithHint(policy.GetBasePolicy().TotalTimeout, policy.GetBasePolicy().SocketTimeout, cmd.key.digest[0], policy.GetBasePolicy().TimeoutDelay)
}

func (cmd *singleCommand) putConnection(conn *Connection) {
	cmd.node.putConnectionWithHint(conn, cmd.key.digest[0])
}
func (cmd *singleCommand) emptySocket(conn *Connection) Error {
	// There should not be any more bytes.
	// Empty the socket to be safe.
	sz := Buffer.BytesToInt64(cmd.dataBuffer, 0)
	headerLength := cmd.dataBuffer[8]
	receiveSize := int(sz&0xFFFFFFFFFFFF) - int(headerLength)

	// Read remaining message bytes.
	if receiveSize > 0 {
		if err := cmd.sizeBufferSz(receiveSize, false); err != nil {
			return err
		}
		if _, err := conn.Read(cmd.dataBuffer, receiveSize); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *singleCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *singleCommand) getNamespace() *string {
	return &cmd.key.namespace
}

func (cmd *singleCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
	// If the connection is already closed, don't bother trying to salvage it.
	if cmd.conn != nil && !cmd.conn.IsConnected() {
		return
	}

	conn.deadline = time.Now().Add(timeoutDelay)

	reader := bufio.NewReader(conn.conn)
	discardedCount := int(cmd.receiveSize - conn.totalReceived)

	for discardedCount > 0 {
		var discarded int
		var err error
		if discarded, err = reader.Discard(discardedCount); err != nil {
			if discarded < discardedCount {
				conn.Close()
				cmd.conn = nil
				return
			}
		}
		discardedCount -= discarded
	}

	conn.refresh()
	node.PutConnection(conn)

	// Record connection recovery metrics
	applyConnectionRecoveredMetrics(cmd.node)
}

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
	"iter"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type batcher interface {
	command

	cloneBatchCommand(batch *batchNode) batcher
	filteredOut() int

	retryBatch(ifc batcher, cluster *Cluster, iteration int) (bool, Error)
	generateBatchNodes(*Cluster) ([]*batchNode, Error)
	setSequence(int, int)

	// executeSingle(*Client) Error
	setInDoubt(batcher)
	inDoubt()
}

type batchCommand struct {
	baseMultiCommand

	client     *Client
	batch      *batchNode
	policy     *BatchPolicy
	sequenceAP int
	sequenceSC int

	splitRetry bool

	filteredOutCnt int
}

func (cmd *batchCommand) setInDoubt(ifc batcher) {
	// Set error/inDoubt for keys associated this batch command when
	// the command was not retried and split. If a split retry occurred,
	// those new subcommands have already set inDoubt on the affected
	// subset of keys.
	if !cmd.splitRetry {
		ifc.inDoubt()
	}
}

func (cmd *batchCommand) inDoubt() {
	// do nothing by defaut
}

func (cmd *batchCommand) prepareRetry(ifc command, isTimeout bool) bool {
	if !(cmd.policy.ReplicaPolicy == SEQUENCE || cmd.policy.ReplicaPolicy == PREFER_RACK) {
		// Perform regular retry to same node.
		return true
	}

	cmd.sequenceAP++

	if !isTimeout || cmd.policy.ReadModeSC != ReadModeSCLinearize {
		cmd.sequenceSC++
	}
	return false
}

func (cmd *batchCommand) retryBatch(ifc batcher, cluster *Cluster, iteration int) (bool, Error) {
	// Retry requires keys for this node to be split among other nodes.
	// This is both recursive and exponential.
	batchNodes, err := ifc.generateBatchNodes(cluster)
	if err != nil {
		return false, err
	}

	if len(batchNodes) == 1 && batchNodes[0].Node == cmd.batch.Node {
		// Batch node is the same. Go through normal retry.
		return false, nil
	}

	cmd.splitRetry = true

	// Run batch requests sequentially in same goroutine.
	var ferr Error
	for _, batchNode := range batchNodes {
		command := ifc.cloneBatchCommand(batchNode)
		command.setSequence(cmd.sequenceAP, cmd.sequenceSC)
		if err := command.executeIter(command, iteration); err != nil {
			ferr = chainErrors(err, ferr)
			if !cmd.policy.AllowPartialResults {
				return false, ferr
			}
		}
	}

	return true, ferr
}

func (cmd *batchCommand) setSequence(ap, sc int) {
	cmd.sequenceAP, cmd.sequenceSC = ap, sc
}

func (cmd *batchCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchCommand) commandType() commandType {
	return ttNone
}

func (cmd *batchCommand) Execute() Error {
	err := cmd.execute(cmd)
	if err != nil {
		cmd.setInDoubt(cmd)
	}
	return err
}

func (cmd *batchCommand) filteredOut() int {
	return cmd.filteredOutCnt
}

func (cmd *batchCommand) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	panic(unreachable)
}

func (cmd *batchCommand) cloneBatchCommand(batch *batchNode) batcher {
	panic(unreachable)
}

func (cmd *batchCommand) writeBuffer(ifc command) Error {
	panic(unreachable)
}

func (cmd *batchCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *batchCommand) getNamespace() *string {
	return &cmd.namespace
}

func (cmd *batchCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
	// If the connection is not connected, there is nothing more to salvage.
	if !conn.IsConnected() && cmd.status != _STATE_PARSING_RESPONSE {
		return
	}

	cmd.parseSalvageConn(conn, timeoutDelay)

	if cmd.status == _STATE_PARSING_RESPONSE_ERROR {
		conn.Close()
		return
	}

	conn.refresh()
	node.PutConnection(conn)
	// Record connection recovery metrics
	applyConnectionRecoveredMetrics(node)
}

func (cmd *batchCommand) parseSalvageConn(conn *Connection, timeoutDelay time.Duration) {
	var err Error

	cmd.bc = newBufferedConn(conn, 0)
	cmd.status = _STATE_PARSING_RESPONSE
	for cmd.status == _STATE_PARSING_RESPONSE {
		// Make sure the underlying connection is not nil
		// if not nil then we are not going to attempt to parse the connection
		if err = cmd.bc.conn.initInflater(false, 0); err != nil {
			cmd.status = _STATE_PARSING_RESPONSE_ERROR
			return
		}
		cmd.bc.reset(8)

		// Read header. If we can't read the header, then we can't parse or get how much
		// data we need to read and discard
		if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
			return
		}

		proto := Buffer.BytesToInt64(cmd.dataBuffer, 0)
		receiveSize := int(proto & 0xFFFFFFFFFFFF)
		if receiveSize <= 0 {
			continue
		}

		if compressedSize := cmd.compressedSize(); compressedSize > 0 {
			cmd.bc.reset(8)
			// Read header.
			if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
				cmd.status = _STATE_PARSING_RESPONSE_ERROR
				return
			}

			receiveSize = int(Buffer.BytesToInt64(cmd.dataBuffer, 0)) - 8
			if err = cmd.conn.initInflater(true, compressedSize-8); err != nil {
				cmd.status = _STATE_PARSING_RESPONSE_ERROR
				return
			}

			// getting compressed received size
			cmd.receiveSize = int64(receiveSize)

			// read the first 8 bytes
			cmd.bc.reset(8)
			if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
				cmd.status = _STATE_PARSING_RESPONSE_ERROR
				return
			}
		} else {
			// getting un-compressed received size
			cmd.receiveSize = int64(receiveSize)
		}

		// Validate header to make sure we are at the beginning of a message
		proto = Buffer.BytesToInt64(cmd.dataBuffer, 0)
		if err = cmd.validateHeader(proto); err != nil {
			cmd.status = _STATE_PARSING_RESPONSE_ERROR
			return
		}

		if receiveSize > 0 {
			cmd.salvageConnParseRecord(receiveSize)
			cmd.discardData(conn, timeoutDelay)
		}
	}

	// if the buffer has been resized, put it back so that it will be reassigned to the connection.
	cmd.dataBuffer = cmd.bc.buf()
}

func (cmd *baseMultiCommand) salvageConnParseRecord(receiveSize int) {
	// Read/parse remaining message bytes one record at a time.
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			cmd.status = _STATE_PARSING_RESPONSE_ERROR
			return
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		if resultCode != 0 && resultCode != types.PARTITION_UNAVAILABLE {
			if resultCode == types.KEY_NOT_FOUND_ERROR || resultCode == types.FILTERED_OUT {
				cmd.status = _STATE_PARSING_RESPONSE_ERROR
				return
			}

			cmd.status = _STATE_PARSING_RESPONSE_ERROR
			return
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			cmd.status = _STATE_PARSING_RESPONSE_DONE
			return
		}
	}
}

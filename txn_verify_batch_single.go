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
	"fmt"
	"iter"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// guarantee batchSingleTxnVerifyCommand implements command interface
var _ command = &batchSingleTxnVerifyCommand{}

type batchSingleTxnVerifyCommand struct {
	singleCommand

	record  *BatchRecord
	policy  *BatchPolicy
	version *uint64
}

func newBatchSingleTxnVerifyCommand(
	client *Client,
	policy *BatchPolicy,
	version *uint64,
	record *BatchRecord,
	node *Node,
) (batchSingleTxnVerifyCommand, Error) {
	var partition *Partition
	var err Error
	if client.cluster != nil {
		partition, err = PartitionForRead(client.cluster, &policy.BasePolicy, record.Key)
		if err != nil {
			return batchSingleTxnVerifyCommand{}, err
		}
	}

	res := batchSingleTxnVerifyCommand{
		singleCommand: newSingleCommand(client.cluster, record.Key, partition),
		record:        record,
		policy:        policy,
		version:       version,
	}
	res.node = node

	return res, nil
}

func (cmd *batchSingleTxnVerifyCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchSingleTxnVerifyCommand) writeBuffer(ifc command) Error {
	return cmd.setTxnVerify(&cmd.policy.BasePolicy, cmd.key, *cmd.version)
}

func (cmd *batchSingleTxnVerifyCommand) getNode(ifc command) (*Node, Error) {
	return cmd.node, nil
}

func (cmd *batchSingleTxnVerifyCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryRead(isTimeout)
	node, err := cmd.partition.GetNodeRead(cmd.cluster)
	if err != nil {
		return false
	}

	cmd.node = node
	return true
}

func (cmd *batchSingleTxnVerifyCommand) parseResult(ifc command, conn *Connection) Error {
	// Read proto and check if compressed
	if _, err := conn.Read(cmd.dataBuffer, 8); err != nil {
		logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
		return err
	}

	if compressedSize := cmd.compressedSize(); compressedSize > 0 {
		// Read compressed size
		if _, err := conn.Read(cmd.dataBuffer, 8); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}

		if err := cmd.conn.initInflater(true, compressedSize); err != nil {
			return newError(types.PARSE_ERROR, fmt.Sprintf("Error setting up zlib inflater for size `%d`: %s", compressedSize, err.Error()))
		}

		// Read header.
		if _, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE)); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}
	} else {
		// Read header.
		if _, err := conn.Read(cmd.dataBuffer[8:], int(_MSG_TOTAL_HEADER_SIZE)-8); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}
	}

	// A number of these are commented out because we just don't care enough to read
	// that section of the header. If we do care, uncomment and check!
	sz := Buffer.BytesToInt64(cmd.dataBuffer, 0)

	// Validate header to make sure we are at the beginning of a message
	if err := cmd.validateHeader(sz); err != nil {
		return err
	}

	headerLength := int(cmd.dataBuffer[8])
	resultCode := types.ResultCode(cmd.dataBuffer[13] & 0xFF)

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, resultCode)
	}

	// generation := Buffer.BytesToUint32(cmd.dataBuffer, 14)
	// expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 18))
	// fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 26)) // almost certainly 0
	// opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 28))
	receiveSize := int((sz & 0xFFFFFFFFFFFF) - int64(headerLength))

	// Read remaining message bytes.
	if receiveSize > 0 {
		if err := cmd.sizeBufferSz(receiveSize, false); err != nil {
			return err
		}
		if _, err := conn.Read(cmd.dataBuffer, receiveSize); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return err
		}
	}

	if resultCode == 0 {
		cmd.record.ResultCode = types.OK
	} else {
		err := newError(resultCode)
		return err.setInDoubt(cmd.isRead(), cmd.commandSentCounter)
	}

	return nil
}

func (cmd *batchSingleTxnVerifyCommand) setInDoubt() bool {
	if cmd.record.ResultCode == types.NO_RESPONSE {
		cmd.record.InDoubt = true
		return true
	}
	return false
}

func (cmd *batchSingleTxnVerifyCommand) isRead() bool {
	return true
}

func (cmd *batchSingleTxnVerifyCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchSingleTxnVerifyCommand) commandType() commandType {
	return ttPut
}

func (cmd *batchSingleTxnVerifyCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *batchSingleTxnVerifyCommand) getNamespace() *string {
	return &cmd.key.namespace
}

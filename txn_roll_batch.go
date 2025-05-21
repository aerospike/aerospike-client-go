// Copyright 2014-2024 Aerospike, Inc.
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
	"reflect"

	"github.com/aerospike/aerospike-client-go/v8/types"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type batchTxnRollCommand struct {
	batchCommand

	txn     *Txn
	keys    []*Key
	records []*BatchRecord
	attr    *batchAttr
}

func newBatchTxnRollCommand(
	client *Client,
	batch *batchNode,
	policy *BatchPolicy,
	txn *Txn,
	keys []*Key,
	records []*BatchRecord,
	attr *batchAttr,
) *batchTxnRollCommand {
	var node *Node
	if batch != nil {
		node = batch.Node
	}

	res := &batchTxnRollCommand{
		batchCommand: batchCommand{
			client:           client,
			baseMultiCommand: *newMultiCommand(node, nil, false),
			policy:           policy,
			batch:            batch,
		},
		txn:     txn,
		keys:    keys,
		records: records,
		attr:    attr,
	}
	return res
}

func (cmd *batchTxnRollCommand) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch

	return &res
}

func (cmd *batchTxnRollCommand) buf() []byte {
	return cmd.dataBuffer
}

func (cmd *batchTxnRollCommand) object(index int) *reflect.Value {
	return nil
}

func (cmd *batchTxnRollCommand) writeBuffer(ifc command) Error {
	return cmd.setBatchTxnRoll(cmd.policy, cmd.txn, cmd.keys, cmd.batch, cmd.attr)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchTxnRollCommand) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		// Aggregate metrics
		metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
		if metricsEnabled {
			cmd.node.stats.updateOrInsert(ifc, resultCode)
		}

		// The only valid server return codes are "ok" and "not found" and "filtered out".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != types.KEY_NOT_FOUND_ERROR {
			if resultCode == types.FILTERED_OUT {
				cmd.filteredOutCnt++
			} else {
				return false, newCustomNodeError(cmd.node, resultCode)
			}
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		// generation := Buffer.BytesToUint32(cmd.dataBuffer, 6)
		// expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 10))
		batchIndex := int(Buffer.BytesToUint32(cmd.dataBuffer, 14))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		// opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))
		err := cmd.skipKey(fieldCount)
		if err != nil {
			return false, err
		}

		record := cmd.records[batchIndex]

		if resultCode == types.OK {
			record.ResultCode = resultCode
		} else {
			record.setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandSentCounter))
		}
	}

	return true, nil
}

// Parses the given byte buffer and populate the result object.
// Returns the number of bytes that were parsed from the given buffer.
func (cmd *batchTxnRollCommand) parseRecord(key *Key, opCount int, generation, expiration uint32) (*Record, Error) {
	bins := make(BinMap, opCount)

	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			return nil, err
		}
		opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		particleType := int(cmd.dataBuffer[5])
		nameSize := int(cmd.dataBuffer[7])

		if err := cmd.readBytes(nameSize); err != nil {
			return nil, err
		}
		name := string(cmd.dataBuffer[:nameSize])

		particleBytesSize := opSize - (4 + nameSize)
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return nil, err
		}
		value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
		if err != nil {
			return nil, err
		}

		if cmd.isOperation {
			if prev, ok := bins[name]; ok {
				if prev2, ok := prev.(OpResults); ok {
					bins[name] = append(prev2, value)
				} else {
					bins[name] = OpResults{prev, value}
				}
			} else {
				bins[name] = value
			}
		} else {
			bins[name] = value
		}
	}

	return newRecord(cmd.node, key, bins, generation, expiration), nil
}

func (cmd *batchTxnRollCommand) inDoubt() {
	if !cmd.attr.hasWrite {
		return
	}

	for index := range cmd.batch.offsets {
		record := cmd.records[index]

		if record.ResultCode == types.NO_RESPONSE {
			record.InDoubt = true
		}
	}
}

func (cmd *batchTxnRollCommand) commandType() commandType {
	return ttBatchWrite
}

func (cmd *batchTxnRollCommand) executeSingle(client *Client) Error {
	panic(unreachable)
}

func (cmd *batchTxnRollCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchTxnRollCommand) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	return newBatchNodeListKeys(cluster, cmd.policy, cmd.keys, cmd.records, cmd.sequenceAP, cmd.sequenceSC, cmd.batch, cmd.attr.hasWrite)
}

func (cmd *batchTxnRollCommand) getNamespaces() iter.Seq2[string, uint64] {
	return cmd.nsIter
}

func (cmd *batchTxnRollCommand) getNamespace() *string {
	return nil
}

func (cmd *batchTxnRollCommand) nsIter(yield func(string, uint64) bool) {
	for _, key := range cmd.keys {
		if !yield(key.namespace, 1) {
			return
		}
	}
}

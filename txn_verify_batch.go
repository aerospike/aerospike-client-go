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

type txnBatchVerifyCommand struct {
	batchCommand

	keys     []*Key
	versions []*uint64
	records  []*BatchRecord
}

func newTxnBatchVerifyCommand(
	client *Client,
	batch *batchNode,
	policy *BatchPolicy,
	keys []*Key,
	versions []*uint64,
	records []*BatchRecord,
) *txnBatchVerifyCommand {
	var node *Node
	if batch != nil {
		node = batch.Node
	}

	res := &txnBatchVerifyCommand{
		batchCommand: batchCommand{
			client:           client,
			baseMultiCommand: *newMultiCommand(node, nil, false),
			policy:           policy,
			batch:            batch,
		},
		keys:     keys,
		versions: versions,
		records:  records,
	}
	return res
}

func (cmd *txnBatchVerifyCommand) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch

	return &res
}

func (cmd *txnBatchVerifyCommand) buf() []byte {
	return cmd.dataBuffer
}

func (cmd *txnBatchVerifyCommand) object(index int) *reflect.Value {
	return nil
}

func (cmd *txnBatchVerifyCommand) writeBuffer(ifc command) Error {
	return cmd.setBatchTxnVerifyForBatchNode(cmd.policy, cmd.keys, cmd.versions, cmd.batch)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *txnBatchVerifyCommand) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
		if metricsEnabled {
			cmd.node.stats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), cmd.commandType(), resultCode)
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
			record.setError(cmd.node, resultCode, false)
		}
	}
	return true, nil
}

func (cmd *txnBatchVerifyCommand) commandType() commandType {
	return ttBatchRead
}

func (cmd *txnBatchVerifyCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *txnBatchVerifyCommand) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	return newBatchNodeListKeys(cluster, cmd.policy, cmd.keys, cmd.records, cmd.sequenceAP, cmd.sequenceSC, cmd.batch, false)
}

func (cmd *txnBatchVerifyCommand) getNamespaces() iter.Seq2[string, uint64] {
	return cmd.nsIter
}

func (cmd *txnBatchVerifyCommand) getNamespace() *string {
	return nil
}

func (cmd *txnBatchVerifyCommand) nsIter(yield func(string, uint64) bool) {
	for _, key := range cmd.keys {
		if !yield(key.namespace, 1) {
			return
		}
	}
}

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

	"github.com/aerospike/aerospike-client-go/v8/types"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type batchCommandExists struct {
	batchCommand

	keys        []*Key
	existsArray []bool
}

func newBatchCommandExists(
	client *Client,
	batch *batchNode,
	policy *BatchPolicy,
	keys []*Key,
	existsArray []bool,
) *batchCommandExists {
	var node *Node
	if batch != nil {
		node = batch.Node
	}

	res := &batchCommandExists{
		batchCommand: batchCommand{
			client:           client,
			baseMultiCommand: *newMultiCommand(node, nil, false),
			policy:           policy,
			batch:            batch,
		},
		keys:        keys,
		existsArray: existsArray,
	}
	return res
}

func (cmd *batchCommandExists) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch

	return &res
}

func (cmd *batchCommandExists) writeBuffer(ifc command) Error {
	if cmd.batch.Node.SupportsBatchAny() {
		attr := newBatchAttr(cmd.policy, _INFO1_READ|_INFO1_NOBINDATA)
		return cmd.setBatchOperate(cmd.policy, cmd.keys, cmd.batch, nil, nil, attr)
	}
	return cmd.setBatchRead(cmd.policy, cmd.keys, cmd.batch, nil, nil, _INFO1_READ|_INFO1_NOBINDATA)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchCommandExists) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0
	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}

		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)
		// generation := Buffer.BytesToUint32(cmd.dataBuffer, 6)
		// expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 10))
		batchIndex := int(Buffer.BytesToUint32(cmd.dataBuffer, 14))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))

		if len(cmd.keys) > batchIndex {
			err := cmd.parseFieldsRead(fieldCount, cmd.keys[batchIndex])
			if err != nil {
				return false, err
			}
		}

		// The only valid server return codes are "ok" and "not found".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != types.KEY_NOT_FOUND_ERROR {
			if resultCode == types.FILTERED_OUT {
				cmd.filteredOutCnt++
			} else {
				return false, newCustomNodeError(cmd.node, resultCode)
			}
		}

		info3 := cmd.dataBuffer[3]

		// If cmd is the end marker of the response, do not proceed further
		if (int(info3) & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		// Aggregate metrics
		metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
		if metricsEnabled {
			cmd.node.stats.updateOrInsert(ifc, resultCode)
		}

		if opCount > 0 {
			return false, newCustomNodeError(cmd.node, types.PARSE_ERROR, "Received bins that were not requested!")
		}

		if len(cmd.keys) > batchIndex {
			err := cmd.parseFieldsRead(fieldCount, cmd.keys[batchIndex])
			if err != nil {
				return false, err
			}
		}

		// only set the results to true; as a result, no synchronization is needed
		cmd.existsArray[batchIndex] = resultCode == 0
	}
	return true, nil
}

func (cmd *batchCommandExists) commandType() commandType {
	return ttBatchRead
}

func (cmd *batchCommandExists) executeSingle(client *Client) Error {
	var err Error
	for _, offset := range cmd.batch.offsets {
		cmd.existsArray[offset], err = client.Exists(&cmd.policy.BasePolicy, cmd.keys[offset])
		if err != nil {
			// Key not found is NOT an error for batch requests
			if err.resultCode() == types.KEY_NOT_FOUND_ERROR {
				continue
			}

			if err.resultCode() == types.FILTERED_OUT {
				cmd.filteredOutCnt++
				continue
			}

			if cmd.policy.AllowPartialResults {
				continue
			}
			return err
		}
	}
	return nil
}

func (cmd *batchCommandExists) Execute() Error {
	if len(cmd.batch.offsets) == 1 {
		return cmd.executeSingle(cmd.client)
	}
	return cmd.execute(cmd)
}

func (cmd *batchCommandExists) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	return newBatchNodeListKeys(cluster, cmd.policy, cmd.keys, nil, cmd.sequenceAP, cmd.sequenceSC, cmd.batch, false)
}

func (cmd *batchCommandExists) getNamespaces() iter.Seq2[string, uint64] {
	return cmd.nsIter
}

func (cmd *batchCommandExists) getNamespace() *string {
	return nil
}

func (cmd *batchCommandExists) nsIter(yield func(string, uint64) bool) {
	for _, key := range cmd.keys {
		if !yield(key.namespace, 1) {
			return
		}
	}
}

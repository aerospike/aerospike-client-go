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
	"github.com/aerospike/aerospike-client-go/v7/types"
	Buffer "github.com/aerospike/aerospike-client-go/v7/utils/buffer"
)

type batchCommandUDF struct {
	batchCommand

	batchUDFPolicy *BatchUDFPolicy
	keys           []*Key
	packageName    string
	functionName   string
	args           ValueArray
	records        []*BatchRecord
	attr           *batchAttr
}

func newBatchCommandUDF(
	client clientIfc,
	batch *batchNode,
	policy *BatchPolicy,
	batchUDFPolicy *BatchUDFPolicy,
	keys []*Key,
	packageName,
	functionName string,
	args ValueArray,
	records []*BatchRecord,
	attr *batchAttr,
) *batchCommandUDF {
	var node *Node
	if batch != nil {
		node = batch.Node
	}

	res := &batchCommandUDF{
		batchCommand: batchCommand{
			client:           client,
			baseMultiCommand: *newMultiCommand(node, nil, false),
			policy:           policy,
			batch:            batch,
		},
		batchUDFPolicy: batchUDFPolicy,
		keys:           keys,
		records:        records,
		packageName:    packageName,
		functionName:   functionName,
		args:           args,
		attr:           attr,
	}
	return res
}

func (cmd *batchCommandUDF) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch

	return &res
}

func (cmd *batchCommandUDF) writeBuffer(ifc command) Error {
	return cmd.setBatchUDF(cmd.policy, cmd.keys, cmd.batch, cmd.packageName, cmd.functionName, cmd.args, cmd.attr)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchCommandUDF) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		// The only valid server return codes are "ok" and "not found" and "filtered out".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 {
			if resultCode != types.KEY_NOT_FOUND_ERROR {
				if resultCode == types.FILTERED_OUT {
					cmd.filteredOutCnt++
				}
			}

			if resultCode != types.KEY_NOT_FOUND_ERROR && resultCode != types.FILTERED_OUT {
				return false, newCustomNodeError(cmd.node, resultCode)
			}
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		generation := Buffer.BytesToUint32(cmd.dataBuffer, 6)
		expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 10))
		batchIndex := int(Buffer.BytesToUint32(cmd.dataBuffer, 14))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))
		err := cmd.skipKey(fieldCount)
		if err != nil {
			return false, err
		}

		if resultCode == 0 {
			if err = cmd.parseRecord(cmd.records[batchIndex], cmd.keys[batchIndex], opCount, generation, expiration); err != nil {
				return false, err
			}
		} else {
			cmd.records[batchIndex].Err = chainErrors(newCustomNodeError(cmd.node, resultCode), cmd.records[batchIndex].Err)
			cmd.records[batchIndex].setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandSentCounter))
		}
	}
	return true, nil
}

// Parses the given byte buffer and populate the result object.
// Returns the number of bytes that were parsed from the given buffer.
func (cmd *batchCommandUDF) parseRecord(rec *BatchRecord, key *Key, opCount int, generation, expiration uint32) Error {
	bins := make(BinMap, opCount)

	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			return err
		}
		opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		particleType := int(cmd.dataBuffer[5])
		nameSize := int(cmd.dataBuffer[7])

		if err := cmd.readBytes(nameSize); err != nil {
			return err
		}
		name := string(cmd.dataBuffer[:nameSize])

		particleBytesSize := opSize - (4 + nameSize)
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return err
		}
		value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
		if err != nil {
			return err
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

	rec.setRecord(newRecord(cmd.node, key, bins, generation, expiration))
	return nil
}

func (cmd *batchCommandUDF) isRead() bool {
	return !cmd.attr.hasWrite
}

func (cmd *batchCommandUDF) executeSingle(client clientIfc) Error {
	for i, key := range cmd.keys {
		policy := cmd.batchUDFPolicy.toWritePolicy(cmd.policy)
		policy.RespondPerEachOp = true
		res, err := client.execute(policy, key, cmd.packageName, cmd.functionName, cmd.args...)
		cmd.records[i].setRecord(res)
		if err != nil {
			cmd.records[i].setRawError(err)

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

func (cmd *batchCommandUDF) Execute() Error {
	if len(cmd.keys) == 1 {
		return cmd.executeSingle(cmd.client)
	}
	return cmd.execute(cmd)
}

func (cmd *batchCommandUDF) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	return newBatchNodeListKeys(cluster, cmd.policy, cmd.keys, nil, cmd.sequenceAP, cmd.sequenceSC, cmd.batch, false)
}

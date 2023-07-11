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
	"math/rand"
	"reflect"

	kvs "github.com/aerospike/aerospike-client-go/v6/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v6/types"
	Buffer "github.com/aerospike/aerospike-client-go/v6/utils/buffer"
)

type batchCommandOperate struct {
	batchCommand

	attr    *batchAttr
	records []BatchRecordIfc

	// pointer to the object that's going to be unmarshalled
	objects      []*reflect.Value
	objectsFound []bool
}

func newBatchCommandOperate(
	node *Node,
	batch *batchNode,
	policy *BatchPolicy,
	records []BatchRecordIfc,
) *batchCommandOperate {
	res := &batchCommandOperate{
		batchCommand: batchCommand{
			baseMultiCommand: *newMultiCommand(node, nil, true),
			policy:           policy,
			batch:            batch,
		},
		records: records,
	}
	return res
}

func (cmd *batchCommandOperate) buf() []byte {
	return cmd.dataBuffer
}

func (cmd *batchCommandOperate) object(index int) *reflect.Value {
	return cmd.objects[index]
}

func (cmd *batchCommandOperate) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch

	return &res
}

func (cmd *batchCommandOperate) writeBuffer(ifc command) Error {
	attr, err := cmd.setBatchOperateIfc(cmd.policy, cmd.records, cmd.batch)
	cmd.attr = attr
	return err
}

func (cmd *batchCommandOperate) isRead() bool {
	return !cmd.attr.hasWrite
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchCommandOperate) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0
	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if resultCode == 0 && (info3&_INFO3_LAST) == _INFO3_LAST {
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

		if resultCode != 0 {
			if resultCode == types.FILTERED_OUT {
				cmd.filteredOutCnt++
			}

			// If it looks like the error is on the first record and the message is marked as last part,
			// the error is for the whole command and not just for the first batchIndex
			lastMessage := (info3 & _INFO3_LAST) == _INFO3_LAST
			if resultCode != 0 && lastMessage && receiveSize == int(_MSG_REMAINING_HEADER_SIZE) {
				return false, newError(resultCode).setNode(cmd.node)
			}

			if resultCode == types.UDF_BAD_RESPONSE {
				rec, err := cmd.parseRecord(cmd.records[batchIndex].key(), opCount, generation, expiration)
				if err != nil {
					cmd.records[batchIndex].setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandWasSent))
					return false, err
				}

				// for UDF failures
				var msg interface{}
				if rec != nil {
					msg = rec.Bins["FAILURE"]
				}

				// Need to store record because failure bin contains an error message.
				cmd.records[batchIndex].setRecord(rec)
				if msg, ok := msg.(string); ok && len(msg) > 0 {
					cmd.records[batchIndex].setErrorWithMsg(cmd.node, resultCode, msg, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandWasSent))
				} else {
					cmd.records[batchIndex].setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandWasSent))
				}

				// If cmd is the end marker of the response, do not proceed further
				if (info3 & _INFO3_LAST) == _INFO3_LAST {
					return false, nil
				}
				continue
			}

			cmd.records[batchIndex].setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandWasSent))

			// If cmd is the end marker of the response, do not proceed further
			if (info3 & _INFO3_LAST) == _INFO3_LAST {
				return false, nil
			}
			continue
		}

		if resultCode == 0 {
			if cmd.objects == nil {
				rec, err := cmd.parseRecord(cmd.records[batchIndex].key(), opCount, generation, expiration)
				if err != nil {
					cmd.records[batchIndex].setError(cmd.node, resultCode, cmd.batchInDoubt(cmd.attr.hasWrite, cmd.commandWasSent))
					return false, err
				}
				cmd.records[batchIndex].setRecord(rec)
			} else if batchObjectParser != nil {
				// mark it as found
				cmd.objectsFound[batchIndex] = true
				if err := batchObjectParser(cmd, batchIndex, opCount, fieldCount, generation, expiration); err != nil {
					return false, err

				}
			}
		}
	}

	return true, nil
}

// Parses the given byte buffer and populate the result object.
// Returns the number of bytes that were parsed from the given buffer.
func (cmd *batchCommandOperate) parseRecord(key *Key, opCount int, generation, expiration uint32) (*Record, Error) {
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

func (cmd *batchCommandOperate) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchCommandOperate) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	return newBatchOperateNodeListIfcRetry(cluster, cmd.policy, cmd.records, cmd.sequenceAP, cmd.sequenceSC, cmd.batch)
}

func (cmd *batchCommandOperate) ExecuteGRPC(clnt *ProxyClient) Error {
	err := cmd.prepareBuffer(cmd, cmd.policy.deadline())
	if err != nil {
		return err
	}

	req := kvs.AerospikeRequestPayload{
		Id:          rand.Uint32(),
		Iteration:   1,
		Payload:     cmd.dataBuffer[:cmd.dataOffset],
		ReadPolicy:  cmd.policy.grpc(),
		WritePolicy: cmd.policy.grpc_write(),
	}

	conn, err := clnt.grpcConn()
	if err != nil {
		return err
	}

	client := kvs.NewKVSClient(conn)

	ctx := cmd.policy.grpcDeadlineContext()

	streamRes, gerr := client.BatchOperate(ctx, &req)
	if gerr != nil {
		return newGrpcError(gerr, gerr.Error())
	}

	readCallback := func() ([]byte, Error) {
		res, gerr := streamRes.Recv()
		if gerr != nil {
			e := newGrpcError(gerr)
			return nil, e
		}

		if res.Status != 0 {
			e := newGrpcStatusError(res)
			return res.Payload, e
		}

		if !res.HasNext {
			return nil, errGRPCStreamEnd
		}

		return res.Payload, nil
	}

	cmd.conn = newGrpcFakeConnection(nil, readCallback)
	err = cmd.parseResult(cmd, cmd.conn)
	if err != nil && err != errGRPCStreamEnd {
		return err
	}

	clnt.returnGrpcConnToPool(conn)

	return nil
}

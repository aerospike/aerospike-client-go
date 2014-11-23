// Copyright 2013-2014 Aerospike, Inc.
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
	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type batchCommandExists struct {
	*baseMultiCommand

	batchNamespace *batchNamespace
	policy         *BasePolicy
	keyMap         map[string]*batchItem
	existsArray    []bool
}

func newBatchCommandExists(
	node *Node,
	batchNamespace *batchNamespace,
	policy *BasePolicy,
	keyMap map[string]*batchItem,
	existsArray []bool,
) *batchCommandExists {
	return &batchCommandExists{
		baseMultiCommand: newMultiCommand(node, nil, nil),
		batchNamespace:   batchNamespace,
		policy:           policy,
		keyMap:           keyMap,
		existsArray:      existsArray,
	}
}

func (cmd *batchCommandExists) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchCommandExists) writeBuffer(ifc command) error {
	return cmd.setBatchExists(cmd.batchNamespace)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchCommandExists) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if !cmd.IsValid() {
			return false, NewAerospikeError(QUERY_TERMINATED)
		}

		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}

		resultCode := ResultCode(cmd.dataBuffer[5] & 0xFF)

		// The only valid server return codes are "ok" and "not found".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != KEY_NOT_FOUND_ERROR {
			return false, NewAerospikeError(resultCode)
		}

		info3 := cmd.dataBuffer[3]

		// If cmd is the end marker of the response, do not proceed further
		if (int(info3) & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		fieldCount := int(Buffer.BytesToInt16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToInt16(cmd.dataBuffer, 20))

		if opCount > 0 {
			return false, NewAerospikeError(PARSE_ERROR, "Received bins that were not requested!")
		}

		key, err := cmd.parseKey(fieldCount)
		if err != nil {
			return false, err
		}

		item := cmd.keyMap[string(key.digest)]

		if item != nil {
			index := item.GetIndex()

			// only set the results to true; as a result, no synchronization is needed
			if resultCode == 0 {
				cmd.existsArray[index] = true
			}
		} else {
			Logger.Debug("Unexpected batch key returned: " + key.namespace + "," + Buffer.BytesToHexString(key.digest))
		}
	}
	return true, nil
}

func (cmd *batchCommandExists) Execute() error {
	return cmd.execute(cmd)
}

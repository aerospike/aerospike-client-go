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

type BatchCommandExists struct {
	BaseMultiCommand

	batchNamespace *batchNamespace
	policy         *BasePolicy
	keyMap         map[string]*BatchItem
	existsArray    []bool
}

func NewBatchCommandExists(
	node *Node,
	batchNamespace *batchNamespace,
	policy *BasePolicy,
	keyMap map[string]*BatchItem,
	existsArray []bool,
) *BatchCommandExists {
	return &BatchCommandExists{
		BaseMultiCommand: *NewMultiCommand(node),
		batchNamespace:   batchNamespace,
		policy:           policy,
		keyMap:           keyMap,
		existsArray:      existsArray,
	}
}

func (this *BatchCommandExists) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *BatchCommandExists) writeBuffer(ifc Command) error {
	return this.SetBatchExists(this.batchNamespace)
}

/**
 * Parse all results in the batch.  Add records to shared list.
 * If the record was not found, the bins will be nil.
 */
func (this *BatchCommandExists) parseRecordResults(ifc Command, receiveSize int) (bool, error) {
	//Parse each message response and add it to the result array
	this.dataOffset = 0

	for this.dataOffset < receiveSize {
		if !this.IsValid() {
			return false, QueryTerminatedErr()
		}

		this.readBytes(int(MSG_REMAINING_HEADER_SIZE))
		resultCode := ResultCode(this.dataBuffer[5] & 0xFF)

		// The only valid server return codes are "ok" and "not found".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != KEY_NOT_FOUND_ERROR {
			return false, NewAerospikeError(resultCode)
		}

		info3 := this.dataBuffer[3]

		// If this is the end marker of the response, do not proceed further
		if (int(info3) & INFO3_LAST) == INFO3_LAST {
			return false, nil
		}

		fieldCount := int(Buffer.BytesToInt16(this.dataBuffer, 18))
		opCount := int(Buffer.BytesToInt16(this.dataBuffer, 20))

		if opCount > 0 {
			return false, ParseErr("Received bins that were not requested!")
		}

		key, err := this.parseKey(fieldCount)
		if err != nil {
			return false, err
		}

		item := this.keyMap[string(key.Digest())]

		if item != nil {
			index := item.GetIndex()

			// only set the results to true; as a result, no synchronization is needed
			if resultCode == 0 {
				this.existsArray[index] = true
			}
		} else {
			Logger.Debug("Unexpected batch key returned: " + *key.namespace + "," + Buffer.BytesToHexString(key.digest))
		}
	}
	return true, nil
}

func (this *BatchCommandExists) Execute() error {
	return this.execute(this)
}

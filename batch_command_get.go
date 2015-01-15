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

type batchCommandGet struct {
	*baseMultiCommand

	batchNamespace *batchNamespace
	policy         Policy
	keyMap         map[string]*batchItem
	binNames       map[string]struct{}
	records        []*Record
	readAttr       int
}

func newBatchCommandGet(
	node *Node,
	batchNamespace *batchNamespace,
	policy Policy,
	keyMap map[string]*batchItem,
	binNames map[string]struct{},
	records []*Record,
	readAttr int,
) *batchCommandGet {
	return &batchCommandGet{
		baseMultiCommand: newMultiCommand(node, nil),
		batchNamespace:   batchNamespace,
		policy:           policy,
		keyMap:           keyMap,
		records:          records,
		readAttr:         readAttr,
	}
}

func (cmd *batchCommandGet) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchCommandGet) writeBuffer(ifc command) error {
	return cmd.setBatchGet(cmd.policy, cmd.batchNamespace, cmd.binNames, cmd.readAttr)
}

// Parse all results in the batch.  Add records to shared list.
// If the record was not found, the bins will be nil.
func (cmd *batchCommandGet) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	//Parse each message response and add it to the result array
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := ResultCode(cmd.dataBuffer[5] & 0xFF)

		// The only valid server return codes are "ok" and "not found".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != KEY_NOT_FOUND_ERROR {
			return false, NewAerospikeError(resultCode)
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		generation := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 6)))
		expiration := TTL(int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 10))))
		fieldCount := int(uint16(Buffer.BytesToInt16(cmd.dataBuffer, 18)))
		opCount := int(uint16(Buffer.BytesToInt16(cmd.dataBuffer, 20)))
		key, err := cmd.parseKey(fieldCount)
		if err != nil {
			return false, err
		}
		item := cmd.keyMap[string(key.digest)]

		if item != nil {
			if resultCode == 0 {
				index := item.GetIndex()
				if cmd.records[index], err = cmd.parseRecord(key, opCount, generation, expiration); err != nil {
					return false, err
				}
			}
		} else {
			Logger.Debug("Unexpected batch key returned: " + string(key.namespace) + "," + Buffer.BytesToHexString(key.digest))
		}
	}
	return true, nil
}

func contains(a map[string]struct{}, elem string) bool {
	_, exists := a[elem]
	return exists
}

// Parses the given byte buffer and populate the result object.
// Returns the number of bytes that were parsed from the given buffer.
func (cmd *batchCommandGet) parseRecord(key *Key, opCount int, generation int, expiration int) (*Record, error) {
	var bins map[string]interface{}

	for i := 0; i < opCount; i++ {
		if !cmd.IsValid() {
			return nil, NewAerospikeError(QUERY_TERMINATED)
		}

		if err := cmd.readBytes(8); err != nil {
			return nil, err
		}
		opSize := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 0)))
		particleType := int(cmd.dataBuffer[5])
		nameSize := int(cmd.dataBuffer[7])

		if err := cmd.readBytes(nameSize); err != nil {
			return nil, err
		}
		name := string(cmd.dataBuffer[:nameSize])

		particleBytesSize := int(opSize - (4 + nameSize))
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return nil, err
		}
		value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
		if err != nil {
			return nil, err
		}

		// Currently, the batch command returns all the bins even if a subset of
		// the bins are requested. We have to filter it on the client side.
		// TODO: Filter batch bins on server!
		if cmd.binNames == nil || contains(cmd.binNames, name) {
			if bins == nil {
				bins = map[string]interface{}{}
			}
			bins[name] = value
		}
	}

	return newRecord(cmd.node, key, bins, generation, expiration), nil
}

func (cmd *batchCommandGet) Execute() error {
	return cmd.execute(cmd)
}

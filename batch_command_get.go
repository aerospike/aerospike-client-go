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

type BatchCommandGet struct {
	BaseMultiCommand

	batchNamespace *batchNamespace
	policy         Policy
	keyMap         map[string]*BatchItem
	binNames       map[string]struct{}
	records        []*Record
	readAttr       int
}

func NewBatchCommandGet(
	node *Node,
	batchNamespace *batchNamespace,
	policy Policy,
	keyMap map[string]*BatchItem,
	binNames map[string]struct{},
	records []*Record,
	readAttr int,
) *BatchCommandGet {
	return &BatchCommandGet{
		BaseMultiCommand: *NewMultiCommand(node),
		batchNamespace:   batchNamespace,
		policy:           policy,
		keyMap:           keyMap,
		records:          records,
		readAttr:         readAttr,
	}
}

func (this *BatchCommandGet) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *BatchCommandGet) writeBuffer(ifc Command) error {
	return this.SetBatchGet(this.batchNamespace, this.binNames, this.readAttr)
}

/**
 * Parse all results in the batch.  Add records to shared list.
 * If the record was not found, the bins will be nil.
 */
func (this *BatchCommandGet) parseRecordResults(ifc Command, receiveSize int) (bool, error) {
	//Parse each message response and add it to the result array
	this.dataOffset = 0

	for this.dataOffset < receiveSize {
		this.readBytes(int(MSG_REMAINING_HEADER_SIZE))
		resultCode := ResultCode(this.dataBuffer[5] & 0xFF)

		// The only valid server return codes are "ok" and "not found".
		// If other return codes are received, then abort the batch.
		if resultCode != 0 && resultCode != KEY_NOT_FOUND_ERROR {
			return false, NewAerospikeError(resultCode)
		}

		info3 := int(this.dataBuffer[3])

		// If this is the end marker of the response, do not proceed further
		if (info3 & INFO3_LAST) == INFO3_LAST {
			return false, nil
		}

		generation := int(Buffer.BytesToInt32(this.dataBuffer, 6))
		expiration := int(Buffer.BytesToInt32(this.dataBuffer, 10))
		fieldCount := int(Buffer.BytesToInt16(this.dataBuffer, 18))
		opCount := int(Buffer.BytesToInt16(this.dataBuffer, 20))
		key, err := this.parseKey(fieldCount)
		if err != nil {
			return false, err
		}
		item := this.keyMap[string(key.Digest())]

		if item != nil {
			if resultCode == 0 {
				index := item.GetIndex()
				if this.records[index], err = this.parseRecord(key, opCount, generation, expiration); err != nil {
					return false, err
				}
			}
		} else {
			Logger.Debug("Unexpected batch key returned: " + string(*key.namespace) + "," + Buffer.BytesToHexString(key.digest))
		}
	}
	return true, nil
}

func contains(a map[string]struct{}, elem string) bool {
	_, exists := a[elem]
	return exists
}

/**
 * Parses the given byte buffer and populate the result object.
 * Returns the number of bytes that were parsed from the given buffer.
 */
func (this *BatchCommandGet) parseRecord(key *Key, opCount int, generation int, expiration int) (*Record, error) {
	var bins map[string]interface{}
	var duplicates []BinMap

	for i := 0; i < opCount; i++ {
		if !this.IsValid() {
			return nil, QueryTerminatedErr()
		}

		this.readBytes(8)
		opSize := int(Buffer.BytesToInt32(this.dataBuffer, 0))
		particleType := int(this.dataBuffer[5])
		version := int(this.dataBuffer[6])
		nameSize := int(this.dataBuffer[7])

		this.readBytes(nameSize)
		name := string(this.dataBuffer[:nameSize])

		particleBytesSize := int(opSize - (4 + nameSize))
		this.readBytes(particleBytesSize)
		value, err := BytesToParticle(particleType, this.dataBuffer, 0, particleBytesSize)
		if err != nil {
			return nil, err
		}

		// Currently, the batch command returns all the bins even if a subset of
		// the bins are requested. We have to filter it on the client side.
		// TODO: Filter batch bins on server!
		if this.binNames == nil || contains(this.binNames, name) {
			var vmap map[string]interface{}

			if version > 0 || duplicates != nil {
				if duplicates == nil {
					duplicates = []BinMap{}
					duplicates = append(duplicates, bins)
					bins = nil

					for j := 0; j < version; j++ {
						duplicates = append(duplicates, nil)
					}
				} else {
					for j := len(duplicates); j < version+1; j++ {
						duplicates = append(duplicates, nil)
					}
				}

				vmap = duplicates[version]
				if vmap == nil {
					vmap = map[string]interface{}{}
					duplicates[version] = vmap
				}
			} else {
				if bins == nil {
					bins = map[string]interface{}{}
				}
				vmap = bins
			}
			vmap[name] = value
		}
	}

	// Remove nil duplicates just in case there were holes in the version number space.
	// TODO: this seems to be a bad idea; O(n) algorithm after another O(n) algorithm
	idx := 0
	for i := range duplicates {
		if duplicates[i] != nil {
			duplicates[idx] = duplicates[i]
			idx++
		}
	}

	return NewRecord(key, bins, duplicates[:idx], generation, expiration), nil
}

func (this *BatchCommandGet) Execute() error {
	return this.execute(this)
}

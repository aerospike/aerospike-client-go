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
	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type QueryRecordCommand struct {
	QueryCommand
}

func NewQueryRecordCommand(node *Node, policy *QueryPolicy, statement *Statement, recChan chan *Record) *QueryRecordCommand {
	return &QueryRecordCommand{
		QueryCommand: *NewQueryCommand(node, policy, statement, recChan),
	}
}

func (this *QueryRecordCommand) parseRecordResults(ifc Command, receiveSize int) (bool, error) {
	// Read/parse remaining message bytes one record at a time.
	this.dataOffset = 0

	for this.dataOffset < receiveSize {
		this.readBytes(int(MSG_REMAINING_HEADER_SIZE))
		resultCode := ResultCode(this.dataBuffer[5] & 0xFF)

		if resultCode != 0 {
			if resultCode == KEY_NOT_FOUND_ERROR {
				return false, nil
			}

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

		// Parse bins.
		var bins BinMap

		for i := 0; i < opCount; i++ {
			if err := this.readBytes(8); err != nil {
				return false, err
			}

			opSize := int(Buffer.BytesToInt32(this.dataBuffer, 0))
			particleType := int(this.dataBuffer[5])
			nameSize := int(this.dataBuffer[7])

			if err := this.readBytes(nameSize); err != nil {
				return false, err
			}
			name := string(this.dataBuffer[:nameSize])

			particleBytesSize := int((opSize - (4 + nameSize)))
			if this.readBytes(particleBytesSize); err != nil {
				return false, err
			}
			value, err := BytesToParticle(particleType, this.dataBuffer, 0, particleBytesSize)
			if err != nil {
				return false, err
			}

			if bins == nil {
				bins = BinMap{}
			}
			bins[name] = value
		}

		record := NewRecord(key, bins, nil, generation, expiration)

		if !this.valid {
			return false, QueryTerminatedErr()
		}

		this.Records <- record
	}

	return true, nil
}

func (this *QueryRecordCommand) Execute() error {
	return this.execute(this)
}

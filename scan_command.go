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

type ScanCommand struct {
	BaseMultiCommand

	policy    *ScanPolicy
	namespace string
	setName   string
	Records   chan *Record
	binNames  []string
}

func NewScanCommand(
	node *Node,
	policy *ScanPolicy,
	namespace string,
	setName string,
	binNames []string,
	recChan chan *Record,
) *ScanCommand {
	// make recChan in case it is nil
	if recChan == nil {
		recChan = make(chan *Record, 1024)
	}

	return &ScanCommand{
		BaseMultiCommand: *NewMultiCommand(node),
		policy:           policy,
		namespace:        namespace,
		setName:          setName,
		Records:          recChan,
		binNames:         binNames,
	}
}

func (this *ScanCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *ScanCommand) writeBuffer(ifc Command) error {
	return this.SetScan(this.policy, &this.namespace, &this.setName, this.binNames)
}

func (this *ScanCommand) parseRecordResults(ifc Command, receiveSize int) (bool, error) {
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
			this.readBytes(8)
			opSize := int(Buffer.BytesToInt32(this.dataBuffer, 0))
			particleType := int(this.dataBuffer[5])
			nameSize := int(this.dataBuffer[7])

			this.readBytes(nameSize)
			name := string(this.dataBuffer[:nameSize])

			particleBytesSize := int(opSize - (4 + nameSize))
			this.readBytes(particleBytesSize)
			value, err := BytesToParticle(particleType, this.dataBuffer, 0, particleBytesSize)
			if err != nil {
				return false, err
			}

			if bins == nil {
				bins = BinMap{}
			}
			bins[name] = value
		}

		if !this.IsValid() {
			return false, ScanTerminatedErr()
		}

		// send back the result on the async channel
		this.Records <- NewRecord(key, bins, nil, generation, expiration)
	}

	return true, nil
}

func (this *ScanCommand) parseResult(ifc Command, conn *Connection) error {
	// close the channel
	defer close(this.Records)

	return this.BaseMultiCommand.parseResult(ifc, conn)
}

func (this *ScanCommand) Execute() error {
	return this.execute(this)
}

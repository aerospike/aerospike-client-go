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
	"time"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type scanCommand struct {
	*baseMultiCommand

	policy    *ScanPolicy
	namespace string
	setName   string
	// Records   chan *Record
	// Errors    chan error
	binNames []string
}

func newScanCommand(
	node *Node,
	policy *ScanPolicy,
	namespace string,
	setName string,
	binNames []string,
	recChan chan *Record,
	errChan chan error,
) *scanCommand {

	// make recChan in case it is nil
	if recChan == nil {
		recChan = make(chan *Record, 1024)
	}

	// make errChan in case it is nil
	if errChan == nil {
		errChan = make(chan error, 1024)
	}

	return &scanCommand{
		baseMultiCommand: newMultiCommand(node, recChan, errChan),
		policy:           policy,
		namespace:        namespace,
		setName:          setName,
		binNames:         binNames,
	}
}

func (cmd *scanCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *scanCommand) writeBuffer(ifc command) error {
	return cmd.setScan(cmd.policy, &cmd.namespace, &cmd.setName, cmd.binNames)
}

func (cmd *scanCommand) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	// Read/parse remaining message bytes one record at a time.
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			cmd.Errors <- newNodeError(cmd.node, err)
			return false, err
		}
		resultCode := ResultCode(cmd.dataBuffer[5] & 0xFF)

		if resultCode != 0 {
			if resultCode == KEY_NOT_FOUND_ERROR {
				return false, nil
			}
			err := NewAerospikeError(resultCode)
			cmd.Errors <- newNodeError(cmd.node, err)
			return false, err
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		generation := int(Buffer.BytesToInt32(cmd.dataBuffer, 6))
		expiration := int(Buffer.BytesToInt32(cmd.dataBuffer, 10))
		fieldCount := int(Buffer.BytesToInt16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToInt16(cmd.dataBuffer, 20))

		key, err := cmd.parseKey(fieldCount)
		if err != nil {
			cmd.Errors <- newNodeError(cmd.node, err)
			return false, err
		}

		// Parse bins.
		var bins BinMap

		for i := 0; i < opCount; i++ {
			if err := cmd.readBytes(8); err != nil {
				cmd.Errors <- newNodeError(cmd.node, err)
				return false, err
			}

			opSize := int(Buffer.BytesToInt32(cmd.dataBuffer, 0))
			particleType := int(cmd.dataBuffer[5])
			nameSize := int(cmd.dataBuffer[7])

			if err := cmd.readBytes(nameSize); err != nil {
				cmd.Errors <- newNodeError(cmd.node, err)
				return false, err
			}
			name := string(cmd.dataBuffer[:nameSize])

			particleBytesSize := int(opSize - (4 + nameSize))
			if err := cmd.readBytes(particleBytesSize); err != nil {
				cmd.Errors <- newNodeError(cmd.node, err)
				return false, err
			}

			value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
			if err != nil {
				cmd.Errors <- newNodeError(cmd.node, err)
				return false, err
			}

			if bins == nil {
				bins = BinMap{}
			}
			bins[name] = value
		}

		if !cmd.IsValid() {
			return false, NewAerospikeError(SCAN_TERMINATED)
		}

	L:
		for {
			select {
			// send back the result on the async channel
			case cmd.Records <- newRecord(cmd.node, key, bins, nil, generation, expiration):
				break L
			case <-time.After(time.Millisecond):
				if !cmd.IsValid() {
					return false, NewAerospikeError(SCAN_TERMINATED)
				}
			}
		}
	}

	return true, nil
}

func (cmd *scanCommand) parseResult(ifc command, conn *Connection) error {
	// close the channel
	defer close(cmd.Records)
	defer close(cmd.Errors)

	return cmd.baseMultiCommand.parseResult(ifc, conn)
}

func (cmd *scanCommand) Execute() error {
	return cmd.execute(cmd)
}

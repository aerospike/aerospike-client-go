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
	// "fmt"

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type ServerCommand struct {
	QueryCommand
}

func NewServerCommand(node *Node, policy *QueryPolicy, statement *Statement) *ServerCommand {
	return &ServerCommand{
		QueryCommand: *NewQueryCommand(node, policy, statement, nil),
	}
}

func (this *ServerCommand) parseRecordResults(ifc Command, receiveSize int) (bool, error) {
	// Server commands (Query/Execute UDF) should only send back a return code.
	// Keep parsing logic to empty socket buffer just in case server does
	// send records back.
	this.dataOffset = 0

	for this.dataOffset < receiveSize {
		if err := this.readBytes(int(MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := ResultCode(this.dataBuffer[5] & 0xFF)

		if resultCode != 0 {
			if resultCode == KEY_NOT_FOUND_ERROR {
				return false, nil
			}
			NewAerospikeError(resultCode)
		}

		info3 := int(this.dataBuffer[3])

		// If this is the end marker of the response, do not proceed further
		if (info3 & INFO3_LAST) == INFO3_LAST {
			return false, nil
		}

		fieldCount := int(Buffer.BytesToInt16(this.dataBuffer, 18))
		opCount := int(Buffer.BytesToInt16(this.dataBuffer, 20))

		if _, err := this.parseKey(fieldCount); err != nil {
			return false, err
		}

		for i := 0; i < opCount; i++ {
			if err := this.readBytes(8); err != nil {
				return false, err
			}
			opSize := int(Buffer.BytesToInt32(this.dataBuffer, 0))
			nameSize := int(this.dataBuffer[7])

			if err := this.readBytes(nameSize); err != nil {
				return false, err
			}

			particleBytesSize := int((opSize - (4 + nameSize)))
			if err := this.readBytes(particleBytesSize); err != nil {
				return false, err
			}
		}

		if !this.valid {
			return false, QueryTerminatedErr()
		}
	}
	return true, nil
}

func (this *ServerCommand) Execute() error {
	return this.execute(this)
}

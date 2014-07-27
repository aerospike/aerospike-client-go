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
	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type ReadHeaderCommand struct {
	SingleCommand

	policy Policy
	record *Record
}

func NewReadHeaderCommand(cluster *Cluster, policy Policy, key *Key) *ReadHeaderCommand {
	newReadHeaderCmd := &ReadHeaderCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
	}

	if policy != nil {
		newReadHeaderCmd.policy = policy
	} else {
		newReadHeaderCmd.policy = NewPolicy()
	}

	return newReadHeaderCmd
}

func (this *ReadHeaderCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *ReadHeaderCommand) writeBuffer(ifc Command) error {
	this.SetReadHeader(this.key)
	return nil
}

func (this *ReadHeaderCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	resultCode := this.dataBuffer[13] & 0xFF

	if resultCode == 0 {
		generation := int(Buffer.BytesToInt32(this.dataBuffer, 14))
		expiration := TTL(int(Buffer.BytesToInt32(this.dataBuffer, 18)))
		this.record = NewRecord(nil, nil, generation, expiration)
	} else {
		if ResultCode(resultCode) == KEY_NOT_FOUND_ERROR {
			this.record = nil
		} else {
			return NewAerospikeError(ResultCode(resultCode))
		}
	}
	this.emptySocket(conn)
	return nil
}

func (this *ReadHeaderCommand) GetRecord() *Record {
	return this.record
}

func (this *ReadHeaderCommand) Execute() error {
	return this.execute(this)
}

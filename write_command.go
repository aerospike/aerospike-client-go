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
	. "github.com/citrusleaf/aerospike-client-go/types"
)

// guarantee WriteCommand implements Command interface
var _ Command = &WriteCommand{}

type WriteCommand struct {
	SingleCommand

	policy    *WritePolicy
	bins      []*Bin
	operation OperationType
}

func NewWriteCommand(cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	bins []*Bin,
	operation OperationType) *WriteCommand {

	newWriteCmd := &WriteCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
		bins:          bins,
		operation:     operation,
	}

	if policy == nil {
		newWriteCmd.policy = NewWritePolicy(0, 0)
	} else {
		newWriteCmd.policy = policy
	}

	return newWriteCmd
}

func (this *WriteCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *WriteCommand) writeBuffer(ifc Command) error {
	return this.SetWrite(this.policy, this.operation, this.key, this.bins)
}

func (this *WriteCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	resultCode := this.dataBuffer[13] & 0xFF

	if resultCode != 0 {
		return NewAerospikeError(ResultCode(resultCode))
	}
	this.emptySocket(conn)
	return nil
}

func (this *WriteCommand) Execute() error {
	return this.execute(this)
}

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
	. "github.com/citrusleaf/go-client/types"
)

// guarantee DeleteCommand implements Command interface
var _ Command = &DeleteCommand{}

type DeleteCommand struct {
	SingleCommand

	policy  *WritePolicy
	existed bool
}

func NewDeleteCommand(cluster *Cluster, policy *WritePolicy, key *Key) *DeleteCommand {
	newDeleteCmd := &DeleteCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
	}

	if policy == nil {
		newDeleteCmd.policy = NewWritePolicy(0, 0)
	} else {
		newDeleteCmd.policy = policy
	}

	return newDeleteCmd
}

func (this *DeleteCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *DeleteCommand) writeBuffer(ifc Command) error {
	this.SetDelete(this.policy, this.key)
	return nil
}

func (this *DeleteCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	resultCode := this.dataBuffer[13] & 0xFF

	if resultCode != 0 && ResultCode(resultCode) != KEY_NOT_FOUND_ERROR {
		NewAerospikeError(ResultCode(resultCode))
	}
	this.existed = resultCode == 0
	this.emptySocket(conn)
	return nil
}

func (this *DeleteCommand) Existed() bool {
	return this.existed
}

func (this *DeleteCommand) Execute() error {
	return this.execute(this)
}

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

// guarantee TouchCommand implements Command interface
var _ Command = &TouchCommand{}

type TouchCommand struct {
	SingleCommand

	policy *WritePolicy
}

func NewTouchCommand(cluster *Cluster, policy *WritePolicy, key *Key) *TouchCommand {
	newTouchCmd := &TouchCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
	}

	if policy == nil {
		newTouchCmd.policy = NewWritePolicy(0, 0)
	} else {
		newTouchCmd.policy = policy
	}

	return newTouchCmd
}

func (this *TouchCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *TouchCommand) writeBuffer(ifc Command) error {
	this.SetTouch(this.policy, this.key)
	return nil
}

func (this *TouchCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	resultCode := this.dataBuffer[13] & 0xFF

	if resultCode != 0 {
		return NewAerospikeError(ResultCode(resultCode))
	}
	this.emptySocket(conn)
	return nil
}

func (this *TouchCommand) Execute() error {
	return this.execute(this)
}

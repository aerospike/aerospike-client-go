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

// guarantee ExistsCommand implements Command interface
var _ Command = &ExistsCommand{}

type ExistsCommand struct {
	SingleCommand

	policy Policy
	exists bool
}

func NewExistsCommand(cluster *Cluster, policy Policy, key *Key) *ExistsCommand {
	newExistsCmd := &ExistsCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
	}

	if policy == nil {
		newExistsCmd.policy = NewWritePolicy(0, 0)
	} else {
		newExistsCmd.policy = policy
	}

	return newExistsCmd
}

func (this *ExistsCommand) getPolicy(ifc Command) Policy {
	return this.policy.GetBasePolicy()
}

func (this *ExistsCommand) writeBuffer(ifc Command) error {
	this.SetExists(this.key)
	return nil
}

func (this *ExistsCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	resultCode := this.dataBuffer[13] & 0xFF

	if resultCode != 0 && ResultCode(resultCode) != KEY_NOT_FOUND_ERROR {
		NewAerospikeError(ResultCode(resultCode))
	}
	this.exists = resultCode == 0
	this.emptySocket(conn)
	return nil
}

func (this *ExistsCommand) Exists() bool {
	return this.exists
}

func (this *ExistsCommand) Execute() error {
	return this.execute(this)
}

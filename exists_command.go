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
)

// guarantee existsCommand implements command interface
var _ command = &existsCommand{}

type existsCommand struct {
	*singleCommand

	policy Policy
	exists bool
}

func newExistsCommand(cluster *Cluster, policy Policy, key *Key) *existsCommand {
	newExistsCmd := &existsCommand{
		singleCommand: newSingleCommand(cluster, key),
	}

	if policy == nil {
		newExistsCmd.policy = NewWritePolicy(0, 0)
	} else {
		newExistsCmd.policy = policy
	}

	return newExistsCmd
}

func (cmd *existsCommand) getPolicy(ifc command) Policy {
	return cmd.policy.GetBasePolicy()
}

func (cmd *existsCommand) writeBuffer(ifc command) error {
	return cmd.setExists(cmd.key)
}

func (cmd *existsCommand) parseResult(ifc command, conn *Connection) error {
	// Read header.
	if _, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE)); err != nil {
		return err
	}

	resultCode := cmd.dataBuffer[13] & 0xFF

	if resultCode != 0 && ResultCode(resultCode) != KEY_NOT_FOUND_ERROR {
		return NewAerospikeError(ResultCode(resultCode))
	}
	cmd.exists = resultCode == 0
	if err := cmd.emptySocket(conn); err != nil {
		return err
	}
	return nil
}

func (cmd *existsCommand) Exists() bool {
	return cmd.exists
}

func (cmd *existsCommand) Execute() error {
	return cmd.execute(cmd)
}

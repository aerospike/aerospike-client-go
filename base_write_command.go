// Copyright 2014-2022 Aerospike, Inc.
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
	"iter"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// guarantee baseWriteCommand implements command interface
var _ command = &baseWriteCommand{}

type baseWriteCommand struct {
	singleCommand

	policy *WritePolicy
}

func newBaseWriteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
) (baseWriteCommand, Error) {

	var partition *Partition
	var err Error
	if cluster != nil {
		partition, err = PartitionForWrite(cluster, &policy.BasePolicy, key)
		if err != nil {
			return baseWriteCommand{}, err
		}
	}

	newBaseWriteCmd := baseWriteCommand{
		singleCommand: newSingleCommand(cluster, key, partition),
		policy:        policy,
	}

	return newBaseWriteCmd, nil
}

func (cmd *baseWriteCommand) writeBuffer(ifc command) Error {
	panic(unreachable)
}

func (cmd *baseWriteCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *baseWriteCommand) getNode(ifc command) (*Node, Error) {
	return cmd.partition.GetNodeWrite(cmd.cluster)
}

func (cmd *baseWriteCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryWrite(isTimeout)
	return true
}

func (cmd *baseWriteCommand) isRead() bool {
	return false
}

func (cmd *baseWriteCommand) parseResult(ifc command, conn *Connection) Error {
	panic(unreachable)
}

func (cmd *baseWriteCommand) Execute() Error {
	panic(unreachable)
}

func (cmd *baseWriteCommand) onInDoubt() {
	if cmd.policy.Txn != nil {
		cmd.policy.Txn.OnWriteInDoubt(cmd.key)
	}

}

func (cmd *baseWriteCommand) commandType() commandType {
	return ttPut
}

func (cmd *baseWriteCommand) parseHeader() (types.ResultCode, Error) {
	rp, err := newRecordParser(&cmd.baseCommand)
	if err != nil {
		return err.resultCode(), err
	}

	if err := rp.parseFields(cmd.policy.Txn, cmd.key, true); err != nil {
		return err.resultCode(), err
	}

	return rp.resultCode, nil
}

func (cmd *baseWriteCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *baseWriteCommand) getNamespace() *string {
	return &cmd.key.namespace
}

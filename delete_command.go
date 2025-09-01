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

// guarantee deleteCommand implements command interface
var _ command = &deleteCommand{}

type deleteCommand struct {
	baseWriteCommand

	existed bool
}

func newDeleteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
) (*deleteCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, policy, key)
	if err != nil {
		return nil, err
	}

	newDeleteCmd := &deleteCommand{
		baseWriteCommand: bwc,
	}

	return newDeleteCmd, nil
}

func (cmd *deleteCommand) writeBuffer(ifc command) Error {
	return cmd.setDelete(cmd.policy, cmd.key)
}

func (cmd *deleteCommand) parseResult(ifc command, conn *Connection) Error {
	resultCode, err := cmd.parseHeader()
	if err != nil {
		return newCustomNodeError(cmd.node, err.resultCode())
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, resultCode)
	}

	switch types.ResultCode(resultCode) {
	case 0:
		cmd.existed = true
	case types.KEY_NOT_FOUND_ERROR:
		cmd.existed = false
	case types.FILTERED_OUT:
		cmd.existed = true
		return ErrFilteredOut.err()
	default:
		return newError(types.ResultCode(resultCode))
	}

	return nil
}

func (cmd *deleteCommand) Existed() bool {
	return cmd.existed
}

func (cmd *deleteCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *deleteCommand) commandType() commandType {
	return ttDelete
}

func (cmd *deleteCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *deleteCommand) getNamespace() *string {
	return &cmd.key.namespace
}

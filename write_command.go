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

// guarantee writeCommand implements command interface
var _ command = &writeCommand{}

type writeCommand struct {
	baseWriteCommand

	bins      []*Bin
	binMap    BinMap
	operation OperationType
}

func newWriteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	bins []*Bin,
	binMap BinMap,
	operation OperationType,
) (writeCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, policy, key)
	if err != nil {
		return writeCommand{}, err
	}

	newWriteCmd := writeCommand{
		baseWriteCommand: bwc,
		bins:             bins,
		binMap:           binMap,
		operation:        operation,
	}

	return newWriteCmd, nil
}

func (cmd *writeCommand) writeBuffer(ifc command) Error {
	return cmd.setWrite(cmd.policy, cmd.operation, cmd.key, cmd.bins, cmd.binMap)
}

func (cmd *writeCommand) parseResult(ifc command, conn *Connection) Error {
	resultCode, err := cmd.parseHeader()
	if err != nil {
		return newCustomNodeError(cmd.node, err.resultCode())
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, resultCode)
	}

	if resultCode != types.OK {
		if resultCode == types.KEY_NOT_FOUND_ERROR {
			return ErrKeyNotFound.err()
		} else if resultCode == types.FILTERED_OUT {
			return ErrFilteredOut.err()
		}

		return newCustomNodeError(cmd.node, types.ResultCode(resultCode))
	}

	return nil
}

func (cmd *writeCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *writeCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *writeCommand) getNamespace() *string {
	return &cmd.key.namespace
}

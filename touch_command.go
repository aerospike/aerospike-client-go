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

// guarantee touchCommand implements command interface
var _ command = &touchCommand{}

type touchCommand struct {
	baseWriteCommand
}

func newTouchCommand(cluster *Cluster, policy *WritePolicy, key *Key) (touchCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, policy, key)
	if err != nil {
		return touchCommand{}, err
	}

	newTouchCmd := touchCommand{
		baseWriteCommand: bwc,
	}

	return newTouchCmd, nil
}

func (cmd *touchCommand) writeBuffer(ifc command) Error {
	return cmd.setTouch(cmd.policy, cmd.key)
}

func (cmd *touchCommand) parseResult(ifc command, conn *Connection) Error {
	resultCode, err := cmd.parseHeader()
	if err != nil {
		return newCustomNodeError(cmd.node, err.resultCode())
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, resultCode)
	}

	switch resultCode {
	case types.OK:
		return nil
	case types.KEY_NOT_FOUND_ERROR:
		return ErrKeyNotFound.err()
	case types.FILTERED_OUT:
		return ErrFilteredOut.err()
	default:
		return newError(types.ResultCode(resultCode))
	}
}

func (cmd *touchCommand) isRead() bool {
	return false
}

func (cmd *touchCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *touchCommand) commandType() commandType {
	return ttPut
}

func (cmd *touchCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *touchCommand) getNamespace() *string {
	return &cmd.key.namespace
}

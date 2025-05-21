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

// guarantee existsCommand implements command interface
var _ command = &existsCommand{}

type existsCommand struct {
	baseReadCommand

	exists bool
}

func newExistsCommand(cluster *Cluster, policy *BasePolicy, key *Key) (existsCommand, Error) {
	brc, err := newBaseReadCommand(cluster, policy, key)
	if err != nil {
		return existsCommand{}, err
	}

	return existsCommand{
		baseReadCommand: brc,
	}, nil
}

func (cmd *existsCommand) writeBuffer(ifc command) Error {
	return cmd.setExists(cmd.policy, cmd.key)
}

func (cmd *existsCommand) parseResult(ifc command, conn *Connection) Error {
	rp, err := newRecordParser(&cmd.baseCommand)
	if err != nil {
		return err
	}

	if err := rp.parseFields(cmd.policy.Txn, cmd.key, false); err != nil {
		return err
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, rp.resultCode)
	}

	switch rp.resultCode {
	case types.OK:
		cmd.exists = true
	case types.KEY_NOT_FOUND_ERROR:
		cmd.exists = false
	case types.FILTERED_OUT:
		cmd.exists = true
		return ErrFilteredOut.err()
	default:
		return newError(rp.resultCode)
	}

	return nil
}

func (cmd *existsCommand) Exists() bool {
	return cmd.exists
}

func (cmd *existsCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *existsCommand) commandType() commandType {
	return ttExists
}

func (cmd *existsCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *existsCommand) getNamespace() *string {
	return &cmd.key.namespace
}

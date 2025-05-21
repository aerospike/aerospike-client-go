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

type readHeaderCommand struct {
	baseReadCommand
}

func newReadHeaderCommand(cluster *Cluster, policy *BasePolicy, key *Key) (readHeaderCommand, Error) {
	brc, err := newBaseReadCommand(cluster, policy, key)
	if err != nil {
		return readHeaderCommand{}, err
	}

	newReadHeaderCmd := readHeaderCommand{
		baseReadCommand: brc,
	}

	return newReadHeaderCmd, nil
}

func (cmd *readHeaderCommand) writeBuffer(ifc command) Error {
	return cmd.setReadHeader(cmd.policy, cmd.key)
}

func (cmd *readHeaderCommand) parseResult(ifc command, conn *Connection) Error {
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

	if rp.resultCode == 0 {
		cmd.record = newRecord(cmd.node, cmd.key, nil, rp.generation, rp.expiration)
	} else {
		switch rp.resultCode {
		case types.KEY_NOT_FOUND_ERROR:
			cmd.record = nil
		case types.FILTERED_OUT:
			return ErrFilteredOut.err()
		default:
			return newError(rp.resultCode)
		}
	}
	return nil
}

func (cmd *readHeaderCommand) GetRecord() *Record {
	return cmd.record
}

func (cmd *readHeaderCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *readHeaderCommand) commandType() commandType {
	return ttGetHeader
}

func (cmd *readHeaderCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *readHeaderCommand) getNamespace() *string {
	return &cmd.key.namespace
}

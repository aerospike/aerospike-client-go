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

type readCommand struct {
	baseReadCommand

	binNames    []string
	isOperation bool
}

func newReadCommand(
	cluster *Cluster,
	policy *BasePolicy,
	key *Key,
	binNames []string,
) (readCommand, Error) {
	brc, err := newBaseReadCommand(cluster, policy, key)
	if err != nil {
		return readCommand{}, err
	}

	return readCommand{
		baseReadCommand: brc,
		binNames:        binNames,
	}, nil
}

func (cmd *readCommand) writeBuffer(ifc command) Error {
	return cmd.setRead(cmd.policy, cmd.key, cmd.binNames)
}

func (cmd *readCommand) parseResult(ifc command, conn *Connection) Error {
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

	if rp.resultCode != 0 {
		if rp.resultCode == types.KEY_NOT_FOUND_ERROR {
			return ErrKeyNotFound.err()
		} else if rp.resultCode == types.FILTERED_OUT {
			return ErrFilteredOut.err()
		}

		return newError(rp.resultCode)
	}

	if cmd.object == nil {
		if rp.opCount == 0 {
			// data Bin was not returned
			cmd.record = newRecord(cmd.node, cmd.key, nil, rp.generation, rp.expiration)
			return nil
		}

		var err Error
		cmd.record, err = rp.parseRecord(cmd.key, cmd.isOperation)
		if err != nil {
			return err
		}
	} else if objectParser != nil {
		if err := objectParser(&cmd.baseReadCommand, rp.opCount, rp.fieldCount, rp.generation, rp.expiration); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *readCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *readCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *readCommand) getNamespace() *string {
	return &cmd.key.namespace
}

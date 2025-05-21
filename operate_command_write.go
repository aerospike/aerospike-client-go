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

type operateCommandWrite struct {
	baseWriteCommand

	record *Record
	args   operateArgs
}

func newOperateCommandWrite(cluster *Cluster, key *Key, args operateArgs) (operateCommandWrite, Error) {
	bwc, err := newBaseWriteCommand(cluster, args.writePolicy, key)
	if err != nil {
		return operateCommandWrite{}, err
	}

	return operateCommandWrite{
		baseWriteCommand: bwc,
		args:             args,
	}, nil
}

func (cmd *operateCommandWrite) writeBuffer(ifc command) (err Error) {
	return cmd.setOperate(cmd.policy, cmd.key, &cmd.args)
}

func (cmd *operateCommandWrite) parseResult(ifc command, conn *Connection) Error {
	rp, err := newRecordParser(&cmd.baseCommand)
	if err != nil {
		return err
	}

	if err := rp.parseFields(cmd.policy.Txn, cmd.key, true); err != nil {
		return err
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, rp.resultCode)
	}

	switch rp.resultCode {
	case types.OK:
		var err Error
		cmd.record, err = rp.parseRecord(cmd.key, true)
		if err != nil {
			return err
		}
		return nil
	case types.KEY_NOT_FOUND_ERROR:
		return ErrKeyNotFound.err()
	case types.FILTERED_OUT:
		return ErrFilteredOut.err()
	default:
		return newError(rp.resultCode)
	}
}

func (cmd *operateCommandWrite) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *operateCommandWrite) commandType() commandType {
	return ttOperate
}

func (cmd *operateCommandWrite) GetRecord() *Record {
	return cmd.record
}

func (cmd *operateCommandWrite) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *operateCommandWrite) getNamespace() *string {
	return &cmd.key.namespace
}

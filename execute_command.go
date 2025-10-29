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

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

type executeCommand struct {
	baseWriteCommand

	record *Record

	packageName  string
	functionName string
	args         *ValueArray
}

func newExecuteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	packageName string,
	functionName string,
	args *ValueArray,
) (executeCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, policy, key)
	if err != nil {
		return executeCommand{}, err
	}

	return executeCommand{
		baseWriteCommand: bwc,
		packageName:      packageName,
		functionName:     functionName,
		args:             args,
	}, nil
}

func (cmd *executeCommand) writeBuffer(ifc command) Error {
	return cmd.setUdf(cmd.policy, cmd.key, cmd.packageName, cmd.functionName, cmd.args)
}

func (cmd *executeCommand) parseResult(ifc command, conn *Connection) Error {
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

	if rp.resultCode != 0 {
		if rp.resultCode == types.KEY_NOT_FOUND_ERROR {
			return ErrKeyNotFound.err()
		} else if rp.resultCode == types.FILTERED_OUT {
			return ErrFilteredOut.err()
		} else if rp.resultCode == types.UDF_BAD_RESPONSE {
			cmd.record, _ = rp.parseRecord(cmd.key, false)
			err := cmd.handleUdfError(rp.resultCode)
			logger.Logger.Debug("UDF execution error: %s", err.Error())
			return err
		}

		return newError(rp.resultCode)
	}

	if rp.opCount == 0 {
		// data Bin was not returned
		cmd.record = newRecord(cmd.node, cmd.key, nil, rp.generation, rp.expiration)
		return nil
	}

	cmd.record, err = rp.parseRecord(cmd.key, false)
	if err != nil {
		return err
	}

	return nil
}

func (cmd *executeCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *executeCommand) commandType() commandType {
	return ttUDF
}

func (cmd *executeCommand) handleUdfError(resultCode types.ResultCode) Error {
	if ret, exists := cmd.record.Bins["FAILURE"]; exists {
		return newError(resultCode, ret.(string))
	}
	return newError(resultCode)
}

func (cmd *executeCommand) GetRecord() *Record {
	return cmd.record
}

func (cmd *executeCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *executeCommand) getNamespace() *string {
	return &cmd.key.namespace
}

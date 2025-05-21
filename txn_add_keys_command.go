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

// guarantee txnAddKeysCommand implements command interface
var _ command = &txnAddKeysCommand{}

type txnAddKeysCommand struct {
	baseWriteCommand

	args operateArgs
	txn  *Txn
}

func newTxnAddKeysCommand(
	cluster *Cluster,
	key *Key,
	args operateArgs,
	txn *Txn,
) (txnAddKeysCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, args.writePolicy, key)
	if err != nil {
		return txnAddKeysCommand{}, err
	}

	newTxnAddKeysCmd := txnAddKeysCommand{
		baseWriteCommand: bwc,
		args:             args,
		txn:              txn,
	}

	return newTxnAddKeysCmd, nil
}

func (cmd *txnAddKeysCommand) writeBuffer(ifc command) Error {
	return cmd.setTxnAddKeys(cmd.policy, cmd.key, cmd.args)
}

func (cmd *txnAddKeysCommand) parseResult(ifc command, conn *Connection) Error {
	rp, err := newRecordParser(&cmd.baseCommand)
	if err != nil {
		return err
	}
	rp.parseTranDeadline(cmd.txn)

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, rp.resultCode)
	}

	if rp.resultCode != types.OK {
		return newCustomNodeError(cmd.node, rp.resultCode)
	}

	return nil
}

func (cmd *txnAddKeysCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *txnAddKeysCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *txnAddKeysCommand) getNamespace() *string {
	return &cmd.key.namespace
}

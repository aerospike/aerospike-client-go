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

// guarantee txnCloseCommand implements command interface
var _ command = &txnCloseCommand{}

type txnCloseCommand struct {
	baseWriteCommand

	txn *Txn
}

func newTxnCloseCommand(
	cluster *Cluster,
	txn *Txn,
	writePolicy *WritePolicy,
	key *Key,
) (txnCloseCommand, Error) {
	bwc, err := newBaseWriteCommand(cluster, writePolicy, key)
	if err != nil {
		return txnCloseCommand{}, err
	}

	newTxnCloseCmd := txnCloseCommand{
		baseWriteCommand: bwc,
		txn:              txn,
	}

	return newTxnCloseCmd, nil
}

func (cmd *txnCloseCommand) writeBuffer(ifc command) Error {
	return cmd.setTxnClose(cmd.txn, cmd.key)
}

func (cmd *txnCloseCommand) parseResult(ifc command, conn *Connection) Error {
	resultCode, err := cmd.parseHeader()
	if err != nil {
		return newCustomNodeError(cmd.node, err.resultCode())
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, resultCode)
	}

	if resultCode == 0 || resultCode == types.KEY_NOT_FOUND_ERROR {
		return nil
	}

	return newCustomNodeError(cmd.node, types.ResultCode(resultCode))
}

func (cmd *txnCloseCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *txnCloseCommand) onInDoubt() {
	return
}

func (cmd *txnCloseCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *txnCloseCommand) getNamespace() *string {
	return &cmd.key.namespace
}

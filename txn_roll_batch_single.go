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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// guarantee batchSingleTxnRollCommand implements command interface
var _ command = &batchSingleTxnRollCommand{}

type batchSingleTxnRollCommand struct {
	singleCommand

	txn    *Txn
	record *BatchRecord
	attr   int
	policy *BatchPolicy
}

func newBatchSingleTxnRollCommand(
	client *Client,
	policy *BatchPolicy,
	txn *Txn,
	record *BatchRecord,
	node *Node,
	attr int,
) (batchSingleTxnRollCommand, Error) {
	var partition *Partition
	var err Error
	if client.cluster != nil {
		partition, err = PartitionForWrite(client.cluster, &policy.BasePolicy, record.Key)
		if err != nil {
			return batchSingleTxnRollCommand{}, err
		}
	}

	res := batchSingleTxnRollCommand{
		singleCommand: newSingleCommand(client.cluster, record.Key, partition),
		txn:           txn,
		record:        record,
		attr:          attr,
		policy:        policy,
	}
	res.node = node

	return res, nil
}

func (cmd *batchSingleTxnRollCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchSingleTxnRollCommand) writeBuffer(ifc command) Error {
	return cmd.setTxnRoll(cmd.key, cmd.txn, cmd.attr)
}

func (cmd *batchSingleTxnRollCommand) getNode(ifc command) (*Node, Error) {
	return cmd.node, nil
}

func (cmd *batchSingleTxnRollCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryWrite(isTimeout)
	node, err := cmd.partition.GetNodeWrite(cmd.cluster)
	if err != nil {
		return false
	}

	cmd.node = node
	return true
}

func (cmd *batchSingleTxnRollCommand) parseResult(ifc command, conn *Connection) Error {
	rp, err := newRecordParser(&cmd.baseCommand)
	if err != nil {
		return err
	}

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, rp.resultCode)
	}

	if rp.resultCode == 0 {
		cmd.record.ResultCode = types.OK
	} else {
		err := newError(rp.resultCode)
		return err.setInDoubt(cmd.isRead(), cmd.commandSentCounter)
	}

	return nil
}

func (cmd *batchSingleTxnRollCommand) setInDoubt() bool {
	if cmd.record.ResultCode == types.NO_RESPONSE {
		cmd.record.InDoubt = true
		return true
	}
	return false
}

func (cmd *batchSingleTxnRollCommand) isRead() bool {
	return false
}

func (cmd *batchSingleTxnRollCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchSingleTxnRollCommand) commandType() commandType {
	return ttPut
}


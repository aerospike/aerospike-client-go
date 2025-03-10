// Copyright 2014-2024 Aerospike, Inc.
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

type TxnRoll struct {
	client        *Client
	txn           *Txn
	verifyRecords []*BatchRecord
	rollRecords   []*BatchRecord
}

func NewTxnRoll(client *Client, txn *Txn) *TxnRoll {
	return &TxnRoll{
		client: client,
		txn:    txn,
	}
}

func (txr *TxnRoll) Verify(verifyPolicy, rollPolicy *BatchPolicy) Error {
	if err := txr.VerifyRecordVersions(verifyPolicy); err != nil {
		txr.txn.SetState(TxnStateAborted)

		if err := txr.Roll(rollPolicy, _INFO4_MRT_ROLL_BACK); err != nil {
			return NewTxnCommitError(CommitErrorVerifyFailAbortAbandoned, txr.verifyRecords, txr.rollRecords, err)

		}

		if txr.txn.CloseMonitor() {
			writePolicy := NewWritePolicy(0, 0)
			writePolicy.BasePolicy = rollPolicy.BasePolicy

			txnKey := getTxnMonitorKey(txr.txn)
			if err := txr.Close(writePolicy, txnKey); err != nil {
				return NewTxnCommitError(CommitErrorVerifyFailCloseAbandoned, txr.verifyRecords, txr.rollRecords, err)
			}
		}

		return NewTxnCommitError(CommitErrorVerifyFail, txr.verifyRecords, txr.rollRecords, err)
	}
	txr.txn.SetState(TxnStateVerified)
	return nil
}

func (txr *TxnRoll) Commit(rollPolicy *BatchPolicy) (CommitStatus, Error) {
	writePolicy := NewWritePolicy(0, 0)
	writePolicy.BasePolicy = rollPolicy.BasePolicy

	txnKey := getTxnMonitorKey(txr.txn)

	if txr.txn.MonitorExists() {
		if err := txr.MarkRollForward(writePolicy, txnKey); err != nil {
			aec := NewTxnCommitError(CommitErrorMarkRollForwardAbandoned, txr.verifyRecords, txr.rollRecords, err)

			if err.resultCode() == types.MRT_ABORTED {
				aec.markInDoubt(false)
				txr.txn.SetInDoubt(false)
				txr.txn.SetState(TxnStateAborted)
			} else if txr.txn.GetInDoubt() {
				aec.markInDoubt(true)
			} else if err.IsInDoubt() {
				aec.markInDoubt(true)
				txr.txn.SetInDoubt(true)
			}
			return CommitStatusRollForwardAbandoned, aec
		}
	}

	txr.txn.SetState(TxnStateCommitted)
	txr.txn.SetInDoubt(false)

	if err := txr.Roll(rollPolicy, _INFO4_MRT_ROLL_FORWARD); err != nil {
		return CommitStatusRollForwardAbandoned, err
	}

	if txr.txn.CloseMonitor() {
		txnKey := getTxnMonitorKey(txr.txn)
		if err := txr.Close(writePolicy, txnKey); err != nil {
			return CommitStatusCloseAbandoned, err
		}
	}
	return CommitStatusOK, nil
}

func (txr *TxnRoll) Abort(rollPolicy *BatchPolicy) (AbortStatus, Error) {
	txr.txn.SetState(TxnStateAborted)

	if err := txr.Roll(rollPolicy, _INFO4_MRT_ROLL_BACK); err != nil {
		return AbortStatusRollBackAbandoned, err
	}

	if txr.txn.CloseMonitor() {
		writePolicy := NewWritePolicy(0, 0)
		writePolicy.BasePolicy = rollPolicy.BasePolicy

		txnKey := getTxnMonitorKey(txr.txn)
		if err := txr.Close(writePolicy, txnKey); err != nil {
			return AbortStatusCloseAbandoned, err
		}
	}
	return AbortStatusOK, nil
}

func (txr *TxnRoll) VerifyRecordVersions(verifyPolicy *BatchPolicy) Error {
	// Validate record versions in a batch.
	reads := txr.txn.GetReads()
	max := len(reads)

	if max == 0 {
		return nil
	}

	records := make([]*BatchRecord, max)
	keys := make([]*Key, max)
	versions := make([]*uint64, max)
	count := 0

	for key, ver := range reads {
		keys[count] = key
		records[count] = newSimpleBatchRecord(key, false)
		versions[count] = ver
		count++
	}

	txr.verifyRecords = records

	bns, err := newBatchNodeList(txr.client.cluster, verifyPolicy, keys, records, false)
	if err != nil {
		return err
	}
	commands := make([]command, len(bns))

	for count, bn := range bns {
		if len(bn.offsets) == 1 {
			i := bn.offsets[0]
			cmd, err := newBatchSingleTxnVerifyCommand(txr.client, verifyPolicy, versions[i], records[i], bn.Node)
			if err != nil {
				return err
			}
			commands[count] = &cmd
		} else {
			commands[count] = newTxnBatchVerifyCommand(txr.client, bn, verifyPolicy, keys, versions, records)
		}
	}

	if err := txr.client.batchExecuteSimple(verifyPolicy, commands); err != nil {
		return newErrorAndWrap(err, types.COMMON_ERROR, "Failed to verify one or more record versions")
	}
	return nil
}

func (txr *TxnRoll) MarkRollForward(writePolicy *WritePolicy, txnKey *Key) Error {
	// Tell Transaction monitor that a roll-forward will commence.
	cmd, err := newTxnMarkRollForwardCommand(txr.client.cluster, writePolicy, txnKey)
	if err != nil {
		return err
	}
	return cmd.execute(&cmd)
}

func (txr *TxnRoll) Roll(rollPolicy *BatchPolicy, txnAttr int) Error {
	keySet := txr.txn.GetWrites()

	if len(keySet) == 0 {
		return nil
	}

	keys := make([]*Key, len(keySet))
	copy(keys, keySet)
	records := make([]*BatchRecord, len(keys))

	for i := 0; i < len(keys); i++ {
		records[i] = newSimpleBatchRecord(keys[i], true)
	}

	txr.rollRecords = records

	attr := batchAttr{}
	attr.setTxn(txnAttr)

	bns, err := newBatchNodeList(txr.client.cluster, rollPolicy, keys, records, true)
	if err != nil {
		return err
	}

	commands := make([]command, len(bns))
	for count, bn := range bns {
		if len(bn.offsets) == 1 {
			i := bn.offsets[0]
			cmd, err := newBatchSingleTxnRollCommand(txr.client, rollPolicy, txr.txn, records[i], bn.Node, txnAttr)
			if err != nil {
				return err
			}
			commands[count] = &cmd
		} else {
			commands[count] = newBatchTxnRollCommand(txr.client, bn, rollPolicy, txr.txn, keys, records, &attr)
		}
	}

	if err = txr.client.batchExecuteSimple(rollPolicy, commands); err != nil {
		rollString := "abort"
		if txnAttr == _INFO4_MRT_ROLL_FORWARD {
			rollString = "commit"
		}
		return newErrorAndWrap(err, types.COMMON_ERROR, "Failed to "+rollString+" one or more record versions")
	}

	return nil
}

func (txr *TxnRoll) Close(writePolicy *WritePolicy, txnKey *Key) Error {
	// Delete Transaction monitor on server.
	cmd, err := newTxnCloseCommand(txr.client.cluster, txr.txn, writePolicy, txnKey)
	if err != nil {
		return err
	}

	if err := cmd.execute(&cmd); err != nil {
		return err
	}

	// Reset Transaction on client.
	cmd.txn.Clear()

	return nil
}


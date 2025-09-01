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

type TxnMonitor struct{}

var txnMonitor = new(TxnMonitor)

var txnOrderedListPolicy = &ListPolicy{
	attributes: ListOrderOrdered,
	flags:      ListWriteFlagsAddUnique | ListWriteFlagsNoFail | ListWriteFlagsPartial,
}

const binNameId = "id"
const binNameDigests = "keyds"

func (tm *TxnMonitor) addKey(cluster *Cluster, policy *WritePolicy, cmdKey *Key) Error {
	txn := policy.Txn
	if err := txn.VerifyCommand(); err != nil {
		return err
	}

	if txn.WriteExistsForKey(cmdKey) {
		// Transaction monitor already contains this key.
		return nil
	}

	ops, err := tm.getTranOps(txn, cmdKey)
	if err != nil {
		return err
	}
	return tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
}

func (tm *TxnMonitor) addKeys(cluster *Cluster, policy *BatchPolicy, keys []*Key) Error {
	ops, err := tm.getTranOpsFromKeys(policy.Txn, keys)
	if err != nil {
		return err
	}
	return tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
}

func (tm *TxnMonitor) addKeysFromRecords(cluster *Cluster, policy *BatchPolicy, records []BatchRecordIfc) Error {
	ops, err := tm.getTranOpsFromBatchRecords(policy.Txn, records)
	if err != nil {
		return err
	}

	if len(ops) > 0 {
		return tm.addWriteKeys(cluster, policy.GetBasePolicy(), ops)
	}
	return nil
}

func (tm *TxnMonitor) getTranOps(txn *Txn, cmdKey *Key) ([]*Operation, Error) {
	if err := txn.SetNamespace(cmdKey.namespace); err != nil {
		return nil, err
	}

	if txn.MonitorExists() {
		return []*Operation{
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, cmdKey.Digest()),
		}, nil
	} else {
		return []*Operation{
			PutOp(NewBin(binNameId, txn.Id())),
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, cmdKey.Digest()),
		}, nil
	}
}

func (tm *TxnMonitor) getTranOpsFromKeys(txn *Txn, keys []*Key) ([]*Operation, Error) {
	if err := txn.VerifyCommand(); err != nil {
		return nil, err
	}

	list := make([]interface{}, 0, len(keys))

	for _, key := range keys {
		if err := txn.SetNamespace(key.namespace); err != nil {
			return nil, err
		}
		list = append(list, NewBytesValue(key.Digest()))
	}
	return tm.getTranOpsFromValueList(txn, list), nil
}

func (tm *TxnMonitor) getTranOpsFromBatchRecords(txn *Txn, records []BatchRecordIfc) ([]*Operation, Error) {
	if err := txn.VerifyCommand(); err != nil {
		return nil, err
	}

	list := make([]interface{}, 0, len(records))

	for _, br := range records {
		if err := txn.SetNamespace(br.key().namespace); err != nil {
			return nil, err
		}

		if br.BatchRec().hasWrite {
			list = append(list, br.key().Digest())
		}
	}

	if len(list) == 0 {
		// Readonly batch does not need to add key digests.
		return nil, nil
	}
	return tm.getTranOpsFromValueList(txn, list), nil
}

func (tm *TxnMonitor) getTranOpsFromValueList(txn *Txn, list []interface{}) []*Operation {
	if txn.MonitorExists() {
		return []*Operation{
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, list...),
		}
	} else {
		return []*Operation{
			PutOp(NewBin(binNameId, txn.Id())),
			ListAppendWithPolicyOp(txnOrderedListPolicy, binNameDigests, list...),
		}
	}
}

func (tm *TxnMonitor) addWriteKeys(cluster *Cluster, policy *BasePolicy, ops []*Operation) Error {
	txn := policy.Txn
	txnKey := getTxnMonitorKey(txn)
	wp := tm.copyTimeoutPolicy(policy)
	args, err := newOperateArgs(cluster, wp, txnKey, ops)
	if err != nil {
		return err
	}

	cmd, err := newTxnAddKeysCommand(cluster, txnKey, args, txn)
	if err != nil {
		return err
	}
	return cmd.Execute()
}

func (tm *TxnMonitor) copyTimeoutPolicy(policy *BasePolicy) *WritePolicy {
	// Inherit some fields from the original command's policy.
	wp := NewWritePolicy(0, 0)
	// wp.Txn = policy.Txn
	// wp.ConnectTimeout = policy.ConnectTimeout
	wp.SocketTimeout = policy.SocketTimeout
	wp.TotalTimeout = policy.TotalTimeout
	wp.TimeoutDelay = policy.TimeoutDelay
	wp.MaxRetries = policy.MaxRetries
	wp.SleepBetweenRetries = policy.SleepBetweenRetries
	wp.UseCompression = policy.UseCompression
	wp.RespondPerEachOp = true

	// Note that the server only accepts the timeout on Transaction monitor record create.
	// The server ignores the Transaction timeout field on successive Transaction monitor record
	// updates.
	wp.Expiration = uint32(policy.Txn.timeout)

	return wp
}

func getTxnMonitorKey(txn *Txn) *Key {
	if txn.GetNamespace() != nil {
		key, _ := NewKey(*txn.GetNamespace(), "<ERO~MRT", txn.Id())
		return key
	}
	return nil
}

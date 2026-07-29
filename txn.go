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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// Transaction state.
type TxnState byte

const (
	TxnStateOpen TxnState = iota
	TxnStateVerified
	TxnStateCommitFailed
	TxnStateCommitted
	TxnStateAborted
)

var txnRandomState atomic.Int64

func init() {
	txnRandomState.Store(time.Now().UnixNano())
}

// Transaction. Each command in the Transaction must use the same namespace.
type Txn struct {
	id           int64
	reads        keyMap[*uint64]
	writes       keyMap[struct{}]
	state        TxnState
	namespace    *string
	timeout      int
	deadline     int
	writeInDoubt bool
	inDoubt      bool
}

// Create Transaction, assign random transaction id and initialize reads/writes hashmaps with default capacities.
//
// The default client Transaction timeout is zero. This means use the server configuration transaction-duration
// as the Transaction timeout. The default transaction-duration is 10 seconds.
func NewTxn() *Txn {
	return &Txn{
		id:      createTxnId(),
		reads:   *newKeyMap[*uint64](16),
		writes:  *newKeyMap[struct{}](16),
		timeout: 0,
		state:   TxnStateOpen,
	}
}

// Create Transaction, assign random transaction id and initialize reads/writes hashmaps with given capacities.
//
// readsCapacity     expected number of record reads in the Transaction. Minimum value is 16.
// writesCapacity    expected number of record writes in the Transaction. Minimum value is 16.
func NewTxnWithCapacity(readsCapacity, writesCapacity int) *Txn {
	if readsCapacity < 16 {
		readsCapacity = 16
	}

	if writesCapacity < 16 {
		writesCapacity = 16
	}

	return &Txn{
		id:      createTxnId(),
		reads:   *newKeyMap[*uint64](readsCapacity),
		writes:  *newKeyMap[struct{}](writesCapacity),
		timeout: 0,
		state:   TxnStateOpen,
	}
}

func createTxnId() int64 {
	// xorshift64* doesn't generate zeroes.
	var oldState, newState int64

	oldState = txnRandomState.Load()
	newState = oldState
	newState ^= newState >> 12
	newState ^= newState << 25
	newState ^= newState >> 27

	for !txnRandomState.CompareAndSwap(oldState, newState) {
		oldState = txnRandomState.Load()
		newState = oldState
		newState ^= newState >> 12
		newState ^= newState << 25
		newState ^= newState >> 27
	}

	return newState // 0x2545f4914f6cdd1dl;
}

// Return Transaction ID.
func (txn *Txn) Id() int64 {
	return txn.id
}

// Return Transaction ID.
func (txn *Txn) State() TxnState {
	return txn.state
}

// Set Transaction ID.
func (txn *Txn) SetState(state TxnState) {
	txn.state = state
}

// Process the results of a record read. For internal use only.
func (txn *Txn) OnRead(key *Key, version *uint64) {
	if version != nil {
		txn.reads.Set(key, version)
	}
}

// Get record version for a given key.
func (txn *Txn) GetReadVersion(key *Key) *uint64 {
	return txn.reads.Get(key)
}

// Get all read keys and their versions.
func (txn *Txn) ReadExistsForKey(key *Key) bool {
	return txn.reads.Exists(key)
}

// Get all read keys and their versions.
func (txn *Txn) GetReads() map[*Key]*uint64 {
	return txn.reads.Clone()
}

// Process the results of a record write. For internal use only.
func (txn *Txn) OnWrite(key *Key, version *uint64, resultCode types.ResultCode) {
	if version != nil {
		txn.reads.Set(key, version)
	} else if resultCode == 0 {
		txn.reads.Delete(key)
		txn.writes.Set(key, struct{}{})
	}
}

// Add key to write hash when write command is in doubt (usually caused by timeout).
func (txn *Txn) OnWriteInDoubt(key *Key) {
	txn.writeInDoubt = true
	txn.reads.Delete(key)
	txn.writes.Set(key, struct{}{})
}

// Get all write keys and their versions.
func (txn *Txn) GetWrites() []*Key {
	return txn.writes.Keys()
}

// Get all write keys and their versions.
func (txn *Txn) WriteExistsForKey(key *Key) bool {
	return txn.writes.Exists(key)
}

// Return Transaction namespace.
func (txn *Txn) GetNamespace() *string {
	return txn.namespace
}

// Verify current Transaction state and namespace for a future read command.
func (txn *Txn) prepareRead(ns string) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.SetNamespace(ns)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareReadForKeys(keys []*Key) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForKeys(keys)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareBatchReads(records []*BatchRead) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForBatchReads(records)
}

// Verify current Transaction state and namespaces for a future batch read command.
func (txn *Txn) prepareReadForBatchRecordsIfc(records []BatchRecordIfc) Error {
	if err := txn.VerifyCommand(); err != nil {
		return err
	}
	return txn.setNamespaceForBatchRecordsIfc(records)
}

// Verify that the Transaction state allows future commands.
func (txn *Txn) VerifyCommand() Error {
	if txn.state != TxnStateOpen {
		return newError(types.COMMON_ERROR, "Issuing commands to this transaction is forbidden because it has been ended by a commit or abort")
	}
	return nil
}

// markCommitFailed transitions to COMMIT_FAILED when mark-roll-forward fails in an in-doubt state.
// The server may still roll the transaction forward; Abort must not be called.
func (txn *Txn) markCommitFailed() {
	if txn.state != TxnStateAborted && txn.state != TxnStateCommitted {
		txn.state = TxnStateCommitFailed
	}
}

// Set Transaction namespace only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) SetNamespace(ns string) Error {
	if txn.namespace == nil {
		txn.namespace = &ns
	} else if *txn.namespace != ns {
		return newError(types.COMMON_ERROR, "Namespace must be the same for all commands in the Transaction. orig: "+
			*txn.namespace+" new: "+ns)
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForKeys(keys []*Key) Error {
	for _, key := range keys {
		if err := txn.SetNamespace(key.namespace); err != nil {
			return err
		}
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForBatchReads(records []*BatchRead) Error {
	for _, br := range records {
		if err := txn.SetNamespace(br.key().namespace); err != nil {
			return err
		}
	}
	return nil
}

// Set Transaction namespaces for each key only if doesn't already exist.
// If namespace already exists, verify new namespace is the same.
func (txn *Txn) setNamespaceForBatchRecordsIfc(records []BatchRecordIfc) Error {
	for _, br := range records {
		if err := txn.SetNamespace(br.key().namespace); err != nil {
			return err
		}
	}
	return nil
}

// Get Transaction deadline.
func (txn *Txn) GetTimeout() time.Duration {
	return time.Duration(txn.timeout) * time.Second
}

// Set Transaction timeout in seconds. The timer starts when the Transaction monitor record is
// created.
// This occurs when the first command in the Transaction is executed. If the timeout is
// reached before
// a commit or abort is called, the server will expire and rollback the Transaction.
//
// If the Transaction timeout is zero, the server configuration transaction-duration is used.
// The default transaction-duration is 10 seconds.
func (txn *Txn) SetTimeout(timeout time.Duration) {
	txn.timeout = int(timeout / time.Second)
}

// Get Transaction inDoubt.
func (txn *Txn) GetInDoubt() bool {
	return txn.inDoubt
}

// Set Transaction inDoubt. For internal use only.
func (txn *Txn) SetInDoubt(inDoubt bool) {
	txn.inDoubt = inDoubt
}

// Return if the MRT monitor record should be closed/deleted. For internal use only.
func (txn *Txn) CloseMonitor() bool {
	return txn.deadline != 0 && !txn.writeInDoubt
}

// Does Transaction monitor record exist.
func (txn *Txn) MonitorExists() bool {
	return txn.deadline != 0
}

// Clear Transaction. Remove all tracked keys.
func (txn *Txn) Clear() {
	txn.namespace = nil
	txn.deadline = 0
	txn.reads.Clear()
	txn.writes.Clear()
}

////////////////////////////////////////////////////////////////////////////
//
// Specialized internal data type to simplify key bookkeeping
//
////////////////////////////////////////////////////////////////////////////

type keyTupple[V any] struct {
	k *Key
	v V
}

// keyMap implements a keyMap with atomic semantics.
type keyMap[V any] struct {
	m     map[[20]byte]*keyTupple[V]
	mutex sync.RWMutex
}

// New generates a new Map instance.
func newKeyMap[V any](length int) *keyMap[V] {
	return &keyMap[V]{
		m: make(map[[20]byte]*keyTupple[V], length),
	}
}

// Exists atomically checks if a key exists in the map
func (m *keyMap[V]) Exists(k *Key) bool {
	if k != nil {
		m.mutex.RLock()
		_, ok := m.m[k.digest]
		m.mutex.RUnlock()
		return ok
	}
	return false
}

// Get atomically retrieves an element from the Map.
func (m *keyMap[V]) Get(k *Key) V {
	if k != nil {
		m.mutex.RLock()
		res, found := m.m[k.digest]
		m.mutex.RUnlock()
		if found {
			return res.v
		}
	}

	var zero V
	return zero
}

// Set atomically sets an element in the Map.
// If idx is out of range, it will return an error.
func (m *keyMap[V]) Set(k *Key, v V) {
	if k != nil {
		m.mutex.Lock()
		m.m[k.digest] = &keyTupple[V]{k: k, v: v}
		m.mutex.Unlock()
	}
}

// Clone copies the map and returns the copy.
func (m *keyMap[V]) Clone() map[*Key]V {
	m.mutex.RLock()
	res := make(map[*Key]V, len(m.m))
	for _, v := range m.m {
		res[v.k] = v.v
	}
	m.mutex.RUnlock()

	return res
}

// Returns the keys from the map.
func (m *keyMap[V]) Keys() []*Key {
	m.mutex.RLock()
	res := make([]*Key, 0, len(m.m))
	for _, v := range m.m {
		res = append(res, v.k)
	}
	m.mutex.RUnlock()

	return res
}

// Clear will remove all entries.
func (m *keyMap[V]) Clear() {
	m.mutex.Lock()
	m.m = make(map[[20]byte]*keyTupple[V], len(m.m))
	m.mutex.Unlock()
}

// Delete will remove the key and return its value.
func (m *keyMap[V]) Delete(k *Key) V {
	if k != nil {
		m.mutex.Lock()
		res, ok := m.m[k.digest]
		delete(m.m, k.digest)
		m.mutex.Unlock()

		if ok {
			return res.v
		}
	}

	var zero V
	return zero
}

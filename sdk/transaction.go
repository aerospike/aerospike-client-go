//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
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

package sdk

import (
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// TransactionalSession is a session with an ambient multi-record transaction.
//
// It embeds the session, so every verb works inside the transaction and the
// builders stamp the transaction onto the policies they issue. Use
// [QueryBuilder.WithTxn] with nil to run one operation outside it.
//
// Finalize with [TransactionalSession.Commit] or
// [TransactionalSession.Abort]. Go has no destructor, so an unfinalized
// transaction is left to expire server-side.
type TransactionalSession struct {
	*Session

	txn       *as.Txn
	finalized atomic.Bool
}

// Transaction begins a multi-record transaction.
//
// Transactions require a strong-consistency namespace on a server that
// supports them (8.0+).
func (s *Session) Transaction() (*TransactionalSession, error) {
	return s.beginTransaction(0)
}

// TransactionWithTimeout begins a transaction with an explicit deadline
// instead of the server's default duration. The timeout is rounded to seconds.
func (s *Session) TransactionWithTimeout(d time.Duration) (*TransactionalSession, error) {
	return s.beginTransaction(d)
}

// beginTransaction mints a transaction-bound session.
func (s *Session) beginTransaction(d time.Duration) (*TransactionalSession, error) {
	txn := as.NewTxn()
	if d > 0 {
		txn.SetTimeout(d)
	}
	inner, err := newSession(s.client, s.behavior, txn)
	if err != nil {
		return nil, err
	}
	return &TransactionalSession{Session: inner, txn: txn}, nil
}

// Txn reports the underlying transaction.
func (t *TransactionalSession) Txn() *as.Txn { return t.txn }

// verifyRollPolicies resolves the commit and abort phase policies from this
// session's behavior, through the two system scopes.
func (t *TransactionalSession) verifyRollPolicies() (*as.TxnVerifyPolicy, *as.TxnRollPolicy, error) {
	verify, err := ToTxnVerifyPolicy(t.behavior.SystemSettingsFor(ScopeSystemTxnVerify))
	if err != nil {
		return nil, nil, err
	}
	roll, err := ToTxnRollPolicy(t.behavior.SystemSettingsFor(ScopeSystemTxnRoll))
	if err != nil {
		return nil, nil, err
	}
	return verify, roll, nil
}

// Commit commits the transaction. Finalizing twice is an error.
//
// The verify and roll phases use the policies resolved from this session's
// behavior (the SystemTxnVerify and SystemTxnRoll scopes), so timeouts,
// retries, replica choice and read consistency for those phases are
// configurable per Behavior.
func (t *TransactionalSession) Commit() (as.CommitStatus, error) {
	if t.finalized.Swap(true) {
		return "", NewError(KindTransaction, "this transaction has already been finalized")
	}
	core, err := t.client.UnderlyingClient()
	if err != nil {
		return "", err
	}
	verify, roll, err := t.verifyRollPolicies()
	if err != nil {
		return "", err
	}
	status, aerr := core.CommitWithPolicies(verify, roll, t.txn)
	if aerr != nil {
		return status, WrapError(aerr)
	}
	return status, nil
}

// Abort abandons the transaction. Finalizing twice is an error.
func (t *TransactionalSession) Abort() (as.AbortStatus, error) {
	if t.finalized.Swap(true) {
		return "", NewError(KindTransaction, "this transaction has already been finalized")
	}
	core, err := t.client.UnderlyingClient()
	if err != nil {
		return "", err
	}
	_, roll, err := t.verifyRollPolicies()
	if err != nil {
		return "", err
	}
	status, aerr := core.AbortWithPolicy(roll, t.txn)
	if aerr != nil {
		return status, WrapError(aerr)
	}
	return status, nil
}

// Rollback is an alias for [TransactionalSession.Abort].
func (t *TransactionalSession) Rollback() (as.AbortStatus, error) { return t.Abort() }

// DoInTransaction runs a function inside a transaction, committing on success
// and aborting on failure.
//
// It retries on the transient conflicts a transaction can hit -- a blocked
// transaction, a version mismatch, or a failed commit -- up to maxAttempts,
// sleeping between tries.
func (s *Session) DoInTransaction(
	fn func(tx *Session) error,
	maxAttempts int,
	sleepBetween time.Duration,
) error {
	if maxAttempts < 1 {
		return NewError(KindInvalidArgument, "maxAttempts must be at least 1")
	}

	var lastErr error
	for attempt := range maxAttempts {
		tx, err := s.Transaction()
		if err != nil {
			return err
		}

		runErr := fn(tx.Session)
		if runErr != nil {
			if _, abortErr := tx.Abort(); abortErr != nil {
				// An abort failure does not mask the original error.
				_ = abortErr
			}
			lastErr = runErr
			if !isRetryableTxnError(runErr) || attempt == maxAttempts-1 {
				return runErr
			}
		} else {
			if _, commitErr := tx.Commit(); commitErr == nil {
				return nil
			} else {
				lastErr = commitErr
				if !isRetryableTxnError(commitErr) || attempt == maxAttempts-1 {
					return commitErr
				}
			}
		}

		if sleepBetween > 0 {
			time.Sleep(sleepBetween)
		}
	}
	return lastErr
}

// isRetryableTxnError reports whether a transaction failure is transient.
func isRetryableTxnError(err error) bool {
	var e *Error
	if !errorsAs(err, &e) {
		return false
	}
	if e.Kind() == KindCommit {
		return true
	}
	return e.Matches(types.MRT_BLOCKED, types.MRT_VERSION_MISMATCH)
}

// errorsAs is a small errors.As for the SDK error type, avoiding an import
// cycle with the standard library helper in hot paths.
func errorsAs(err error, target **Error) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

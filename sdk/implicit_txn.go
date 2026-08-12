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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// implicitTxnApplies reports whether a batch should be wrapped in an implicit
// multi-record transaction, so its writes commit atomically.
//
// Every condition must hold:
//
//  1. the namespace runs in strong-consistency mode,
//  2. the batch contains writes,
//  3. no explicit transaction is active and none was explicitly declined,
//  4. every cluster node supports transactions, and
//  5. the setting is enabled, which it is by default.
//
// No API change is needed to benefit: ordinary batch writes on a
// strong-consistency namespace gain atomicity.
func (c *chain) implicitTxnApplies(specs []operationSpec) bool {
	if c.txn != nil || c.txnOptOut {
		return false
	}
	if c.mode() != ModeSC {
		return false
	}

	hasWrites := false
	rows := 0
	for _, s := range specs {
		rows += len(s.keys)
		if s.verb.isWrite() {
			hasWrites = true
		}
	}
	if !hasWrites || rows < 2 {
		return false
	}

	settings := c.session.client.SystemSettings()
	if !settings.implicitBatchWriteTransactions() {
		return false
	}
	return c.session.client.SupportsMRT()
}

// runInImplicitTxn wraps attempt in a fresh transaction, committing on success
// and aborting on failure, retrying the transient conflicts.
func (c *chain) runInImplicitTxn(attempt func(txn *as.Txn) (*RecordStream, error)) (*RecordStream, error) {
	core, err := c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	settings := c.session.client.SystemSettings()
	attempts := settings.numberOfAttempts()
	sleep := settings.sleepBetweenAttempts()

	// The verify and roll phases use the policies resolved from the session's
	// Behavior, exactly as an explicit transaction does, so the
	// SystemTxnVerify and SystemTxnRoll scopes apply here too.
	verify, err := ToTxnVerifyPolicy(c.session.behavior.SystemSettingsFor(ScopeSystemTxnVerify))
	if err != nil {
		return nil, err
	}
	roll, err := ToTxnRollPolicy(c.session.behavior.SystemSettingsFor(ScopeSystemTxnRoll))
	if err != nil {
		return nil, err
	}

	var lastErr error
	for i := range attempts {
		txn := as.NewTxn()

		stream, runErr := attempt(txn)
		if runErr == nil {
			if _, commitErr := core.CommitWithPolicies(verify, roll, txn); commitErr == nil {
				return stream, nil
			} else {
				if stream != nil {
					stream.Close()
				}
				lastErr = WrapError(commitErr)
			}
		} else {
			if stream != nil {
				stream.Close()
			}
			if _, abortErr := core.AbortWithPolicy(roll, txn); abortErr != nil {
				logger.Logger.Debug("sdk: implicit transaction abort failed: %s", abortErr)
			}
			lastErr = runErr
		}

		if !isRetryableTxnError(lastErr) || i == attempts-1 {
			return nil, lastErr
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
	return nil, lastErr
}

// retryableImplicitCodes are the result codes that make an implicit
// transaction worth retrying.
var retryableImplicitCodes = []types.ResultCode{
	types.MRT_BLOCKED,
	types.MRT_VERSION_MISMATCH,
}

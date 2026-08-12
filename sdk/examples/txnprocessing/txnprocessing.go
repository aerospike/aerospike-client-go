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

// Package txnprocessing posts a card transaction across three records.
//
// Port of the Java SDK's TransactionProcessingExample, by way of the Rust SDK's
// `transaction_processing`. A payment touches three records at once -- the
// transaction log, the account balance, and the customer's running total and
// status tier -- so all three writes are expressed as one chained multi-record
// write, with the status tier computed server-side from the freshly incremented
// total.
//
// The example runs that posting twice:
//
//  1. against the ordinary availability-mode namespace, where the chained write
//     is a single batch;
//  2. inside an explicit multi-record transaction, which commits all three
//     records atomically, and then a second posting that is aborted instead --
//     leaving nothing behind. Transactions need a strong-consistency namespace,
//     so this half stands down when none is configured.
package txnprocessing

import (
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

const (
	customersSet = "txn_customers"
	accountsSet  = "txn_accounts"
	txnsSet      = "txn_txns"

	customerID = "CUST-10042"
	pan        = "4532015112830366"
	txnID      = "TXN-00001"
	// abortedTxnID is a second posting, written inside a transaction that is
	// then aborted.
	abortedTxnID = "TXN-00002"

	amountCents = int64(45_000)
)

// Run executes the example.
func Run(env *exrun.Env) error {
	// ============================================================
	// Part 1: the posting as one chained multi-record write
	// ============================================================
	env.Printf("=== Posting a transaction on namespace %q (availability mode) ===", env.Namespace)
	sets, err := newSets(env, false)
	if err != nil {
		return err
	}
	if err := sets.seed(env.Session); err != nil {
		return err
	}
	rows, err := sets.post(env.Session, txnID)
	if err != nil {
		return err
	}
	env.Printf("  chained write touched %d records", rows)
	if err := sets.report(env); err != nil {
		return err
	}

	// ============================================================
	// Part 2: the same posting inside a multi-record transaction
	// ============================================================
	env.Printf("")
	env.Printf("=== The same posting inside a multi-record transaction ===")
	if env.SkipSC("the transaction half") {
		return nil
	}
	status, err := env.Session.NamespaceScStatus(env.SCNamespace)
	if err != nil {
		return err
	}
	if !status.IsSC {
		env.Printf("  skipping: %s", status.Detail)
		return nil
	}
	env.Printf("  using SC namespace %q", env.SCNamespace)

	sc, err := newSets(env, true)
	if err != nil {
		return err
	}
	if err := sc.seed(env.Session); err != nil {
		return err
	}

	// DoInTransaction opens a transaction, runs the function, commits on success,
	// and aborts and retries the whole function when the server reports a
	// transient transaction conflict.
	written := 0
	if err := env.Session.DoInTransaction(func(tx *sdk.Session) error {
		n, err := sc.post(tx, txnID)
		written = n
		return err
	}, 5, 200*time.Millisecond); err != nil {
		return err
	}
	env.Printf("  committed a transaction over %d records", written)
	if err := sc.report(env); err != nil {
		return err
	}

	// An aborted transaction leaves nothing behind: a second posting is rolled
	// back instead of committed, so neither the customer total nor the new
	// transaction record survives.
	customerKey := sc.customer()
	before, err := readBin(env.Session, customerKey, "totalSpend")
	if err != nil {
		return err
	}
	tx, err := env.Session.Transaction()
	if err != nil {
		return err
	}
	if _, err := sc.post(tx.Session, abortedTxnID); err != nil {
		return err
	}
	abortStatus, err := tx.Abort()
	if err != nil {
		return err
	}
	after, err := readBin(env.Session, customerKey, "totalSpend")
	if err != nil {
		return err
	}
	env.Printf("  abort status %q: totalSpend %v -> %v (unchanged)", abortStatus, before, after)

	abortedKey := sc.txn(abortedTxnID)
	_, getErr := env.Session.Get(abortedKey, sdk.AllBins)
	env.Printf("  aborted transaction record %s absent: %t", abortedTxnID, getErr != nil)
	return nil
}

// sets are the three datasets a posting touches, in one namespace.
type sets struct {
	customers *sdk.DataSet
	accounts  *sdk.DataSet
	txns      *sdk.DataSet
}

// newSets mints and truncates the three datasets, so repeated runs start clean.
func newSets(env *exrun.Env, strongConsistency bool) (*sets, error) {
	mint := env.DataSet
	if strongConsistency {
		mint = env.SCDataSet
	}
	customers, err := mint(customersSet)
	if err != nil {
		return nil, err
	}
	accounts, err := mint(accountsSet)
	if err != nil {
		return nil, err
	}
	txns, err := mint(txnsSet)
	if err != nil {
		return nil, err
	}
	return &sets{customers: customers, accounts: accounts, txns: txns}, nil
}

func (s *sets) customer() *as.Key { return s.customers.Key(customerID) }
func (s *sets) account() *as.Key  { return s.accounts.Key(pan) }

func (s *sets) txn(id string) *as.Key { return s.txns.Key(id) }

// seed creates the customer and the account the posting will update.
func (s *sets) seed(session *sdk.Session) error {
	customerKey := s.customer()
	accountKey := s.account()

	stream, err := session.Insert(customerKey).
		SetTo("customerId", customerID).
		SetTo("firstName", "Jane").
		SetTo("lastName", "Morrison").
		SetTo("email", "jane.morrison@example.com").
		SetTo("phone", "+1-555-867-5309").
		SetTo("totalSpend", 0).
		SetTo("statusLevel", "BRONZE").
		Insert(accountKey).
		SetTo("pan", pan).
		SetTo("customerId", customerID).
		SetTo("expiryDate", "03/28").
		SetTo("balanceCents", 0).
		SetTo("creditLimit", 500_000).
		SetTo("status", "ACTIVE").
		Execute()
	if err != nil {
		return err
	}
	return raiseAny(stream)
}

// post logs the transaction, moves the balance, and updates the customer's total
// and tier -- one chained multi-record write.
//
// It reports how many records the write answered for, and turns any failed row
// into an error, so a transaction wrapping it aborts instead of committing.
func (s *sets) post(session *sdk.Session, id string) (int, error) {
	txnKey := s.txn(id)
	accountKey := s.account()
	customerKey := s.customer()

	stream, err := session.Insert(txnKey).
		SetTo("id", id).
		SetTo("desc", "Car repairs").
		SetTo("amountInCents", amountCents).
		SetTo("date", time.Now().UnixMilli()).
		Update([]*as.Key{accountKey}).
		Add("balanceCents", amountCents).
		Update([]*as.Key{customerKey}).
		Add("totalSpend", amountCents).
		Bin("statusLevel").WriteFrom(statusLevelExpression(), as.ExpWriteFlagDefault).
		Execute()
	if err != nil {
		return 0, err
	}
	rows, err := stream.Collect()
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if _, err := row.OrRaise(); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// report prints the three records after the posting.
func (s *sets) report(env *exrun.Env) error {
	customerKey := s.customer()
	customer, err := env.Session.Get(customerKey, sdk.AllBins)
	if err != nil {
		return err
	}
	env.Printf("  customer %s: totalSpend=%v statusLevel=%v", customerID,
		customer.Bins["totalSpend"], customer.Bins["statusLevel"])

	accountKey := s.account()
	account, err := env.Session.Get(accountKey, sdk.AllBins)
	if err != nil {
		return err
	}
	env.Printf("  account  %s: balanceCents=%v", pan, account.Bins["balanceCents"])

	txnKey := s.txn(txnID)
	txn, err := env.Session.Get(txnKey, sdk.AllBins)
	if err != nil {
		return err
	}
	env.Printf("  txn      %s: desc=%v amountInCents=%v", txnID,
		txn.Bins["desc"], txn.Bins["amountInCents"])
	return nil
}

// statusLevelExpression recomputes the status tier server-side from totalSpend.
//
// The Java example writes this as AEL text
// (`when ($.totalSpend > 100000 => 'PLATINUM', ...)`); the SDK takes AEL text in
// filter position only, so a bin write uses the equivalent typed expression.
// Because the operations in one write apply in order, the condition already sees
// the incremented total.
func statusLevelExpression() *as.Expression {
	return as.ExpCond(
		as.ExpGreater(as.ExpIntBin("totalSpend"), as.ExpIntVal(100_000)),
		as.ExpStringVal("PLATINUM"),
		as.ExpGreater(as.ExpIntBin("totalSpend"), as.ExpIntVal(10_000)),
		as.ExpStringVal("GOLD"),
		as.ExpGreater(as.ExpIntBin("totalSpend"), as.ExpIntVal(100)),
		as.ExpStringVal("SILVER"),
		as.ExpStringVal("BRONZE"),
	)
}

// raiseAny drains a stream and turns the first failed row into an error.
func raiseAny(stream *sdk.RecordStream) error {
	rows, err := stream.Collect()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := row.OrRaise(); err != nil {
			return err
		}
	}
	return nil
}

// readBin reads one bin, reporting nil when the record does not carry it.
func readBin(session *sdk.Session, key *as.Key, bin string) (any, error) {
	rec, err := session.Get(key, sdk.AllBins)
	if err != nil {
		return nil, err
	}
	return rec.Bins[bin], nil
}

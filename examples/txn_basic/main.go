/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Basic multi-record transaction (MRT) example with commit-status handling.
//
// Requires Aerospike server 8.0+ and a strong-consistency namespace.
//
// Run:
//
//	go run main.go -h 127.0.0.1 -p 3000 -n <ns> -s <set>
package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
	shared "github.com/aerospike/aerospike-client-go/v8/examples/shared"
)

func main() {
	txn := as.NewTxn()
	log.Printf("Initialize transaction: %d", txn.Id())

	if err := runCommands(txn); err != nil {
		log.Printf("Transaction commands failed: %v", err)
		abortTxn(txn)
		return
	}

	log.Printf("Commit transaction: %d", txn.Id())
	handleCommit(txn)
}

// runCommands performs reads and writes in a single transaction.
func runCommands(txn *as.Txn) error {
	wp := as.NewWritePolicy(0, 0)
	wp.Txn = txn

	rp := as.NewPolicy()
	rp.Txn = txn

	key, err := as.NewKey(*shared.Namespace, *shared.Set, "txn-basic-1")
	if err != nil {
		return err
	}

	log.Println("Write record")
	if err := shared.Client.PutBins(wp, key, as.NewBin("a", 1234)); err != nil {
		return err
	}

	log.Println("Read record")
	if _, err := shared.Client.Get(rp, key); err != nil {
		return err
	}

	log.Println("Update record in transaction")
	if err := shared.Client.PutBins(wp, key, as.NewBin("a", 5678)); err != nil {
		return err
	}

	return nil
}

// handleCommit branches on CommitStatus and Txn.State().
// Do not call Abort() blindly after every commit error.
func handleCommit(txn *as.Txn) {
	status, err := shared.Client.Commit(txn)

	switch status {
	case as.CommitStatusOK:
		log.Println("Commit succeeded")

	case as.CommitStatusAlreadyCommitted:
		log.Println("Transaction already committed on server (e.g. MRT_COMMITTED on retry)")

	case as.CommitStatusAlreadyAborted:
		log.Println("Transaction already aborted — nothing to do")

	case as.CommitStatusUnverified:
		// Verify failed before mark, client set ABORTED and attempted rollback.
		log.Printf("Verify failed: %v", err)
		if txn.State() == as.TxnStateAborted {
			if txnErr, ok := err.(*as.TxnError); ok && txnErr.CommitError == as.CommitErrorVerifyFailAbortAbandoned {
				log.Println("Rollback abandoned — retry Abort()")
				abortTxn(txn)
			} else {
				log.Println("Transaction aborted after verify failure — do not Abort() again")
			}
		}

	case as.CommitStatusMarkRollForwardAbandoned:
		// AS_COMMIT_MARK_ROLL_FORWARD_ABANDONED — mark phase failed before roll-forward.
		log.Printf("Mark roll-forward abandoned: %v", err)
		switch txn.State() {
		case as.TxnStateVerified:
			// Clean failure (e.g. MRT_EXPIRED) — abort to release locks.
			log.Println("Clean mark failure — calling Abort()")
			abortTxn(txn)
		case as.TxnStateCommitFailed:
			// In-doubt failure — retry Commit(); do not Abort().
			log.Println("In-doubt mark failure — retry Commit(); do not Abort()")
			// retryCommit(txn) in production
		case as.TxnStateAborted:
			log.Println("Server already aborted (MRT_ABORTED) — do not Abort()")
		default:
			log.Printf("Unexpected txn state after mark failure: %v", txn.State())
		}

	case as.CommitStatusRollForwardAbandoned:
		// AS_COMMIT_ROLL_FORWARD_ABANDONED — mark succeeded and client roll failed.
		log.Printf("Roll-forward abandoned: %v", err)
		log.Println("Mark succeeded — state COMMITTED; do not Abort()")
		// Optional: retry Commit() to finish close or return AlreadyCommitted.

	case as.CommitStatusCloseAbandoned:
		// AS_COMMIT_CLOSE_ABANDONED — roll succeeded and monitor delete failed.
		log.Printf("Monitor close abandoned: %v", err)
		log.Println("Roll succeeded — state COMMITTED; do not Abort()")
		// Optional: retry Commit() to delete the monitor.

	default:
		log.Printf("Commit finished: status=%v err=%v state=%v", status, err, txn.State())
	}
}

func abortTxn(txn *as.Txn) {
	status, err := shared.Client.Abort(txn)
	if err != nil {
		log.Printf("Abort failed: status=%v err=%v", status, err)
		return
	}
	log.Printf("Abort succeeded: %v", status)
}

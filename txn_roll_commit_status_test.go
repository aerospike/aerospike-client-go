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

package aerospike

// A MRT_COMMITTED answer on the mark-roll-forward is only a reporting
// difference: the roll forward and the monitor close still run, and a failure
// in either of those later steps outranks it. These specs drive Commit with no
// monitor and no writes, so no command reaches the network.

import (
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("TxnRoll.Commit status for an already committed mark", func() {

	newRoll := func(alreadyCommitted bool) (*TxnRoll, *Txn) {
		txn := NewTxn()
		gm.Expect(txn.SetNamespace("test")).ToNot(gm.HaveOccurred())
		gm.Expect(txn.MonitorExists()).To(gm.BeFalse())

		txr := NewTxnRoll(&Client{}, txn)
		txr.alreadyCommitted = alreadyCommitted
		return txr, txn
	}

	gg.It("reports CommitStatusAlreadyCommitted when the mark saw MRT_COMMITTED", func() {
		txr, txn := newRoll(true)

		status, err := txr.Commit(&NewTxnRollPolicy().BatchPolicy)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(status).To(gm.Equal(CommitStatusAlreadyCommitted))
		gm.Expect(txn.State()).To(gm.Equal(TxnStateCommitted))
		gm.Expect(txn.GetInDoubt()).To(gm.BeFalse())
	})

	gg.It("reports CommitStatusOK when the mark returned a plain success", func() {
		txr, txn := newRoll(false)

		status, err := txr.Commit(&NewTxnRollPolicy().BatchPolicy)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(status).To(gm.Equal(CommitStatusOK))
		gm.Expect(txn.State()).To(gm.Equal(TxnStateCommitted))
		gm.Expect(txn.GetInDoubt()).To(gm.BeFalse())
	})
})

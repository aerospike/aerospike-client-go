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

import (
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// Pins the invariant that broke GetHeader with SendKey=true and GetHeader
// inside a transaction: the fieldCount declared in the message header must
// account for every field the command writes. The bug was sizing with
// estimateRawKeySize (namespace/set/digest only) while writing with
// writeKeyWithPolicy, which also emits the transaction fields -- the server
// rejects the undercounted message with PARAMETER_ERROR.
//
// Header reads deliberately never carry the user key: SendKey is stripped on a
// copy of the policy, so the sized and written field sets agree at three key
// fields (namespace, set, digest) regardless of SendKey.
//
// The specs need no server: they walk the declared fields in the written
// buffer and require them to consume exactly the whole message body.

var _ = gg.Describe("setReadHeader declares every field it writes", func() {

	key, _ := NewKey("test", "wire", 1)

	sendKeyPolicy := func() *BasePolicy {
		p := NewPolicy()
		p.SendKey = true
		return p
	}

	txnPolicy := func(txn *Txn, sendKey bool) *BasePolicy {
		p := NewPolicy()
		p.Txn = txn
		p.SendKey = sendKey
		return p
	}

	// assertWireConsistent checks the three ways a miscounted message shows
	// up: the declared fields must consume exactly the message body, the
	// operation count must be zero for a header read, and the proto size must
	// match what was written.
	assertWireConsistent := func(cmd *baseCommand, wantFields int) {
		// Proto header: 6-byte size in the low bits of the first 8 bytes.
		protoSize := int(Buffer.BytesToInt64(cmd.dataBuffer, 0) & 0xFFFFFFFFFFFF)
		gm.ExpectWithOffset(1, protoSize).To(gm.Equal(cmd.dataOffset-8), "proto size")

		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 26))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 28))

		gm.ExpectWithOffset(1, opCount).To(gm.BeZero(), "a header read must declare no operations")
		gm.ExpectWithOffset(1, fieldCount).To(gm.Equal(wantFields), "declared field count")

		// Walk the declared fields; they must land exactly on the end of the
		// message. Landing short means fields were written but not counted
		// (the PARAMETER_ERROR bug); landing past the end means the opposite.
		off := int(_MSG_TOTAL_HEADER_SIZE)
		for i := 0; i < fieldCount; i++ {
			gm.ExpectWithOffset(1, off+4).To(gm.BeNumerically("<=", cmd.dataOffset),
				"field %d of %d starts past the end of the message", i+1, fieldCount)
			flen := int(Buffer.BytesToUint32(cmd.dataBuffer, off))
			off += 4 + flen
		}
		gm.ExpectWithOffset(1, off).To(gm.Equal(cmd.dataOffset),
			"the %d declared fields must consume exactly the message body: fieldCount does not match the fields written", fieldCount)
	}

	encode := func(policy *BasePolicy) *baseCommand {
		cmd := &baseCommand{}
		gm.Expect(cmd.setReadHeader(policy, key)).ToNot(gm.HaveOccurred())
		return cmd
	}

	gg.It("with a plain policy", func() {
		assertWireConsistent(encode(NewPolicy()), 3) // ns, set, digest
	})

	gg.It("with SendKey, which header reads deliberately strip", func() {
		assertWireConsistent(encode(sendKeyPolicy()), 3)
	})

	gg.It("inside a transaction, which adds the MRT_ID field", func() {
		assertWireConsistent(encode(txnPolicy(NewTxn(), false)), 4)
	})

	gg.It("inside a transaction with a recorded read version, which adds MRT_ID and RECORD_VERSION", func() {
		txn := NewTxn()
		ver := uint64(7)
		txn.OnRead(key, &ver)
		assertWireConsistent(encode(txnPolicy(txn, false)), 5)
	})

	gg.It("inside a transaction with SendKey, which adds only the MRT_ID field", func() {
		assertWireConsistent(encode(txnPolicy(NewTxn(), true)), 4)
	})
})

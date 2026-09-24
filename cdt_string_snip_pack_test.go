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
)

// CLIENT-5353: the server's snip argument list is positional — start at slot 0,
// end at slot 1, flags at slot 2 — so a payload of [SNIP, start, flags] lands
// the flags in the `end` slot and silently snips the empty range [start, 0).
// The one-argument form must therefore pack exactly [SNIP, start]. These
// in-package tests pin the payload element count on both the operation and the
// expression path.
var _ = gg.Describe("String snip payload shape (CLIENT-5353)", func() {

	unpackOp := func(op *Operation) []any {
		raw, ok := op.binValue.(*RawBlobValue)
		gm.Expect(ok).To(gm.BeTrue(), "expected a pre-packed RawBlobValue payload")
		list, err := newUnpacker(raw.Data, 0, len(raw.Data)).UnpackList()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return list
	}

	gg.It("StrSnipFromOp packs [SNIP, start] with no trailing flags element", func() {
		gm.Expect(unpackOp(StrSnipFromOp(DefaultStringPolicy, "sbin", 5))).To(
			gm.Equal([]any{int(_STR_OP_SNIP), 5}))
	})

	gg.It("StrSnipFromOp omits the flags element for a non-default policy too", func() {
		gm.Expect(unpackOp(StrSnipFromOp(NewStringPolicy(StringWriteNoFail), "sbin", 5))).To(
			gm.Equal([]any{int(_STR_OP_SNIP), 5}))
	})

	gg.It("StrSnipOp still packs [SNIP, start, end, flags]", func() {
		gm.Expect(unpackOp(StrSnipOp(DefaultStringPolicy, "sbin", 5, 11))).To(
			gm.Equal([]any{int(_STR_OP_SNIP), 5, 11, 0}))
	})

	// packCommand packs one msgpack element per Expression argument, so the
	// argument count is the packed payload count.
	gg.It("ExpStringSnipFrom carries two arguments, not three", func() {
		exp := ExpStringSnipFrom(DefaultStringPolicy, ExpStringBin("sbin"), ExpIntVal(5))
		gm.Expect(exp.arguments).To(gm.HaveLen(2))
		gm.Expect(exp.arguments[0]).To(gm.Equal(IntegerValue(_STR_OP_SNIP)))
	})

	gg.It("ExpStringSnip still carries four arguments", func() {
		exp := ExpStringSnip(DefaultStringPolicy, ExpStringBin("sbin"), ExpIntVal(5), ExpIntVal(11))
		gm.Expect(exp.arguments).To(gm.HaveLen(4))
	})
})

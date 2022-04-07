// Copyright 2017-2022 Aerospike, Inc.
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

package aerospike_test

import (
	as "github.com/aerospike/aerospike-client-go/v6"
	ast "github.com/aerospike/aerospike-client-go/v6/types"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Expression Operations", func() {

	var ns = *namespace
	var set = randString(50)

	binA := "A"
	binB := "B"
	binC := "C"
	binD := "D"
	binH := "H"
	expVar := "EV"

	keyA, _ := as.NewKey(ns, set, "A")
	keyB, _ := as.NewKey(ns, set, []byte{'B'})

	gg.BeforeEach(func() {
		_, err := client.Delete(nil, keyA)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = client.Delete(nil, keyB)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		err = client.PutBins(nil, keyA, as.NewBin(binA, 1), as.NewBin(binD, 2))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		err = client.PutBins(nil, keyB, as.NewBin(binB, 2), as.NewBin(binD, 2))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.It("Expression ops on lists should work", func() {
		list := []as.Value{as.StringValue("a"), as.StringValue("b"), as.StringValue("c"), as.StringValue("d")}
		exp := as.ExpListVal(list...)
		rec, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp("var", exp, as.ExpReadFlagDefault),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"C": []interface{}{nil, []interface{}{"a", "b", "c", "d"}}, "var": []interface{}{"a", "b", "c", "d"}}))
	})

	gg.It("Read Eval error should work", func() {
		exp := as.ExpNumAdd(as.ExpIntBin(binA), as.ExpIntVal(4))

		r, err := client.Operate(nil, keyA, as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		_, err = client.Operate(nil, keyB, as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyB, as.ExpReadOp(expVar, exp, as.ExpReadFlagEvalNoFail))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))
	})

	gg.It("Read On Write Eval error should work", func() {
		rexp := as.ExpIntBin(binD)
		wexp := as.ExpIntBin(binA)

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binD, wexp, as.ExpWriteFlagDefault),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		_, err = client.Operate(nil, keyB,
			as.ExpWriteOp(binD, wexp, as.ExpWriteFlagDefault),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyB,
			as.ExpWriteOp(binD, wexp, as.ExpWriteFlagEvalNoFail),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagEvalNoFail),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))
	})

	gg.It("Write Eval error should work", func() {
		wexp := as.ExpNumAdd(as.ExpIntBin(binA), as.ExpIntVal(4))
		rexp := as.ExpIntBin(binC)

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagDefault),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		_, err = client.Operate(nil, keyB,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagDefault),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyB,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagEvalNoFail),
			as.ExpReadOp(expVar, rexp, as.ExpReadFlagEvalNoFail),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))
	})

	gg.It("Write Policy error should work", func() {
		wexp := as.ExpNumAdd(as.ExpIntBin(binA), as.ExpIntVal(4))

		_, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagUpdateOnly),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.BIN_NOT_FOUND)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagUpdateOnly|as.ExpWriteFlagPolicyNoFail),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		_, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagCreateOnly),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		_, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagCreateOnly),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.BIN_EXISTS_ERROR)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagUpdateOnly|as.ExpWriteFlagPolicyNoFail),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		dexp := as.ExpNilValue()

		_, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, dexp, as.ExpWriteFlagDefault),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, dexp, as.ExpWriteFlagPolicyNoFail),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		r, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, dexp, as.ExpWriteFlagAllowDelete),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		r, err = client.Operate(nil, keyA,
			as.ExpWriteOp(binC, wexp, as.ExpWriteFlagCreateOnly),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))
	})

	gg.It("Return Unknown should work", func() {
		exp := as.ExpCond(
			as.ExpEq(as.ExpIntBin(binC), as.ExpIntVal(5)), as.ExpUnknown(),
			as.ExpBinExists(binA), as.ExpIntVal(5),
			as.ExpUnknown(),
		)

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
		)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())

		r, err = client.Operate(nil, keyB,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagEvalNoFail),
			as.GetBinOp(binC),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(r.Bins)).To(gm.BeNumerically(">", 0))

		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{binC: []interface{}{nil, nil}}))
	})

	gg.It("Return Nil should work", func() {
		exp := as.ExpNilValue()

		r, err := client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
			as.GetBinOp(binC),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: nil, binC: nil}))
	})

	gg.It("Return Int should work", func() {
		exp := as.ExpNumAdd(as.ExpIntBin(binA), as.ExpIntVal(4))

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: 5, binC: []interface{}{nil, 5}}))

		r, err = client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: 5}))
	})

	gg.It("Return Float should work", func() {
		exp := as.ExpNumAdd(as.ExpToFloat(as.ExpIntBin(binA)), as.ExpFloatVal(4.1))

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: 5.1, binC: []interface{}{nil, 5.1}}))

		r, err = client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: 5.1}))
	})

	gg.It("Return String should work", func() {
		str := "xxx"
		exp := as.ExpStringVal(str)

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: str, binC: []interface{}{nil, str}}))

		r, err = client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: str}))
	})

	gg.It("Return BLOB should work", func() {
		blob := []byte{0x78, 0x78, 0x78}
		exp := as.ExpBlobVal(blob)

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: blob, binC: []interface{}{nil, blob}}))

		r, err = client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: blob}))
	})

	gg.It("Return Boolean should work", func() {
		exp := as.ExpEq(as.ExpIntBin(binA), as.ExpIntVal(1))

		r, err := client.Operate(nil, keyA,
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{expVar: true, binC: []interface{}{nil, true}}))
	})

	gg.It("Return HLL should work", func() {
		exp := as.ExpHLLInit(as.DefaultHLLPolicy(), as.ExpIntVal(4), as.ExpNilValue())

		r, err := client.Operate(nil, keyA,
			as.HLLInitOp(as.DefaultHLLPolicy(), binH, 4, -1),
			as.ExpWriteOp(binC, exp, as.ExpWriteFlagDefault),
			as.GetBinOp(binH),
			as.GetBinOp(binC),
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{
			binH: []interface{}{
				nil,
				[]uint8{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			binC: []interface{}{
				nil,
				[]uint8{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			expVar: []uint8{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}))

		r, err = client.Operate(nil, keyA,
			as.ExpReadOp(expVar, exp, as.ExpReadFlagDefault),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins).To(gm.Equal(as.BinMap{
			expVar: []uint8{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}))
	})

}) // Describe

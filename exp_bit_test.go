// Copyright 2017-2021 Aerospike, Inc.
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
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/internal/atomic"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Expression Filters - Bitwise", func() {

	const keyCount = 100

	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	var qpolicy = as.NewQueryPolicy()

	insertRecs := atomic.NewBool(true)

	gg.BeforeEach(func() {
		if !insertRecs.Get() {
			return
		}

		for ii := 0; ii < keyCount; ii++ {
			key, _ := as.NewKey(ns, set, ii)
			bin := as.BinMap{"bin": []byte{0b00000001, 0b01000010}}
			client.Delete(wpolicy, key)
			err := client.Put(nil, key, bin)
			gm.Expect(err).NotTo(gm.HaveOccurred())

			data := []as.Value{as.NewValue("asd"), as.NewValue(ii)}
			data2 := []as.Value{as.NewValue("asd"), as.NewValue(ii), as.NewValue(ii + 1)}

			ops := []*as.Operation{
				as.HLLAddOp(
					as.DefaultHLLPolicy(),
					"hllbin",
					data,
					8,
					0,
				),
				as.HLLAddOp(
					as.DefaultHLLPolicy(),
					"hllbin2",
					data2,
					8,
					0,
				),
			}

			_, err = client.Operate(nil, key, ops...)
			gm.Expect(err).NotTo(gm.HaveOccurred())
		}

		insertRecs.Set(false)
	})

	runQuery := func(filter *as.FilterExpression, set_name string) *as.Recordset {
		qpolicy.FilterExpression = filter
		stmt := as.NewStatement(ns, set_name)
		rs, err := client.Query(qpolicy, stmt)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		return rs
	}

	countResults := func(rs *as.Recordset) int {
		count := 0

		for res := range rs.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			count += 1
		}

		return count
	}

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(16),
					as.ExpBlobBin("bin"),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(16),
					as.ExpBitResize(
						as.DefaultBitPolicy(),
						as.ExpIntVal(4),
						as.BitResizeFlagsDefault,
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(16),
					as.ExpBitInsert(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpBlobVal([]byte{0b11111111}),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(9),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitRemove(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(1),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitSet(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpBlobVal([]byte{0b10101010}),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(4),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitOr(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpBlobVal([]byte{0b10101010}),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(5),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitXor(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpBlobVal([]byte{0b10101011}),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(4),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitAnd(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpBlobVal([]byte{0b10101011}),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitNot(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(7),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitLShift(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(16),
						as.ExpIntVal(9),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitRShift(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpIntVal(3),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitAdd(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpIntVal(128),
						false,
						as.BitOverflowActionWrap,
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitSubtract(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpIntVal(1),
						false,
						as.BitOverflowActionWrap,
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitCount(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBitSetInt(
						as.DefaultBitPolicy(),
						as.ExpIntVal(0),
						as.ExpIntVal(8),
						as.ExpIntVal(255),
						as.ExpBlobBin("bin"),
					),
				),
				as.ExpIntVal(8),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitGet(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					as.ExpBlobBin("bin"),
				),
				as.ExpBlobVal([]byte{0b00000001}),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitLScan(
					as.ExpIntVal(8),
					as.ExpIntVal(8),
					as.ExpBoolVal(true),
					as.ExpBlobBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitRScan(
					as.ExpIntVal(8),
					as.ExpIntVal(8),
					as.ExpBoolVal(true),
					as.ExpBlobBin("bin"),
				),
				as.ExpIntVal(6),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It(" should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpBitGetInt(
					as.ExpIntVal(0),
					as.ExpIntVal(8),
					false,
					as.ExpBlobBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})
})

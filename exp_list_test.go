// Copyright 2017-2020 Aerospike, Inc.
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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("Expression Filters - Lists", func() {

	const keyCount = 100

	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	var qpolicy = as.NewQueryPolicy()

	insertRecs := atomic.NewAtomicBool(true)

	BeforeEach(func() {
		if !insertRecs.Get() {
			return
		}

		wpolicy = as.NewWritePolicy(0, 24*60*60)
		for ii := 0; ii < keyCount; ii++ {
			key, _ := as.NewKey(ns, set, ii)
			ibin := as.NewBin("bin", []int{1, 2, 3, ii})
			client.Delete(wpolicy, key)
			client.PutBins(wpolicy, key, ibin)
		}

		insertRecs.Set(false)
	})

	runQuery := func(filter *as.FilterExpression, set_name string) *as.Recordset {
		qpolicy.FilterExpression = filter
		stmt := as.NewStatement(ns, set_name)
		rs, err := client.Query(qpolicy, stmt)
		Expect(err).NotTo(HaveOccurred())

		return rs
	}

	countResults := func(rs *as.Recordset) int {
		count := 0

		for res := range rs.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			count += 1
		}

		return count
	}

	It("ExpListAppend should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListAppend(
						as.DefaultListPolicy(),
						as.ExpIntVal(999),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(5),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListAppendItems should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListAppendItems(
						as.DefaultListPolicy(),
						as.ExpListVal(as.NewValue(555), as.NewValue("asd")),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(6),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListClear should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListClear(as.ExpListBin("bin")),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ListReturnTypeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByValue(
					as.ListReturnTypeCount,
					as.ExpIntVal(234),
					as.ExpListInsert(
						as.DefaultListPolicy(),
						as.ExpIntVal(1),
						as.ExpIntVal(234),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ListReturnTypeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByValueList(
					as.ListReturnTypeCount,
					as.ExpListVal(as.NewValue(51), as.NewValue(52)),
					as.ExpListBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(2))
	})

	It("ExpListInsertItems should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListInsertItems(
						as.DefaultListPolicy(),
						as.ExpIntVal(4),
						as.ExpListVal(as.NewValue(222), as.NewValue(223)),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(6),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByIndex(
					as.ListReturnTypeValue,
					as.ExpTypeINT,
					as.ExpIntVal(3),
					as.ExpListIncrement(
						as.DefaultListPolicy(),
						as.ExpIntVal(3),
						as.ExpIntVal(100),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(102),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByIndex(
					as.ListReturnTypeValue,
					as.ExpTypeINT,
					as.ExpIntVal(3),
					as.ExpListSet(
						as.DefaultListPolicy(),
						as.ExpIntVal(3),
						as.ExpIntVal(100),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(100),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByIndexRangeCount(
					as.ListReturnTypeValue,
					as.ExpIntVal(2),
					as.ExpIntVal(2),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(3), as.NewValue(15)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByIndexRange(
					as.ListReturnTypeValue,
					as.ExpIntVal(2),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(3), as.NewValue(15)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByRank(
					as.ListReturnTypeValue,
					as.ExpTypeINT,
					as.ExpIntVal(3),
					as.ExpListBin("bin"),
				),
				as.ExpIntVal(25),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByRankRange(
					as.ListReturnTypeValue,
					as.ExpIntVal(2),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(3), as.NewValue(25)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByRankRangeCount(
					as.ListReturnTypeValue,
					as.ExpIntVal(2),
					as.ExpIntVal(2),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(3), as.NewValue(3)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(1))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByValueRange(
					as.ListReturnTypeValue,
					as.ExpIntVal(1),
					as.ExpIntVal(3),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(1), as.NewValue(2)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(98))
	})

	It("ListReturnTypeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByValueRelativeRankRange(
					as.ListReturnTypeCount,
					as.ExpIntVal(2),
					as.ExpIntVal(0),
					as.ExpListBin("bin"),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(98))
	})

	It("ListReturnTypeValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListGetByValueRelativeRankRangeCount(
					as.ListReturnTypeValue,
					as.ExpIntVal(2),
					as.ExpIntVal(1),
					as.ExpIntVal(1),
					as.ExpListBin("bin"),
				),
				as.ExpListVal(as.NewValue(3)),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(99))
	})

	It("ExpListRemoveByValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValue(
						as.ExpIntVal(3),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(99))
	})

	It("ExpListRemoveByValueList should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueList(
						as.ExpListVal(as.NewValue(1), as.NewValue(2)),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(98))
	})

	It("ExpListRemoveByValueRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRange(
						as.ExpIntVal(1),
						as.ExpIntVal(3),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(98))
	})

	It("ExpListRemoveByValueRelativeRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRelativeRankRange(
						as.ExpIntVal(3),
						as.ExpIntVal(1),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(97))
	})

	It("ExpListRemoveByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRelativeRankRangeCount(
						as.ExpIntVal(2),
						as.ExpIntVal(1),
						as.ExpIntVal(1),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByIndex should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndex(
						as.ExpIntVal(0),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRange(
						as.ExpIntVal(2),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRangeCount(
						as.ExpIntVal(2),
						as.ExpIntVal(1),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRangeCount(
						as.ExpIntVal(2),
						as.ExpIntVal(1),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByRank should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByRank(
						as.ExpIntVal(2),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByRankRange(
						as.ExpIntVal(2),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})

	It("ExpListRemoveByRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByRankRangeCount(
						as.ExpIntVal(2),
						as.ExpIntVal(1),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		Expect(count).To(Equal(100))
	})
})

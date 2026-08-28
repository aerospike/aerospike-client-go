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
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Expression Filters - Lists", gg.Ordered, func() {

	const keyCount = 100

	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	var qpolicy = as.NewQueryPolicy()

	gg.BeforeAll(func() {
		for ii := 0; ii < keyCount; ii++ {
			key, _ := as.NewKey(ns, set, ii)
			ibin := as.NewBin("bin", []int{1, 2, 3, ii})
			client.Delete(wpolicy, key)
			err := client.PutBins(wpolicy, key, ibin)
			gm.Expect(err).NotTo(gm.HaveOccurred())
		}
	})

	runQuery := func(filter *as.Expression, set_name string) *as.Recordset {
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

	gg.It("ExpListAppend should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListAppendItems should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListClear should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ListReturnTypeCount should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ListReturnTypeCount should work", func() {
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
		gm.Expect(count).To(gm.Equal(2))
	})

	gg.It("ExpListInsertItems should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ListReturnTypeCount should work", func() {
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
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ListReturnTypeValue should work", func() {
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
		gm.Expect(count).To(gm.Equal(99))
	})

	gg.It("ExpListRemoveByValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValue(
						as.ListReturnTypeNone,
						as.ExpIntVal(3),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(99))
	})

	gg.It("ExpListRemoveByValueList should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueList(
						as.ListReturnTypeNone,
						as.ExpListVal(as.NewValue(1), as.NewValue(2)),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ExpListRemoveByValueRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRange(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ExpListRemoveByValueRelativeRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRelativeRankRange(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(97))
	})

	gg.It("ExpListRemoveByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByValueRelativeRankRangeCount(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByIndex should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRange(
						as.ListReturnTypeNone,
						as.ExpIntVal(2),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRangeCount(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByIndexRangeCount(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByRank should work", func() {
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
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByRankRange(
						as.ListReturnTypeNone,
						as.ExpIntVal(2),
						as.ExpListBin("bin"),
					),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpListRemoveByRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpListSize(
					as.ExpListRemoveByRankRangeCount(
						as.ListReturnTypeNone,
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
		gm.Expect(count).To(gm.Equal(100))
	})

	// string_list_join is a CDT list read op (code 28), the inverse of the
	// string `split` expression. It requires server 8.1.3+.
	gg.Context("ExpListJoin", func() {

		const variable = "v"

		var joinSet = randString(50)
		var key *as.Key

		gg.BeforeEach(func() {
			requiredVersion, err := version.Parse("8.1.3")
			if err != nil {
				gg.Fail("Failed to parse server required version")
			}
			nodeVersion := client.GetNodes()[0].GetServerVersion()
			if nodeVersion.IsSmaller(requiredVersion) {
				gg.Skip("string_list_join requires server version 8.1.3+.")
				return
			}

			key, err = as.NewKey(ns, joinSet, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			client.Delete(wpolicy, key)
			err = client.PutBins(wpolicy, key, as.NewBin("sbin", []any{"one", "two", "three"}))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		eval := func(e *as.Expression) any {
			rec, err := client.Operate(wpolicy, key, as.ExpReadOp(variable, e, as.ExpReadFlagDefault))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return rec.Bins[variable]
		}

		gg.It("joins with and without a separator", func() {
			gm.Expect(eval(as.ExpListJoinBySeparator(as.ExpStringVal("|"), as.ExpListBin("sbin")))).
				To(gm.Equal("one|two|three"))
			gm.Expect(eval(as.ExpListJoin(as.ExpListBin("sbin")))).To(gm.Equal("onetwothree"))
		})

		gg.It("joins a list nested in a map", func() {
			err := client.PutBins(wpolicy, key, as.NewBin("mbin", map[any]any{"k": []any{"a", "b"}}))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(eval(as.ExpListJoinBySeparator(as.ExpStringVal("-"), as.ExpMapBin("mbin"), as.CtxMapKey(as.StringValue("k"))))).
				To(gm.Equal("a-b"))
		})

		gg.It("is usable as a query filter", func() {
			filterSet := randString(50)
			for _, l := range [][]any{{"one", "two", "three"}, {"four", "five"}} {
				k, err := as.NewKey(ns, filterSet, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(client.PutBins(wpolicy, k, as.NewBin("sbin", l))).ToNot(gm.HaveOccurred())
			}

			rs := runQuery(
				as.ExpEq(
					as.ExpListJoinBySeparator(as.ExpStringVal(","), as.ExpListBin("sbin")),
					as.ExpStringVal("one,two,three"),
				),
				filterSet,
			)
			gm.Expect(countResults(rs)).To(gm.Equal(1))
		})

	})
})

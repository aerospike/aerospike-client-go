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
var _ = gg.Describe("Expression Filters - Maps", func() {

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

		wpolicy = as.NewWritePolicy(0, 24*60*60)
		for ii := 0; ii < keyCount; ii++ {
			key, _ := as.NewKey(ns, set, ii)
			ibin := as.BinMap{"bin": map[string]interface{}{"test": ii, "test2": "a"}}
			client.Delete(wpolicy, key)
			client.Put(wpolicy, key, ibin)
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

	gg.It("ExpMapGetByKey should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByKey(
					as.MapReturnType.VALUE,
					as.ExpTypeINT,
					as.ExpStringVal("test3"),
					as.ExpMapPut(
						as.DefaultMapPolicy(),
						as.ExpStringVal("test3"),
						as.ExpIntVal(999),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(999),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByKeyList should work", func() {
		amap := map[interface{}]interface{}{
			"test4": 333,
			"test5": 444,
		}
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByKeyList(
					as.MapReturnType.VALUE,
					as.ExpListVal(as.NewValue("test4"), as.NewValue("test5")),
					as.ExpMapPutItems(
						as.DefaultMapPolicy(),
						as.ExpMapVal(amap),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpListVal(as.NewValue(333), as.NewValue(444)),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValue(
					as.MapReturnType.COUNT,
					as.ExpIntVal(5),
					as.ExpMapIncrement(
						as.DefaultMapPolicy(),
						as.ExpStringVal("test"),
						as.ExpIntVal(1),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapClear should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapClear(as.ExpMapBin("bin")),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByValueList should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueList(
					as.MapReturnType.COUNT,
					as.ExpListVal(as.NewValue(1), as.NewValue("a")),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapGetByValueRelativeRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRelativeRankRange(
					as.MapReturnType.COUNT,
					as.ExpIntVal(1),
					as.ExpIntVal(0),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(99))
	})

	gg.It("ExpMapGetByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRelativeRankRangeCount(
					as.MapReturnType.COUNT,
					as.ExpIntVal(1),
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRelativeRankRangeCount(
					as.MapReturnType.COUNT,
					as.ExpIntVal(1),
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRelativeRankRangeCount(
					as.MapReturnType.COUNT,
					as.ExpIntVal(1),
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByValueRelativeRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRelativeRankRangeCount(
					as.MapReturnType.COUNT,
					as.ExpIntVal(1),
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByIndex should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByIndex(
					as.MapReturnType.VALUE,
					as.ExpTypeINT,
					as.ExpIntVal(0),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapGetByIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByIndexRange(
					as.MapReturnType.COUNT,
					as.ExpIntVal(0),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByIndexRangeCount(
					as.MapReturnType.VALUE,
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpListVal(as.NewValue(2)),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapGetByRank should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByRank(
					as.MapReturnType.VALUE,
					as.ExpTypeINT,
					as.ExpIntVal(0),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapGetByRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByRankRange(
					as.MapReturnType.VALUE,
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpListVal(as.NewValue("a")),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByRankRangeCount(
					as.MapReturnType.VALUE,
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpListVal(as.NewValue(15)),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapGetByValueRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByValueRange(
					as.MapReturnType.COUNT,
					as.ExpIntVal(0),
					as.ExpIntVal(18),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(18))
	})

	gg.It("ExpMapGetByKeyRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByKeyRange(
					as.MapReturnType.COUNT,
					nil,
					as.ExpStringVal("test25"),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByKeyRelativeIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByKeyRelativeIndexRange(
					as.MapReturnType.COUNT,
					as.ExpStringVal("test"),
					as.ExpIntVal(0),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapGetByKeyRelativeIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapGetByKeyRelativeIndexRangeCount(
					as.MapReturnType.COUNT,
					as.ExpStringVal("test"),
					as.ExpIntVal(0),
					as.ExpIntVal(1),
					as.ExpMapBin("bin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByKey should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByKey(
						as.ExpStringVal("test"),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByKeyList should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByKeyList(
						as.ExpListVal(as.NewValue("test"), as.NewValue("test2")),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByKeyRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByKeyRange(
						as.ExpStringVal("test"),
						nil,
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByKeyRelativeIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByKeyRelativeIndexRange(
						as.ExpStringVal("test"),
						as.ExpIntVal(0),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByKeyRelativeIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByKeyRelativeIndexRangeCount(
						as.ExpStringVal("test"),
						as.ExpIntVal(0),
						as.ExpIntVal(1),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByValue should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByValue(
						as.ExpIntVal(5),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapRemoveByValueList should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByValueList(
						as.ExpListVal(as.NewValue("a"), as.NewValue(15)),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpMapRemoveByValueRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByValueRange(
						as.ExpIntVal(5),
						as.ExpIntVal(15),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(10))
	})

	gg.It("ExpMapRemoveByIndex should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByIndex(
						as.ExpIntVal(0),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByIndexRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByIndexRange(
						as.ExpIntVal(0),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByIndexRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByIndexRangeCount(
						as.ExpIntVal(0),
						as.ExpIntVal(1),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByRank should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByRank(
						as.ExpIntVal(0),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByRankRange should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByRankRange(
						as.ExpIntVal(0),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(0),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpMapRemoveByRankRangeCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpMapSize(
					as.ExpMapRemoveByRankRangeCount(
						as.ExpIntVal(0),
						as.ExpIntVal(1),
						as.ExpMapBin("bin"),
					),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})
})

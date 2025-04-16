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
var _ = gg.Describe("Expression Filters - HLL", func() {

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
			bin := as.BinMap{"bin": ii, "lbin": []interface{}{ii, "a"}}
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

	gg.It("ExpHLLGetCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpHLLGetCount(
					as.ExpHLLAddWithIndexAndMinHash(
						as.DefaultHLLPolicy(),
						as.ExpListVal(as.NewValue(48715414)),
						as.ExpIntVal(8),
						as.ExpIntVal(0),
						as.ExpHLLBin("hllbin"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(99))
	})

	gg.It("ExpHLLMayContain should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpHLLMayContain(
					as.ExpListVal(as.NewValue(55)),
					as.ExpHLLBin("hllbin"),
				),
				as.ExpIntVal(1),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("ExpListGetByIndex should work", func() {
		rs := runQuery(
			as.ExpLess(
				as.ExpListGetByIndex(
					as.ListReturnTypeValue,
					as.ExpTypeINT,
					as.ExpIntVal(0),
					as.ExpHLLDescribe(as.ExpHLLBin("hllbin")),
				),
				as.ExpIntVal(10),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(100))
	})

	gg.It("ExpHLLGetUnion should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpHLLGetCount(
					as.ExpHLLGetUnion(
						as.ExpHLLBin("hllbin"),
						as.ExpHLLBin("hllbin2"),
					),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ExpHLLGetUnionCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpHLLGetUnionCount(
					as.ExpHLLBin("hllbin"),
					as.ExpHLLBin("hllbin2"),
				),
				as.ExpIntVal(3),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(98))
	})

	gg.It("ExpHLLGetIntersectCount should work", func() {
		rs := runQuery(
			as.ExpEq(
				as.ExpHLLGetIntersectCount(
					as.ExpHLLBin("hllbin"),
					as.ExpHLLBin("hllbin2"),
				),
				as.ExpIntVal(2),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(99))
	})

	gg.It("ExpHLLGetSimilarity should work", func() {
		rs := runQuery(
			as.ExpGreater(
				as.ExpHLLGetSimilarity(
					as.ExpHLLBin("hllbin"),
					as.ExpHLLBin("hllbin2"),
				),
				as.ExpFloatVal(0.5),
			),
			set,
		)
		count := countResults(rs)
		gm.Expect(count).To(gm.Equal(99))
	})
})

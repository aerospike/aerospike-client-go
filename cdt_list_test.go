// Copyright 2013-2017 Aerospike, Inc.
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
	"math"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
	// . "github.com/aerospike/aerospike-client-go/types"
)

var _ = Describe("CDT List Test", func() {
	initTestVars()

	if !featureEnabled("cdt-list") {
		By("CDT List Tests will not run since feature is not supported by the server.")
		return
	}

	// connection data
	var ns = "test"
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)
	var cdtBinName string
	var list []interface{}

	BeforeEach(func() {
		key, err = as.NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		cdtBinName = randString(10)
	})

	It("should create a valid CDT List", func() {
		cdtList, err := client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
		Expect(err).ToNot(HaveOccurred())
		Expect(cdtList).To(BeNil())

		list := []interface{}{}
		for i := 1; i <= 100; i++ {
			list = append(list, i)

			sz, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, i))
			Expect(err).ToNot(HaveOccurred())
			Expect(sz.Bins[cdtBinName]).To(Equal(i))

			sz, err = client.Operate(wpolicy, key, as.ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(sz.Bins[cdtBinName]).To(Equal(i))
		}

		sz, err := client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, 100))
		Expect(err).ToNot(HaveOccurred())
		Expect(sz.Bins[cdtBinName]).To(Equal(list))

		sz, err = client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, list...))
		Expect(err).ToNot(HaveOccurred())
		Expect(sz.Bins[cdtBinName]).To(Equal(100 * 2))
	})

	Describe("CDT List Operations", func() {

		const listSize = 10

		// make a fresh list before each operation
		BeforeEach(func() {
			list = []interface{}{}

			for i := 1; i <= listSize; i++ {
				list = append(list, i)

				sz, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, i))
				Expect(err).ToNot(HaveOccurred())
				Expect(sz.Bins[cdtBinName]).To(Equal(i))
			}
		})

		It("should Get the last element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize))
		})

		It("should Get the last element again", func() {
			ops := []*as.Operation{as.ListGetOp(cdtBinName, -1)}
			cdtListRes, err := client.Operate(wpolicy, key, ops...)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize))

			cdtListRes, err = client.Operate(wpolicy, key, ops...)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize))
		})

		It("should Get the last 3 element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, -3, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{listSize - 2, listSize - 1, listSize - 0}))
		})

		It("should Get the from element #7 till the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{listSize - 2, listSize - 1, listSize - 0}))
		})

		It("should Get by value", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetByValueOp(cdtBinName, 7, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{7}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByValueListOp(cdtBinName, []interface{}{7, 9}, as.ListReturnTypeIndex))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{6, 8}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByValueRangeOp(cdtBinName, 5, 9, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{5, 6, 7, 8}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByValueRangeOp(cdtBinName, 5, 9, as.ListReturnTypeIndex))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{4, 5, 6, 7}))
		})

		It("should Get by index", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetByIndexOp(cdtBinName, 7, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(8))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByIndexRangeOp(cdtBinName, 7, as.ListReturnTypeIndex))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{7, 8, 9}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByIndexRangeOp(cdtBinName, 7, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByIndexRangeOp(cdtBinName, 8, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByIndexRangeCountOp(cdtBinName, 5, 2, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{6, 7}))
		})

		It("should Get by rank", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetByRankOp(cdtBinName, 7, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(8))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByRankRangeOp(cdtBinName, 7, as.ListReturnTypeIndex))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{7, 8, 9}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByRankRangeOp(cdtBinName, 7, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByRankRangeOp(cdtBinName, 8, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetByRankRangeCountOp(cdtBinName, 5, 2, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{6, 7}))
		})

		It("should append an element to the tail", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, listSize))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should append the same cached element to the tail", func() {
			ops := []*as.Operation{as.ListAppendOp(cdtBinName, math.MaxInt64)}
			cdtListRes, err := client.Operate(wpolicy, key, ops...)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, listSize))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))

			cdtListRes, err = client.Operate(wpolicy, key, ops...)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 2))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, listSize))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should append a few elements to the tail", func() {
			elems := []interface{}{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}
			cdtListRes, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, elems...))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, listSize, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should append a few elements to the tail with policy", func() {
			elems := []interface{}{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}
			cdtListRes, err := client.Operate(wpolicy, key, as.ListAppendWithPolicyOp(as.DefaultListPolicy(), cdtBinName, elems...))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListAppendWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 4))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, listSize, 4))
			Expect(err).ToNot(HaveOccurred())
			elems = append(elems, 0)
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should prepend an element to the head via ListInsertOp", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListInsertOp(cdtBinName, 0, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should prepend an element to the head via ListInsertWithPolicyOp", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListInsertWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListInsertWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0, math.MaxInt64-1, math.MaxInt64-2))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{math.MaxInt64 - 1, math.MaxInt64 - 2, math.MaxInt64, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
		})

		It("should prepend a few elements to the tail via ListInsertOp", func() {
			elems := []interface{}{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}
			cdtListRes, err := client.Operate(wpolicy, key, as.ListInsertOp(cdtBinName, 0, elems...))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should pop elements from the head", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListPopRangeOp(cdtBinName, 0, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[:3]))

			cdtListRes, err = client.Operate(wpolicy, key, as.GetOpForBin(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[3:]))
		})

		It("should pop element range from the index", func() {
			for i := listSize; i > 0; i-- {
				cdtListRes, err := client.Operate(wpolicy, key, as.ListPopOp(cdtBinName, 0))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[0]))

				list = list[1:]

				// TODO: Remove the IF later when server has changed
				if i > 1 {
					cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, i))
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				}
			}
		})

		It("should pop elements from element #7 to the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			cdtPopRes, err := client.Operate(wpolicy, key, as.ListPopRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtPopRes.Bins[cdtBinName]).To(Equal(list[7:]))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[:7]))

		})

		It("should remove elements from the head", func() {
			for i := listSize; i > 0; i-- {
				cdtListRes, err := client.Operate(wpolicy, key, as.ListRemoveOp(cdtBinName, 0))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(1))

				list = list[1:]

				// TODO: Remove the IF later when server has changed
				if i > 1 {
					cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, i))
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				}
			}
		})

		It("should remove elements from element #7 to the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			cdtRemoveRes, err := client.Operate(wpolicy, key, as.ListRemoveRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtRemoveRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[:7]))

		})

		It("should remove elements from the head in increasing numbers", func() {
			elemCount := listSize
			for i := 1; i <= 4; i++ {
				cdtListRes, err := client.Operate(wpolicy, key, as.ListRemoveRangeOp(cdtBinName, 0, i))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(i))

				list = list[i:]
				elemCount -= i

				// TODO: Remove the IF later when server has changed
				cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, elemCount))
				if elemCount > 0 {
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				}
			}
		})

		It("should remove elements by value", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListRemoveByValueListOp(cdtBinName, []interface{}{1, 2, 3, 4, 5, 6, 7}, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{1, 2, 3, 4, 5, 6, 7}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByValueOp(cdtBinName, 9, as.ListReturnTypeCount))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{8, 10}))
		})

		It("should remove elements by value range", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListRemoveByValueRangeOp(cdtBinName, as.ListReturnTypeValue, 1, 5))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{1, 2, 3, 4}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{5, 6, 7, 8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByValueRangeOp(cdtBinName, as.ListReturnTypeCount, 6, 9))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{5, 9, 10}))
		})

		It("should remove elements by index", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListRemoveByIndexOp(cdtBinName, 0, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3, 4, 5, 6, 7, 8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByIndexRangeOp(cdtBinName, 5, as.ListReturnTypeCount))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(4))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3, 4, 5, 6}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByIndexRangeCountOp(cdtBinName, 2, 3, as.ListReturnTypeCount))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3}))
		})

		It("should remove elements by rank", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListSortOp(cdtBinName, as.ListSortFlagsDefault))
			Expect(err).ToNot(HaveOccurred())

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByRankOp(cdtBinName, 0, as.ListReturnTypeValue))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(1))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3, 4, 5, 6, 7, 8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByRankRangeOp(cdtBinName, 5, as.ListReturnTypeCount))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(4))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3, 4, 5, 6}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListRemoveByRankRangeCountOp(cdtBinName, 2, 3, as.ListReturnTypeCount))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3}))
		})

		It("should increment elements", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			elemRes, err := client.Operate(wpolicy, key, as.ListIncrementOp(cdtBinName, 0, 10))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(11))

			elemRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(11))

			elemRes, err = client.Operate(wpolicy, key, as.ListIncrementByOneOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(12))

			elemRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(12))
		})

		It("should increment elements with policy", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			elemRes, err := client.Operate(wpolicy, key, as.ListIncrementWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0, 10))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(11))

			elemRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(11))

			elemRes, err = client.Operate(wpolicy, key, as.ListIncrementByOneWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(12))

			elemRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(12))
		})

		It("should sort elements with policy", func() {
			elemRes, err := client.Operate(wpolicy, key, as.ListIncrementWithPolicyOp(as.DefaultListPolicy(), cdtBinName, 0, 100))
			Expect(err).ToNot(HaveOccurred())
			Expect(elemRes.Bins[cdtBinName]).To(Equal(101))

			cdtListRes, err := client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{101, 2, 3, 4, 5, 6, 7, 8, 9, 10}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListSortOp(cdtBinName, as.ListSortFlagsDefault))
			Expect(err).ToNot(HaveOccurred())

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{2, 3, 4, 5, 6, 7, 8, 9, 10, 101}))
		})

		It("should set elements", func() {
			elems := []interface{}{}
			for i := 0; i < listSize; i++ {
				cdtListRes, err := client.Operate(wpolicy, key, as.ListSetOp(cdtBinName, i, math.MaxInt64))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins).To(Equal(as.BinMap{}))

				elems = append(elems, math.MaxInt64)

				cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, i+1))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
			}
		})

		It("should set the last element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, as.ListSetOp(cdtBinName, -1, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins).To(Equal(as.BinMap{}))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetOp(cdtBinName, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should trim list elements", func() {
			elems := []interface{}{3, 4, 5}
			cdtListRes, err := client.Operate(wpolicy, key, as.ListTrimOp(cdtBinName, 2, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(7))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListGetRangeOp(cdtBinName, 0, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should clear list elements", func() {
			for i := 0; i < listSize; i++ {
				_, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, i))
				Expect(err).ToNot(HaveOccurred())
			}
			cdtListRes, err := client.Operate(wpolicy, key, as.ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).NotTo(Equal(0))

			cdtListRes, err = client.Operate(wpolicy, key, as.ListClearOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(BeNil())

			cdtListRes, err = client.Operate(wpolicy, key, as.ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(0))
		})

	})

}) // describe

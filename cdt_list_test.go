// Copyright 2013-2016 Aerospike, Inc.
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

	. "github.com/aerospike/aerospike-client-go"
	// . "github.com/aerospike/aerospike-client-go/types"
)

var _ = Describe("CDT List Test", func() {
	initTestVars()

	// connection data
	var client *Client
	var err error
	var ns = "test"
	var set = randString(50)
	var key *Key
	var wpolicy = NewWritePolicy(0, 0)
	var cdtBinName string
	var list []interface{}

	BeforeEach(func() {
		client, err = NewClientWithPolicy(clientPolicy, *host, *port)
		Expect(err).ToNot(HaveOccurred())
		key, err = NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		cdtBinName = randString(10)
	})

	It("should create a valid CDT List", func() {
		cdtList, err := client.Operate(wpolicy, key, ListGetOp(cdtBinName, 0))
		Expect(err).ToNot(HaveOccurred())
		Expect(cdtList).To(BeNil())

		list := []interface{}{}
		for i := 1; i <= 100; i++ {
			list = append(list, i)

			sz, err := client.Operate(wpolicy, key, ListAppendOp(cdtBinName, i))
			Expect(err).ToNot(HaveOccurred())
			Expect(sz.Bins[cdtBinName]).To(Equal(i))

			sz, err = client.Operate(wpolicy, key, ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(sz.Bins[cdtBinName]).To(Equal(i))
		}

		sz, err := client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, 100))
		Expect(err).ToNot(HaveOccurred())
		Expect(sz.Bins[cdtBinName]).To(Equal(list))

		sz, err = client.Operate(wpolicy, key, ListAppendOp(cdtBinName, list...))
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

				sz, err := client.Operate(wpolicy, key, ListAppendOp(cdtBinName, i))
				Expect(err).ToNot(HaveOccurred())
				Expect(sz.Bins[cdtBinName]).To(Equal(i))
			}
		})

		It("should Get the last element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListGetOp(cdtBinName, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize))
		})

		It("should Get the last 3 element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, -3, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{listSize - 2, listSize - 1, listSize - 0}))
		})

		It("should Get the from element #7 till the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListGetRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal([]interface{}{listSize - 2, listSize - 1, listSize - 0}))
		})

		It("should append an element to the tail", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListAppendOp(cdtBinName, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetOp(cdtBinName, listSize))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should append a few elements to the tail", func() {
			elems := []interface{}{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}
			cdtListRes, err := client.Operate(wpolicy, key, ListAppendOp(cdtBinName, elems...))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, listSize, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should prepend an element to the head via ListInsertOp", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListInsertOp(cdtBinName, 0, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 1))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should prepend a few elements to the tail via ListInsertOp", func() {
			elems := []interface{}{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2}
			cdtListRes, err := client.Operate(wpolicy, key, ListInsertOp(cdtBinName, 0, elems...))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(listSize + 3))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should pop elements from the head", func() {
			for i := listSize; i > 0; i-- {
				cdtListRes, err := client.Operate(wpolicy, key, ListPopOp(cdtBinName, 0))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[0]))

				list = list[1:]

				// TODO: Remove the IF later when server has changed
				if i > 1 {
					cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, i))
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				}
			}
		})

		It("should pop elements from element #7 to the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			cdtPopRes, err := client.Operate(wpolicy, key, ListPopRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtPopRes.Bins[cdtBinName]).To(Equal(list[7:]))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[:7]))

		})

		It("should remove elements from the head", func() {
			for i := listSize; i > 0; i-- {
				cdtListRes, err := client.Operate(wpolicy, key, ListRemoveOp(cdtBinName, 0))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(1))

				list = list[1:]

				// TODO: Remove the IF later when server has changed
				if i > 1 {
					cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, i))
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				}
			}
		})

		It("should remove elements from element #7 to the end of list", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))

			cdtRemoveRes, err := client.Operate(wpolicy, key, ListRemoveRangeFromOp(cdtBinName, 7))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtRemoveRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeFromOp(cdtBinName, 0))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list[:7]))

		})

		It("should remove elements from the head in increasing numbers", func() {
			elemCount := listSize
			for i := 1; i <= 4; i++ {
				cdtListRes, err := client.Operate(wpolicy, key, ListRemoveRangeOp(cdtBinName, 0, i))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(i))

				list = list[i:]
				elemCount -= i

				// TODO: Remove the IF later when server has changed
				cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, elemCount))
				if elemCount > 0 {
					Expect(err).ToNot(HaveOccurred())
					Expect(cdtListRes.Bins[cdtBinName]).To(Equal(list))
				} else {
					Expect(err).To(HaveOccurred())
				}
			}
		})

		It("should set elements", func() {
			elems := []interface{}{}
			for i := 0; i < listSize; i++ {
				cdtListRes, err := client.Operate(wpolicy, key, ListSetOp(cdtBinName, i, math.MaxInt64))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins).To(Equal(BinMap{}))

				elems = append(elems, math.MaxInt64)

				cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, i+1))
				Expect(err).ToNot(HaveOccurred())
				Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
			}
		})

		It("should set the last element", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListSetOp(cdtBinName, -1, math.MaxInt64))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins).To(Equal(BinMap{}))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetOp(cdtBinName, -1))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(math.MaxInt64))
		})

		It("should trim list elements", func() {
			elems := []interface{}{3, 4, 5}
			cdtListRes, err := client.Operate(wpolicy, key, ListTrimOp(cdtBinName, 2, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(7))

			cdtListRes, err = client.Operate(wpolicy, key, ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(3))

			cdtListRes, err = client.Operate(wpolicy, key, ListGetRangeOp(cdtBinName, 0, 3))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(elems))
		})

		It("should trim list elements", func() {
			cdtListRes, err := client.Operate(wpolicy, key, ListClearOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(BeNil())

			cdtListRes, err = client.Operate(wpolicy, key, ListSizeOp(cdtBinName))
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtListRes.Bins[cdtBinName]).To(Equal(0))
		})

	})

}) // describe

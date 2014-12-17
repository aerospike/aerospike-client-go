// Copyright 2013-2014 Aerospike, Inc.
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
	"flag"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/types"
)

var _ = Describe("LargeList Test", func() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	// connection data
	var client *Client
	var err error
	var ns = "test"
	var set = randString(50)
	var key *Key
	var wpolicy = NewWritePolicy(0, 0)

	BeforeEach(func() {
		client, err = NewClient(*host, *port)
		Expect(err).ToNot(HaveOccurred())
		key, err = NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())
	})

	It("should create a valid LargeList; Support Add(), Remove(), Find(), Size(), Scan(), Destroy() and GetCapacity()", func() {
		llist := client.GetLargeList(wpolicy, key, randString(10), "")
		res, err := llist.Size()
		Expect(err).ToNot(HaveOccurred()) // bin not exists
		Expect(res).To(Equal(0))

		for i := 1; i <= 100; i++ {
			err = llist.Add(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			// confirm that the LLIST size has been increased to the expected size
			sz, err := llist.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sz).To(Equal(i))
		}

		sz, err := llist.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		cap, err := llist.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		// default capacity is 100
		Expect(cap).To(Equal(sz))

		// Scan() the list
		scanResult, err := llist.Scan()
		scanExpectation := []interface{}{}
		for i := 1; i <= 100; i++ {
			scanExpectation = append(scanExpectation, interface{}(i))
		}
		Expect(err).ToNot(HaveOccurred())
		Expect(len(scanResult)).To(Equal(100))
		Expect(scanResult).To(Equal(scanExpectation))

		for i := 1; i <= 100; i++ {
			// confirm that the value already exists in the LLIST
			findResult, err := llist.Find(NewValue(i))
			Expect(err).ToNot(HaveOccurred())
			Expect(findResult).To(Equal([]interface{}{i}))

			// check for a non-existing element
			findResult, err = llist.Find(i * 70000)
			Expect(err).To(HaveOccurred())
			Expect(findResult).To(BeNil())

			// remove the value
			err = llist.Remove(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			// make sure the value has been removed
			findResult, err = llist.Find(NewValue(i))
			Expect(err).To(HaveOccurred())
			Expect(err.(AerospikeError).ResultCode()).To(Equal(LARGE_ITEM_NOT_FOUND))
		}

		err = llist.Destroy()
		Expect(err).ToNot(HaveOccurred())

		scanResult, err = llist.Scan()
		Expect(err).ToNot(HaveOccurred())
		Expect(len(scanResult)).To(Equal(0))
	})

	It("should correctly GetConfig()", func() {
		llist := client.GetLargeList(wpolicy, key, randString(10), "")
		err = llist.Add(NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		config, err := llist.GetConfig()
		Expect(err).ToNot(HaveOccurred())
		Expect(config["SUMMARY"]).To(Equal("LList Summary"))
	})

	It("should correctly Get/SetCapacity()", func() {
		const cap = 99

		llist := client.GetLargeList(wpolicy, key, randString(10), "")
		err = llist.Add(NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		err = llist.SetCapacity(cap)
		Expect(err).ToNot(HaveOccurred())

		tcap, err := llist.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		Expect(tcap).To(Equal(cap))

		for i := 1; i < cap; i++ {
			err = llist.Add(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			sz, err := llist.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sz).To(Equal(i + 1))
		}

		sz, err := llist.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		Expect(sz).To(Equal(cap))
	})

}) // describe

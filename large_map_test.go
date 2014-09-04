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
)

var _ = Describe("LargeMap Test", func() {
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

	It("should create a valid LargeMap; Support PutMap() and Size()", func() {
		lmap := client.GetLargeMap(wpolicy, key, randString(10), "")
		_, err := lmap.Size()
		Expect(err).To(HaveOccurred()) // bin not exists

		testMap := make(map[interface{}]interface{})
		for i := 1; i <= 100; i++ {
			testMap[i*100] = i
		}

		err = lmap.PutMap(testMap)
		Expect(err).ToNot(HaveOccurred())

		// confirm that the LMAP size has been increased to the expected size
		sz, err := lmap.Size()
		Expect(err).ToNot(HaveOccurred())
		Expect(sz).To(Equal(100))
	})

	It("should create a valid LargeMap; Support Put(), Remove(), Find(), Size(), Scan() and GetCapacity()", func() {
		lmap := client.GetLargeMap(wpolicy, key, randString(10), "")
		_, err := lmap.Size()
		Expect(err).To(HaveOccurred()) // bin not exists

		for i := 1; i <= 100; i++ {
			err = lmap.Put(NewValue(i*100), NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			// confirm that the LMAP size has been increased to the expected size
			sz, err := lmap.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sz).To(Equal(i))
		}

		sz, err := lmap.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		cap, err := lmap.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		// default capacity is 100
		Expect(cap).To(Equal(sz))

		// Scan() the map
		scanResult, err := lmap.Scan()
		scanExpectation := make(map[interface{}]interface{})
		for i := 1; i <= 100; i++ {
			scanExpectation[interface{}(i*100)] = i
		}
		Expect(err).ToNot(HaveOccurred())
		Expect(len(scanResult)).To(Equal(100))
		Expect(scanResult).To(Equal(scanExpectation))

		// for i := 1; i <= 100; i++ {
		// 	// confirm that the value already exists in the LMAP
		// 	findResult, err := lmap.Get(NewValue(i * 100))
		// 	Expect(err).ToNot(HaveOccurred())
		// 	Expect(findResult).To(Equal([]interface{}{i}))

		// 	// remove the value
		// 	err = lmap.Remove(NewValue(i))
		// 	Expect(err).ToNot(HaveOccurred())

		// 	// make sure the value has been removed
		// 	findResult, err = lmap.Find(NewValue(i))
		// 	Expect(err).To(HaveOccurred())
		// 	Expect(err.Error()).To(ContainSubstring("LDT-Item Not Found"))
		// }

	})

	It("should correctly GetConfig()", func() {
		lmap := client.GetLargeMap(wpolicy, key, randString(10), "")
		err = lmap.Put(NewValue(0), NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		config, err := lmap.GetConfig()
		Expect(err).ToNot(HaveOccurred())
		Expect(config["SUMMARY"]).To(Equal("LMAP Summary"))
	})

	// It("should correctly Get/SetCapacity()", func() {
	// 	const cap = 100000

	// 	lmap := client.GetLargeMap(wpolicy, key, randString(10), "")
	// 	err = lmap.Put(NewValue(0), NewValue(0))
	// 	Expect(err).ToNot(HaveOccurred())

	// 	err = lmap.SetCapacity(cap)
	// 	Expect(err).ToNot(HaveOccurred())

	// 	tcap, err := lmap.GetCapacity()
	// 	Expect(err).ToNot(HaveOccurred())

	// 	Expect(tcap).To(Equal(cap))

	// 	for i := 1; i < cap; i++ {
	// 		err = lmap.Put(NewValue(i*100), NewValue(i))
	// 		Expect(err).ToNot(HaveOccurred())

	// 		sz, err := lmap.Size()
	// 		Expect(err).ToNot(HaveOccurred())
	// 		Expect(sz).To(Equal(i + 1))
	// 	}

	// 	sz, err := lmap.GetCapacity()
	// 	Expect(err).ToNot(HaveOccurred())

	// 	Expect(sz).To(Equal(cap))
	// })

}) // describe

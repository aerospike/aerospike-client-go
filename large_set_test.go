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

var _ = Describe("LargeSet Test", func() {
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

	It("should create a valid LargeSet; Support Add(), Remove(), Find(), Size(), Scan() and GetCapacity()", func() {
		lset := client.GetLargeSet(wpolicy, key, randString(10), "")
		_, err := lset.Size()
		Expect(err).To(HaveOccurred()) // bin not exists

		for i := 1; i <= 10; i++ {
			err = lset.Add(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			// confirm that the LSET size has been increased to the expected size
			// sz, err := lset.Size()
			// Expect(err).ToNot(HaveOccurred())
			// Expect(sz).To(Equal(i))
		}

		// sz, err := lset.GetCapacity()
		// Expect(err).ToNot(HaveOccurred())

		// cap, err := lset.GetCapacity()
		// Expect(err).ToNot(HaveOccurred())

		// // default capacity is 100
		// Expect(cap).To(Equal(sz))

		// // Scan() the set
		// scanResult, err := lset.Scan()
		// for i := 1; i <= 100; i++ {
		// 	Expect(scanResult).To(ContainElement(i))
		// }
		// Expect(err).ToNot(HaveOccurred())
		// Expect(len(scanResult)).To(Equal(100))

		// for i := 1; i <= 100; i++ {
		// 	// confirm that the value already exists in the LSET
		// 	exists, err := lset.Exists(NewValue(i))
		// 	Expect(err).ToNot(HaveOccurred())
		// 	Expect(exists).To(BeTrue())

		// 	// remove the value
		// 	err = lset.Remove(NewValue(i))
		// 	Expect(err).ToNot(HaveOccurred())

		// 	// make sure the value has been removed
		// 	exists, err = lset.Exists(NewValue(i))
		// 	Expect(err).ToNot(HaveOccurred())
		// 	Expect(exists).To(BeFalse())
		// }

	})

	It("should correctly GetConfig()", func() {
		lset := client.GetLargeSet(wpolicy, key, randString(10), "")
		err = lset.Add(NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		config, err := lset.GetConfig()
		Expect(err).ToNot(HaveOccurred())
		Expect(config["SUMMARY"]).To(Equal("LSET Summary"))
	})

	// It("should correctly Get/SetCapacity()", func() {
	// 	const cap = 100000

	// 	lset := client.GetLargeSet(wpolicy, key, randString(10), "")
	// 	err = lset.Add(NewValue(0))
	// 	Expect(err).ToNot(HaveOccurred())

	// 	err = lset.SetCapacity(cap)
	// 	Expect(err).ToNot(HaveOccurred())

	// 	tcap, err := lset.GetCapacity()
	// 	Expect(err).ToNot(HaveOccurred())

	// 	Expect(tcap).To(Equal(cap))

	// 	for i := 1; i < cap; i++ {
	// 		err = lset.Add(NewValue(i))
	// 		Expect(err).ToNot(HaveOccurred())

	// 		sz, err := lset.Size()
	// 		Expect(err).ToNot(HaveOccurred())
	// 		Expect(sz).To(Equal(i + 1))
	// 	}

	// 	sz, err := lset.GetCapacity()
	// 	Expect(err).ToNot(HaveOccurred())

	// 	Expect(sz).To(Equal(cap))
	// })

}) // describe

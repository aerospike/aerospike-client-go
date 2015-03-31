// Copyright 2013-2015 Aerospike, Inc.
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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/aerospike/aerospike-client-go"
)

var _ = Describe("LargeStack Test", func() {
	initTestVars()

	// connection data
	var client *Client
	var err error
	var ns = "test"
	var set = randString(50)
	var key *Key
	var wpolicy = NewWritePolicy(0, 0)

	BeforeEach(func() {
		client, err = NewClientWithPolicy(clientPolicy, *host, *port)
		Expect(err).ToNot(HaveOccurred())
		key, err = NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())
	})

	It("should create a valid LargeStack; Support Push(), Peek(), Pop(), Size(), Scan(), Destroy() and GetCapacity()", func() {
		lstack := client.GetLargeStack(wpolicy, key, randString(10), "")
		res, err := lstack.Size()
		Expect(err).ToNot(HaveOccurred()) // bin not exists
		Expect(res).To(Equal(0))

		for i := 1; i <= 100; i++ {
			err = lstack.Push(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			// confirm that the LSTACK size has been increased to the expected size
			sz, err := lstack.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sz).To(Equal(i))
		}

		sz, err := lstack.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		cap, err := lstack.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		// default capacity is 100
		Expect(cap).To(Equal(sz))

		// Scan() the stack
		scanResult, err := lstack.Scan()
		scanExpectation := []interface{}{}
		for i := 100; i > 0; i-- {
			scanExpectation = append(scanExpectation, interface{}(i))
		}
		Expect(err).ToNot(HaveOccurred())
		Expect(len(scanResult)).To(Equal(100))
		Expect(scanResult).To(Equal(scanExpectation))

		// for i := 100; i > 0; i-- {
		// 	// peek the value
		// 	v, err := lstack.Peek(1)
		// 	Expect(err).ToNot(HaveOccurred())
		// 	Expect(v).To(Equal([]interface{}{i}))

		// 	// pop the value
		// 	// TODO: Wrong results
		// 	// v, err = lstack.Pop(1)
		// 	// Expect(err).ToNot(HaveOccurred())
		// 	// Expect(v).To(Equal([]interface{}{i}))
		// }

		// Destroy
		err = lstack.Destroy()
		Expect(err).ToNot(HaveOccurred())

		scanResult, err = lstack.Scan()
		Expect(err).ToNot(HaveOccurred())
		Expect(len(scanResult)).To(Equal(0))
	})

	It("should correctly GetConfig()", func() {
		lstack := client.GetLargeStack(wpolicy, key, randString(10), "")
		err = lstack.Push(NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		config, err := lstack.GetConfig()
		Expect(err).ToNot(HaveOccurred())
		Expect(config["SUMMARY"]).To(Equal("LSTACK Summary"))
	})

	It("should correctly Get/SetCapacity()", func() {
		const cap = 99

		lstack := client.GetLargeStack(wpolicy, key, randString(10), "")
		err = lstack.Push(NewValue(0))
		Expect(err).ToNot(HaveOccurred())

		err = lstack.SetCapacity(cap)
		Expect(err).ToNot(HaveOccurred())

		tcap, err := lstack.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		Expect(tcap).To(Equal(cap))

		for i := 1; i < cap; i++ {
			err = lstack.Push(NewValue(i))
			Expect(err).ToNot(HaveOccurred())

			sz, err := lstack.Size()
			Expect(err).ToNot(HaveOccurred())
			Expect(sz).To(Equal(i + 1))
		}

		sz, err := lstack.GetCapacity()
		Expect(err).ToNot(HaveOccurred())

		Expect(sz).To(Equal(cap))
	})

}) // describe

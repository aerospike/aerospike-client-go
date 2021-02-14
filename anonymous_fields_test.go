// +build !as_performance

// Copyright 2014-2019 Aerospike, Inc.
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

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Aerospike", func() {

	gg.Describe("PutObject and GetObject with anonymous fields", func() {
		// connection data
		var err error
		var ns = *namespace
		var set = randString(50)

		type anonymousStructA struct {
			A   int    `as:"a"`
			TTL uint32 `asm:"ttl"`
			GEN uint32 `asm:"gen"`
		}

		type anonymousStructB struct {
			B   string `as:"b"`
			TTL uint32 `asm:"ttl"`
			GEN uint32 `asm:"gen"`
		}

		type anonymousStructABC struct {
			anonymousStructA
			anonymousStructB
			A          bool    `as:"ace"`
			B          int     `as:"bce"`
			C          float64 `as:"c"`
			TTL1, TTL2 uint32  `asm:"ttl"`
			GEN1, GEN2 uint32  `asm:"gen"`
		}

		type anonymousStructABCD struct {
			ABC *anonymousStructABC
			D   bool `as:"d"`
		}

		type testStruct struct {
			anonymousStructABC
			anonymousStructABCD
		}

		makeTestObject := func() *testStruct {
			obj := &testStruct{}
			obj.A = true
			obj.B = 20
			obj.anonymousStructA.A = 10
			obj.anonymousStructB.B = "Hello"
			obj.C = 3.14159
			obj.ABC = &anonymousStructABC{}
			obj.ABC.A = false
			obj.ABC.B = 42
			obj.ABC.anonymousStructA.A = 28
			obj.ABC.anonymousStructB.B = "World!"
			obj.ABC.C = 2.17828
			obj.D = true
			return obj
		}

		gg.Context("PutObject & GetObject operations", func() {
			gg.It("must save an object with anonymous fields", func() {
				key, _ := as.NewKey(ns, set, randString(50))
				expected := makeTestObject()
				err = client.PutObject(nil, key, expected)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				actual := &testStruct{}
				err = client.GetObject(nil, key, actual)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				// make sure the returned BinMap here reflects what you
				// expect the final marshalled object should be.
				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{
					"ABC": map[interface{}]interface{}{
						"b":   "World!",
						"ace": 0,
						"bce": 42,
						"c":   2.17828,
						"a":   28,
					},
					"d":   1,
					"a":   10,
					"b":   "Hello",
					"ace": 1,
					"bce": 20,
					"c":   3.14159,
				}))

				gm.Expect(actual.TTL1).NotTo(gm.Equal(uint32(0)))
				gm.Expect(actual.TTL1).To(gm.Equal(actual.TTL2))
				gm.Expect(actual.TTL1).To(gm.Equal(actual.anonymousStructA.TTL))
				gm.Expect(actual.TTL1).To(gm.Equal(actual.anonymousStructB.TTL))

				gm.Expect(actual.GEN1).To(gm.Equal(uint32(1)))
				gm.Expect(actual.GEN2).To(gm.Equal(uint32(1)))
				gm.Expect(actual.anonymousStructA.GEN).To(gm.Equal(uint32(1)))
				gm.Expect(actual.anonymousStructB.GEN).To(gm.Equal(uint32(1)))

			})
		})
	})
})

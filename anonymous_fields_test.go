// +build !as_performance

// Copyright 2013-2019 Aerospike, Inc.
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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Aerospike", func() {

	Describe("PutObject and GetObject with anonymous fields", func() {
		// connection data
		var err error
		var ns = *namespace
		var set = randString(50)

		type anonymousStructA struct {
			A int `as:"a"`
		}

		type anonymousStructB struct {
			B string `as:"b"`
		}

		type anonymousStructABC struct {
			anonymousStructA
			anonymousStructB
			C float64 `as:"c"`
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
			obj.A = 10
			obj.B = "Hello"
			obj.C = 3.14159
			obj.ABC = &anonymousStructABC{}
			obj.ABC.A = 20
			obj.ABC.B = "World"
			obj.ABC.C = 2.17828
			obj.D = true
			return obj
		}

		Context("PutObject & GetObject operations", func() {
			It("must save an object with anonymous fields", func() {
				key, _ := as.NewKey(ns, set, randString(50))
				expected := makeTestObject()
				err = client.PutObject(nil, key, expected)
				Expect(err).ToNot(HaveOccurred())

				var actual testStruct
				err = client.GetObject(nil, key, &actual)
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(Equal(*expected))
			})
		})
	})
})

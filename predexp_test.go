// Copyright 2017 Aerospike, Inc.
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
	"fmt"
	
	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("predexp operations", func() {
	initTestVars()

	const keyCount = 1000
	
	var ns = "test"
	var set = randString(10)
	var wpolicy = NewWritePolicy(0, 0)

	BeforeEach(func() {
		for ii := 0; ii < keyCount; ii++ {
			key, err := NewKey(ns, set, ii)
			Expect(err).ToNot(HaveOccurred())
			bins := BinMap{
				"intval":	ii,
				"strval":	fmt.Sprintf("0x%04x", ii),
				"modval":	ii % 10,
			}
			err = client.Put(wpolicy, key, bins)
		}

		idxTask, err := client.CreateIndex(
			wpolicy, ns, set, "intval", "intval", NUMERIC)
		Expect(err).ToNot(HaveOccurred())
		Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())

		idxTask, err = client.CreateIndex(
			wpolicy, ns, set, "strval", "strval", STRING)
		Expect(err).ToNot(HaveOccurred())
		Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.DropIndex(nil, ns, set, "intval")).ToNot(HaveOccurred())
		Expect(client.DropIndex(nil, ns, set, "strval")).ToNot(HaveOccurred())
	})

	It("predexp must additionally filter indexed query results", func() {

		stm := NewStatement(ns, set)
		stm.Addfilter(NewRangeFilter("intval", 0, 400))
		stm.AddPredExp(NewPredExpIntegerValue(8))
		stm.AddPredExp(NewPredExpIntegerBin("modval"))
		stm.AddPredExp(NewPredExpIntegerGreaterEq())
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		// The query clause selects [0, 1, ... 400, 401] The predexp
		// only takes mod 8 and 9, should be 2 pre decade or 80 total.
		
		cnt := 0
		for res := range recordset.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			cnt++
		}

		Expect(cnt).To(BeNumerically("==", 80))
	})
})

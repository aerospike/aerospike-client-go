// Copyright 2014-2022 Aerospike, Inc.
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
	as "github.com/aerospike/aerospike-client-go/v6"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Query operations with Context", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 1000

	bin1Name := "List"
	var keys map[string]*as.Key

	gg.BeforeEach(func() {
		keys = make(map[string]*as.Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			keys[string(key.Digest())] = key

			list := []int{i, i + 1, i + 2, i + 3, i + 4}
			bin1 := as.NewBin(bin1Name, list)
			err = client.PutBins(wpolicy, key, bin1)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		idxTask1, err := client.CreateComplexIndex(wpolicy, ns, set, set+bin1Name, bin1Name, as.NUMERIC, as.ICT_DEFAULT, as.CtxListRank(-1))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until index is created
		gm.Expect(<-idxTask1.OnComplete()).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		gm.Expect(client.DropIndex(nil, ns, set, set+bin1Name)).ToNot(gm.HaveOccurred())
	})

	var queryPolicy = as.NewQueryPolicy()

	gg.It("must Query with a Context", func() {
		begin := 14
		end := 18

		stm := as.NewStatement(ns, set, bin1Name)
		stm.SetFilter(as.NewRangeFilter(bin1Name, int64(begin), int64(end), as.CtxListRank(-1)))

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			list := rec.Bins[bin1Name].([]interface{})
			received := list[len(list)-1].(int)

			gm.Expect(received < begin || received > end).To(gm.BeFalse())

			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically("==", 5))
	})

})

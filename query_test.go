// Copyright 2013-2020 Aerospike, Inc.
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
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const udfFilter = `
local function map_profile(record)
 -- Add name and age to returned map.
 -- Could add other record bins here as well.
 -- return map {name=record["name"], age=32}
 return map {bin4=record.Aerospike4, bin5=record["Aerospike5"]}
end

function filter_by_name(stream,name)
 local function filter_name(record)
   return (record.Aerospike5 == -1) and (record.Aerospike4 == 'constValue')
 end
 return stream : filter(filter_name) : map(map_profile)
end`

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("Query operations", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 1000
	bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := as.NewBin("Aerospike2", randString(100))
	bin3 := as.NewBin("Aerospike3", rand.Intn(math.MaxInt16))
	bin4 := as.NewBin("Aerospike4", "constValue")
	bin5 := as.NewBin("Aerospike5", -1)
	bin6 := as.NewBin("Aerospike6", 1)
	var keys map[string]*as.Key
	var indexName string
	var indexName2 string

	// read all records from the channel and make sure all of them are returned
	var checkResults = func(recordset *as.Recordset, cancelCnt int) {
		counter := 0
		for res := range recordset.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			rec := res.Record

			key, exists := keys[string(rec.Key.Digest())]

			Expect(exists).To(Equal(true))
			Expect(key.Value().GetObject()).To(Equal(rec.Key.Value().GetObject()))
			Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
			Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))

			delete(keys, string(rec.Key.Digest()))

			counter++
			// cancel scan abruptly
			if cancelCnt != 0 && counter == cancelCnt {
				recordset.Close()
			}
		}

		Expect(counter).To(BeNumerically(">", 0))
	}

	BeforeEach(func() {
		keys = make(map[string]*as.Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			keys[string(key.Digest())] = key
			bin3 = as.NewBin("Aerospike3", rand.Intn(math.MaxInt16))
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4, bin5, bin6)
			Expect(err).ToNot(HaveOccurred())
		}

		// queries only work on indices
		indexName = set + bin3.Name
		idxTask, err := client.CreateIndex(wpolicy, ns, set, indexName, bin3.Name, as.NUMERIC)
		Expect(err).ToNot(HaveOccurred())

		// wait until index is created
		Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())

		// queries only work on indices
		indexName2 = set + bin6.Name
		idxTask, err = client.CreateIndex(wpolicy, ns, set, indexName2, bin6.Name, as.NUMERIC)
		Expect(err).ToNot(HaveOccurred())

		// wait until index is created
		Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())
	})

	for _, failOnClusterChange := range []bool{false, true} {
		var queryPolicy = as.NewQueryPolicy()
		queryPolicy.FailOnClusterChange = failOnClusterChange

		It(fmt.Sprintf("must return error if query on non-indexed field. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("Non-Existing", 0, math.MaxInt16/2))

			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			for res := range recordset.Results() {
				Expect(res.Err).To(HaveOccurred())
			}
		})

		It(fmt.Sprintf("must Query a range and get all records back. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			checkResults(recordset, 0)

			Expect(len(keys)).To(Equal(0))
		})

		It(fmt.Sprintf("must Query a range and get all records back with policy.RecordsPerSecond set. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)

			policy := as.NewQueryPolicy()
			policy.RecordsPerSecond = keyCount - 100
			policy.FailOnClusterChange = queryPolicy.FailOnClusterChange
			recordset, err := client.Query(policy, stm)
			Expect(err).ToNot(HaveOccurred())

			checkResults(recordset, 0)

			Expect(len(keys)).To(Equal(0))
		})

		It(fmt.Sprintf("must Query a range and get all records back without the Bin Data. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)
			qp := as.NewQueryPolicy()
			qp.IncludeBinData = false
			qp.FailOnClusterChange = queryPolicy.FailOnClusterChange
			recordset, err := client.Query(qp, stm)
			Expect(err).ToNot(HaveOccurred())

			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				rec := res.Record

				key, exists := keys[string(rec.Key.Digest())]

				Expect(exists).To(Equal(true))
				Expect(key.Value().GetObject()).To(Equal(rec.Key.Value().GetObject()))
				Expect(len(rec.Bins)).To(Equal(0))

				delete(keys, string(rec.Key.Digest()))
			}

			Expect(len(keys)).To(Equal(0))
		})

		It(fmt.Sprintf("must Cancel Query abruptly. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			checkResults(recordset, keyCount/2)

			Expect(len(keys)).To(BeNumerically("<=", keyCount/2))
		})

		It(fmt.Sprintf("must Query a specific range and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			cnt := 0
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				rec := res.Record
				cnt++
				_, exists := keys[string(rec.Key.Digest())]
				Expect(exists).To(Equal(true))
				Expect(rec.Bins[bin3.Name]).To(BeNumerically("<=", math.MaxInt16/2))
			}

			Expect(cnt).To(BeNumerically(">", 0))
		})

		It(fmt.Sprintf("must Query a specific range by applying a udf filter and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			regTask, err := client.RegisterUDF(nil, []byte(udfFilter), "udfFilter.lua", as.LUA)
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is created
			err = <-regTask.OnComplete()
			Expect(err).ToNot(HaveOccurred())

			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
			stm.SetAggregateFunction("udfFilter", "filter_by_name", []as.Value{as.NewValue("Aeropsike")}, true)

			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			cnt := 0
			for rec := range recordset.Records {
				results := rec.Bins["SUCCESS"].(map[interface{}]interface{})
				Expect(results["bin4"]).To(Equal("constValue"))
				// Expect(results["bin5"]).To(Equal(-1))
				cnt++
			}

			Expect(cnt).To(BeNumerically(">", 0))
		})

		It(fmt.Sprintf("must Query specific equality filters and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			// save a record with requested value
			key, err := as.NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			bin3 := as.NewBin("Aerospike3", rand.Intn(math.MaxInt16))
			err = client.PutBins(wpolicy, key, bin3)
			Expect(err).ToNot(HaveOccurred())

			stm := as.NewStatement(ns, set, bin3.Name)
			stm.SetFilter(as.NewEqualFilter(bin3.Name, bin3.Value))

			recordset, err := client.Query(queryPolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			recs := []interface{}{}
			// consume recordset and check errors
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				rec := res.Record
				Expect(rec).ToNot(BeNil())
				recs = append(recs, rec.Bins[bin3.Name])
			}

			// there should be at least one result
			Expect(len(recs)).To(BeNumerically(">", 0))
			Expect(recs).To(ContainElement(bin3.Value.GetObject()))
		})

		It(fmt.Sprintf("must Query specific equality filters and apply operations on the records. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

			bin7 := as.NewBin("Aerospike7", 42)
			tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin7))
			Expect(err).ToNot(HaveOccurred())
			Expect(<-tsk.OnComplete()).To(BeNil())

			// read records back
			stmRes := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stmRes)
			Expect(err).ToNot(HaveOccurred())

			recs := []interface{}{}
			// consume recordset and check errors
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				rec := res.Record
				Expect(rec).ToNot(BeNil())
				recs = append(recs, rec.Bins[bin3.Name])
				Expect(rec.Bins[bin7.Name]).To(Equal(bin7.Value.GetObject().(int)))
			}

			// there should be at least one result
			Expect(len(recs)).To(Equal(keyCount))
		})

		It(fmt.Sprintf("must Query specific equality filters and apply operations on the records without filters. FailOnClusterChange: %v", failOnClusterChange), func() {
			defer client.DropIndex(nil, ns, set, indexName)
			defer client.DropIndex(nil, ns, set, indexName2)

			stm := as.NewStatement(ns, set)

			bin7 := as.NewBin("Aerospike7", 42)
			tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin7))
			Expect(err).ToNot(HaveOccurred())
			Expect(<-tsk.OnComplete()).To(BeNil())

			// read records back
			stmRes := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stmRes)
			Expect(err).ToNot(HaveOccurred())

			recs := []interface{}{}
			// consume recordset and check errors
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				rec := res.Record
				Expect(rec).ToNot(BeNil())
				recs = append(recs, rec.Bins[bin3.Name])
				Expect(rec.Bins[bin7.Name]).To(Equal(bin7.Value.GetObject().(int)))
			}

			// there should be at least one result
			Expect(len(recs)).To(Equal(keyCount))
		})
	}
})

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
	"math/rand"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/types"

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

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Query operations", func() {
	initTestVars()

	// connection data
	var ns = "test"
	var set = randString(50)
	var wpolicy = NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 1000
	bin1 := NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := NewBin("Aerospike2", randString(100))
	bin3 := NewBin("Aerospike3", rand.Intn(math.MaxInt16))
	bin4 := NewBin("Aerospike4", "constValue")
	bin5 := NewBin("Aerospike5", -1)
	var keys map[string]*Key

	// use the same client for all
	client, err := NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		panic(err)
	}

	// read all records from the channel and make sure all of them are returned
	var checkResults = func(recordset *Recordset, cancelCnt int) {
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
		keys = make(map[string]*Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			keys[string(key.Digest())] = key
			bin3 = NewBin("Aerospike3", rand.Intn(math.MaxInt16))
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4, bin5)
			Expect(err).ToNot(HaveOccurred())
		}

		// queries only work on indices
		idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin3.Name, bin3.Name, NUMERIC)
		Expect(err).ToNot(HaveOccurred())

		// wait until index is created
		Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())
	})

	It("must return error if more than onlt filter passed to the command", func() {
		stm := NewStatement(ns, set)
		stm.Addfilter(NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		stm.Addfilter(NewRangeFilter(bin3.Name, 2, math.MaxInt16/2))

		Expect(len(stm.Filters)).To(Equal(2))

		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		for res := range recordset.Results() {
			Expect(res.Err).To(HaveOccurred())
			ae, ok := res.Err.(AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(ae.ResultCode()).To(Equal(PARAMETER_ERROR))
		}
	})

	It("must Query a range and get all records back", func() {
		stm := NewStatement(ns, set)
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, 0)

		Expect(len(keys)).To(Equal(0))
	})

	It("must Cancel Query abruptly", func() {
		stm := NewStatement(ns, set)
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, keyCount/2)

		Expect(len(keys)).To(BeNumerically("<=", keyCount/2))
	})

	It("must Query a specific range and get only relevant records back", func() {
		stm := NewStatement(ns, set)
		stm.Addfilter(NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		recordset, err := client.Query(nil, stm)
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

	It("must Query a specific range by applying a udf filter and get only relevant records back", func() {
		regTask, err := client.RegisterUDF(nil, []byte(udfFilter), "udfFilter.lua", LUA)
		Expect(err).ToNot(HaveOccurred())

		// wait until UDF is created
		err = <-regTask.OnComplete()
		Expect(err).ToNot(HaveOccurred())

		stm := NewStatement(ns, set)
		stm.Addfilter(NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		stm.SetAggregateFunction("udfFilter", "filter_by_name", []Value{NewValue("Aeropsike")}, true)

		recordset, err := client.Query(nil, stm)
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

	It("must Query specific equality filters and get only relevant records back", func() {
		// save a record with requested value
		key, err := NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		bin3 := NewBin("Aerospike3", rand.Intn(math.MaxInt16))
		err = client.PutBins(wpolicy, key, bin3)
		Expect(err).ToNot(HaveOccurred())

		stm := NewStatement(ns, set, bin3.Name)
		stm.Addfilter(NewEqualFilter(bin3.Name, bin3.Value))

		recordset, err := client.Query(nil, stm)
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

})

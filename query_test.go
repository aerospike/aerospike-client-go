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
	"bytes"
	"errors"
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go/v6"
	ast "github.com/aerospike/aerospike-client-go/v6/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
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
var _ = gg.Describe("Query operations", func() {

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
	bin7 := as.NewBin("Aerospike7", nil)
	var keys map[string]*as.Key
	var indexName string
	var indexName2 string
	var indexName3 string

	// read all records from the channel and make sure all of them are returned
	var checkResults = func(recordset *as.Recordset, cancelCnt int) {
		counter := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
			gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
			gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

			delete(keys, string(rec.Key.Digest()))

			counter++
			// cancel scan abruptly
			if cancelCnt != 0 && counter == cancelCnt {
				recordset.Close()
			}
		}

		gm.Expect(counter).To(gm.BeNumerically(">", 0))
	}

	gg.BeforeEach(func() {
		nativeClient.Truncate(nil, ns, set, nil)

		keys = make(map[string]*as.Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			keys[string(key.Digest())] = key
			bin3 = as.NewBin("Aerospike3", rand.Intn(math.MaxInt16))
			bin7 = as.NewBin("Aerospike7", i%3)
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		indexName = set + bin3.Name
		createIndex(wpolicy, ns, set, indexName, bin3.Name, as.NUMERIC)

		// queries only work on indices
		indexName2 = set + bin6.Name
		createIndex(wpolicy, ns, set, indexName2, bin6.Name, as.NUMERIC)

		// queries only work on indices
		indexName3 = set + bin7.Name
		createIndex(wpolicy, ns, set, indexName3, bin7.Name, as.NUMERIC)
	})

	gg.AfterEach(func() {
		indexName = set + bin3.Name
		gm.Expect(nativeClient.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())

		indexName = set + bin6.Name
		gm.Expect(nativeClient.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())

		indexName = set + bin7.Name
		gm.Expect(nativeClient.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())
	})

	var queryPolicy = as.NewQueryPolicy()

	gg.It("must Query and get all records back for a specified node using Results() channel", func() {
		if *proxy {
			gg.Skip("Not Supported for Proxy Client")
		}

		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		stm := as.NewStatement(ns, set)

		counter := 0
		for _, node := range client.GetNodes() {
			recordset, err := client.QueryNode(queryPolicy, node, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				key, exists := keys[string(res.Record.Key.Digest())]

				gm.Expect(exists).To(gm.Equal(true))
				gm.Expect(key.Value().GetObject()).To(gm.Equal(res.Record.Key.Value().GetObject()))
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(res.Record.Key.Digest()))

				counter++
			}
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
		gm.Expect(counter).To(gm.Equal(keyCount))
	})

	gg.It("must Scan and get all partition records back for a specified key", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0

		var rkey *as.Key
		for _, k := range keys {
			rkey = k

			pf := as.NewPartitionFilterByKey(rkey)
			stm := as.NewStatement(ns, set)
			recordset, err := client.QueryPartitions(queryPolicy, stm, pf)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				// the key itself should not be returned
				gm.Expect(bytes.Equal(rkey.Digest(), res.Record.Key.Digest())).To(gm.BeFalse())

				delete(keys, string(res.Record.Key.Digest()))

				counter++
			}

		}
		gm.Expect(len(keys)).To(gm.BeNumerically(">", 0))
		gm.Expect(counter).To(gm.BeNumerically(">", 0))
		gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
	})

	gg.It("must Query per key partition and get all partition records back for a specified key and filter", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0

		var rkey *as.Key
		for _, k := range keys {
			rkey = k

			pf := as.NewPartitionFilterByKey(rkey)
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter(bin7.Name, 1, 2))
			recordset, err := client.QueryPartitions(queryPolicy, stm, pf)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin7.Name]).To(gm.BeNumerically(">=", 1))
				gm.Expect(res.Record.Bins[bin7.Name]).To(gm.BeNumerically("<=", 2))

				delete(keys, string(res.Record.Key.Digest()))

				counter++
			}
		}

		gm.Expect(len(keys)).To(gm.Equal(334))
		// This depends on how many keys end up in the same partition.
		// Since keys are statistically distributed randomly and uniformly,
		// we expect that there aren't many partitions that share more than one key.
		gm.Expect(counter).To(gm.BeNumerically("~", keyCount-334, 50))
	})

	gg.It("must Query and get all partition records back for a specified key and filter", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0

		pf := as.NewPartitionFilterAll()
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin7.Name, 1, 2))
		recordset, err := client.QueryPartitions(queryPolicy, stm, pf)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).NotTo(gm.HaveOccurred())
			gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
			gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

			delete(keys, string(res.Record.Key.Digest()))

			counter++
		}

		gm.Expect(len(keys)).To(gm.Equal(334))
		gm.Expect(counter).To(gm.Equal(keyCount - 334))
	})

	gg.It("must return error on a Query when index is not found", func() {
		pf := as.NewPartitionFilterAll()
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(randString(10), 1, 2))
		recordset, err := client.QueryPartitions(queryPolicy, stm, pf)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
			gm.Expect(res.Err.Matches(ast.INDEX_NOTFOUND)).To(gm.BeTrue())
		}
	})

	gg.It("must Query and get all partition records back for a specified partition range", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		pbegin := 1000
		for i := 1; i < 10; i++ {
			counter := 0

			pf := as.NewPartitionFilterByRange(pbegin, rand.Intn(i*191)+1)
			stm := as.NewStatement(ns, set)
			recordset, err := client.QueryPartitions(queryPolicy, stm, pf)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(res.Record.Key.Digest()))

				counter++

				gm.Expect(counter).To(gm.BeNumerically(">", 0))
				gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
			}
		}
		gm.Expect(len(keys)).To(gm.BeNumerically(">", 0))
	})

	gg.It("must return error if query on non-indexed field", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("Non-Existing", 0, math.MaxInt16/2))

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
		}
	})

	gg.It("must Query a range and get all records back", func() {
		stm := as.NewStatement(ns, set)
		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Query a range and get all records back with policy.RecordsPerSecond set", func() {
		stm := as.NewStatement(ns, set)

		policy := as.NewQueryPolicy()
		policy.RecordsPerSecond = keyCount - 100
		recordset, err := client.Query(policy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Query a range and get all records back without the Bin Data", func() {
		stm := as.NewStatement(ns, set)
		qp := as.NewQueryPolicy()
		qp.IncludeBinData = false
		recordset, err := client.Query(qp, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
			gm.Expect(len(rec.Bins)).To(gm.Equal(0))

			delete(keys, string(rec.Key.Digest()))
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Cancel Query abruptly", func() {
		stm := as.NewStatement(ns, set)
		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, keyCount/2)

		gm.Expect(len(keys)).To(gm.BeNumerically("<=", keyCount/2))
	})

	gg.It("must Query a specific range and get only relevant records back", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			cnt++
			_, exists := keys[string(rec.Key.Digest())]
			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(rec.Bins[bin3.Name]).To(gm.BeNumerically("<=", math.MaxInt16/2))
		}

		gm.Expect(cnt).To(gm.BeNumerically(">", 0))
	})

	gg.It("must Query a specific range by applying a udf filter and get only relevant records back", func() {
		regTask, err := nativeClient.RegisterUDF(nil, []byte(udfFilter), "udfFilter.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until UDF is created
		err = <-regTask.OnComplete()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		stm.SetAggregateFunction("udfFilter", "filter_by_name", []as.Value{as.NewValue("Aeropsike")}, true)

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			results := rec.Bins["SUCCESS"].(map[interface{}]interface{})
			gm.Expect(results["bin4"]).To(gm.Equal("constValue"))
			// gm.Expect(results["bin5"]).To(gm.Equal(-1))
			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically(">", 0))
	})

	gg.It("must Query specific equality filters and get only relevant records back", func() {
		// save a record with requested value
		key, err := as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		bin3 := as.NewBin("Aerospike3", rand.Intn(math.MaxInt16))
		err = client.PutBins(wpolicy, key, bin3)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stm := as.NewStatement(ns, set, bin3.Name)
		stm.SetFilter(as.NewEqualFilter(bin3.Name, bin3.Value))

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		recs := []interface{}{}
		// consume recordset and check errors
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())
			recs = append(recs, rec.Bins[bin3.Name])
		}

		// there should be at least one result
		gm.Expect(len(recs)).To(gm.BeNumerically(">", 0))
		gm.Expect(recs).To(gm.ContainElement(bin3.Value.GetObject()))
	})

	gg.It("must Query specific equality filters and apply operations on the records", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

		bin7 := as.NewBin("Aerospike7", 42)
		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin7))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk.OnComplete()).To(gm.BeNil())

		// read records back
		stmRes := as.NewStatement(ns, set)
		recordset, err := client.Query(queryPolicy, stmRes)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		recs := []interface{}{}
		// consume recordset and check errors
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())
			recs = append(recs, rec.Bins[bin3.Name])
			gm.Expect(rec.Bins[bin7.Name]).To(gm.Equal(bin7.Value.GetObject().(int)))
		}

		// there should be at least one result
		gm.Expect(len(recs)).To(gm.Equal(keyCount))
	})

	gg.It("must handle a Query on a non-existing set without timing out", func() {
		stm := as.NewStatement(ns, set+"NON_EXISTING")

		bin7 := as.NewBin("Aerospike7", 42)
		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin7))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk.OnComplete()).To(gm.BeNil())

		rs, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		cnt := 0
		for res := range rs.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}
		gm.Expect(cnt).To(gm.Equal(0))
	})

	gg.It("must return an error if read operations are requested in a background query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

		bin7 := as.NewBin("Aerospike7", 42)
		_, err := client.QueryExecute(queryPolicy, nil, stm, as.GetBinOp(bin7.Name))
		gm.Expect(err).To(gm.HaveOccurred())

		var typedErr *as.AerospikeError
		isAsErr := errors.As(err, &typedErr)
		gm.Expect(isAsErr).To(gm.BeTrue())
		gm.Expect(typedErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must Query specific equality filters and apply operations on the records without filters", func() {
		stm := as.NewStatement(ns, set)

		bin7 := as.NewBin("Aerospike7", 42)
		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin7))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk.OnComplete()).To(gm.BeNil())

		// read records back
		stmRes := as.NewStatement(ns, set)
		recordset, err := client.Query(queryPolicy, stmRes)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		recs := []interface{}{}
		// consume recordset and check errors
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())
			recs = append(recs, rec.Bins[bin3.Name])
			gm.Expect(rec.Bins[bin7.Name]).To(gm.Equal(bin7.Value.GetObject().(int)))
		}

		// there should be at least one result
		gm.Expect(len(recs)).To(gm.Equal(keyCount))
	})

	gg.It("must return the error for invalid expression", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))

		queryPolicy := as.NewQueryPolicy()
		queryPolicy.FilterExpression = as.ExpEq(as.ExpListGetByValueRange(as.ListReturnTypeValue, as.ExpIntVal(10), as.ExpIntVal(13), as.ExpListBin(bin1.Name)), as.ExpIntVal(11))

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
			gm.Expect(res.Err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
		}
	})

})

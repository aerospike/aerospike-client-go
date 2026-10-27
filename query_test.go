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
	"sort"
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

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
		client.Truncate(nil, ns, set, nil)

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
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())

		indexName = set + bin6.Name
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())

		indexName = set + bin7.Name
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())
	})

	var queryPolicy = as.NewQueryPolicy()

	gg.It("must Query and get all records back for a specified node using Results() channel", func() {
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
		regTask, err := client.RegisterUDF(nil, []byte(udfFilter), "udfFilter.lua", as.LUA)
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
			results := rec.Bins["SUCCESS"].(map[any]any)
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

		recs := []any{}
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

		recs := []any{}
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

		recs := []any{}
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

	gg.It("must not return server errors when QueryExecute is called with BinNames populated", func() {
		// Create a statement with specific bin names
		stm := as.NewStatement(ns, set, bin1.Name, bin3.Name, bin4.Name)
		stm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

		bin8 := as.NewBin("Aerospike8", "BinNamesTest")

		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin8))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(tsk).ToNot(gm.BeNil())

		completionErr := <-tsk.OnComplete()
		gm.Expect(completionErr).To(gm.BeNil())

		// Verify the operation was successful by reading back the records
		// and checking that the new bin was added
		verifyStm := as.NewStatement(ns, set)
		verifyStm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

		recordset, err := client.Query(queryPolicy, verifyStm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		foundRecords := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())

			gm.Expect(rec.Bins[bin8.Name]).To(gm.Equal(bin8.Value.GetObject()))
			foundRecords++
		}

		// Ensure we found the expected number of records
		gm.Expect(foundRecords).To(gm.Equal(keyCount))
	})

	gg.It("must not return server errors when QueryExecute is called with BinNames in scan mode (no filter)", func() {
		// Create a statement with specific bin names but NO filter (scan mode)
		stm := as.NewStatement(ns, set, bin1.Name, bin2.Name, bin3.Name)

		bin9 := as.NewBin("Aerospike9", "ScanBinNamesTest")

		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(bin9))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(tsk).ToNot(gm.BeNil())

		completionErr := <-tsk.OnComplete()
		gm.Expect(completionErr).To(gm.BeNil())

		// Verify the operation was successful by scanning back all records
		verifyStm := as.NewStatement(ns, set)

		recordset, err := client.Query(queryPolicy, verifyStm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		foundRecords := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())

			gm.Expect(rec.Bins[bin9.Name]).To(gm.Equal(bin9.Value.GetObject()))
			foundRecords++
		}

		// Ensure we found all records in the set
		gm.Expect(foundRecords).To(gm.Equal(keyCount))
	})

	gg.It("must allow reusing the same statement for multiple QueryExecute calls without errors", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewEqualFilter(bin6.Name, 1))

		updateBin := as.NewBin("Aerospike7", 100)

		tsk1, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(updateBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk1.OnComplete()).To(gm.BeNil())

		updateBin2 := as.NewBin("Aerospike7", 200)
		tsk2, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(updateBin2))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk2.OnComplete()).To(gm.BeNil())

		updateBin3 := as.NewBin("Aerospike7", 300)
		tsk3, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(updateBin3))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk3.OnComplete()).To(gm.BeNil())

		// Verify that the last operation was applied correctly by reading records back
		stmRead := as.NewStatement(ns, set)
		recordset, err := client.Query(queryPolicy, stmRead)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		foundRecords := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec).ToNot(gm.BeNil())
			gm.Expect(rec.Bins["Aerospike7"]).To(gm.Equal(300))
			foundRecords++
		}

		gm.Expect(foundRecords).To(gm.Equal(keyCount))
	})

	gg.It("must allow reusing the same statement for multiple Query calls without errors", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16))

		recordset1, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count1 := 0
		for res := range recordset1.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			count1++
		}

		recordset2, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count2 := 0
		for res := range recordset2.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			count2++
		}

		recordset3, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count3 := 0
		for res := range recordset3.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			count3++
		}

		// All calls should return the same number of records
		gm.Expect(count1).To(gm.Equal(count2))
		gm.Expect(count2).To(gm.Equal(count3))
		gm.Expect(count1).To(gm.BeNumerically(">", 0))
	})

	gg.It("must validate taskId for background query jobs using query-show info command", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16))

		updateBin := as.NewBin("Aerospike10", 888)

		et, err := client.QueryExecute(queryPolicy, nil, stm, as.PutOp(updateBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(et).ToNot(gm.BeNil())

		// Get and validate task ID
		taskId := et.TaskId()
		gm.Expect(taskId).To(gm.BeNumerically(">", 0))

		// Query all nodes to verify the task ID is actually valid and tracked by the server
		nodes := client.GetNodes()
		gm.Expect(len(nodes)).To(gm.BeNumerically(">", 0))

		taskIdStr := strconv.FormatUint(taskId, 10)
		taskFoundAndValidated := false

		// Check that at least one node has the task tracked with the correct task ID
		for _, node := range nodes {
			ip := as.NewInfoPolicy()

			infoCmd := "query-show:id=" + taskIdStr

			res, err := node.RequestInfo(ip, infoCmd)
			if err == nil {
				response := res[infoCmd]
				// If we don't get ERROR:2, the task exists or existed
				if !bytes.Contains([]byte(response), []byte("ERROR:2")) {
					// Verify response contains expected job information
					gm.Expect(response).To(gm.ContainSubstring("status="))

					// Verify the response contains the task ID we're looking for
					gm.Expect(response).To(gm.Or(
						gm.ContainSubstring("id="+taskIdStr),
						gm.ContainSubstring("trid="+taskIdStr),
					))

					taskFoundAndValidated = true
				}
			}
		}

		gm.Expect(taskFoundAndValidated).To(gm.BeTrue(),
			"Task ID %d should be tracked by at least one server node", taskId)

		gm.Expect(<-et.OnComplete()).To(gm.BeNil())

		// Verify IsDone returns true after completion
		done, err := et.IsDone()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(done).To(gm.BeTrue())

		// After completion, verify that the task ID is still consistent
		taskIdAfterCompletion := et.TaskId()
		gm.Expect(taskIdAfterCompletion).To(gm.Equal(taskId),
			"Task ID should remain consistent before and after completion")

		// After completion, verify that records were actually updated
		verifyStm := as.NewStatement(ns, set)
		verifyStm.SetFilter(as.NewRangeFilter(bin3.Name, 0, math.MaxInt16))
		recordset, err := client.Query(queryPolicy, verifyStm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		updatedCount := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			if rec.Bins["Aerospike10"] == 888 {
				updatedCount++
			}
		}

		// All matching records should have been updated
		gm.Expect(updatedCount).To(gm.BeNumerically(">", 0))
	})
})

var _ = gg.Describe("Query Top-K", func() {
	var (
		ns   = *namespace
		set  string
		keys []*as.Key
	)

	gg.BeforeEach(func() {
		set = randString(50)
		keys = nil
	})

	gg.AfterEach(func() {
		for _, key := range keys {
			client.Delete(nil, key)
		}
	})

	expectParameterError := func(err as.Error, messagePart string) {
		gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
		gm.ExpectWithOffset(1, err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
		gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring(messagePart))
	}

	// putScores stores one record per score, with its position as the id.
	putScores := func(scores ...int) {
		for id, score := range scores {
			key, err := as.NewKey(ns, set, id)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(client.PutBins(nil, key, as.NewBin("score", score), as.NewBin("id", id))).To(gm.Succeed())
			keys = append(keys, key)
		}
	}

	// rankByScore returns a statement ordered by the highest score, returning
	// the score and id bins.
	rankByScore := func() *as.Statement {
		statement := as.NewStatement(ns, set, "score", "id")
		gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
		return statement
	}

	// topTen returns a statement for the 10 highest scores.
	topTen := func() *as.Statement {
		statement := rankByScore()
		gm.Expect(statement.SetTopK(10)).To(gm.Succeed())
		return statement
	}

	queryIDs := func(statement *as.Statement) []int {
		recordset, err := client.Query(nil, statement)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		var ids []int
		for result := range recordset.Results() {
			gm.Expect(result.Err).ToNot(gm.HaveOccurred())
			ids = append(ids, result.Record.Bins["id"].(int))
		}
		return ids
	}

	gg.Context("invalid requests", func() {
		gg.It("refuses SetTopK before SetOrderBy", func() {
			expectParameterError(as.NewStatement(ns, set).SetTopK(10), "SetTopK requires SetOrderBy")
		})

		gg.It("refuses ThenOrderBy before SetOrderBy", func() {
			expectParameterError(as.NewStatement(ns, set).ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc), "ThenOrderBy requires SetOrderBy")
		})

		gg.It("refuses a third order-by bin", func() {
			statement := topTen()
			gm.Expect(statement.ThenOrderBy("id", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())
			expectParameterError(statement.ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc), "order-by names more than 2 bins")
		})

		invalid := []struct {
			name        string
			change      func(statement *as.Statement, policy *as.QueryPolicy)
			messagePart string
		}{
			{"a limit of 0", func(s *as.Statement, _ *as.QueryPolicy) { s.SetTopK(0) }, "top-k limit 0 out of range - must be 1 to 1000"},
			{"a limit over 1000", func(s *as.Statement, _ *as.QueryPolicy) { s.SetTopK(1001) }, "top-k limit 1001 out of range - must be 1 to 1000"},
			{"an order-by bin name over 15 bytes", func(s *as.Statement, _ *as.QueryPolicy) {
				s.BinNames = append(s.BinNames, "a_bin_name_16_by")
				s.SetOrderBy("a_bin_name_16_by", as.BinDataTypeInteger, as.OrderAsc)
			}, "order-by bin name length 16 invalid - must be 1 to 15"},
			{"an order-by bin name with a null byte", func(s *as.Statement, _ *as.QueryPolicy) {
				s.BinNames = append(s.BinNames, "sc\x00re")
				s.SetOrderBy("sc\x00re", as.BinDataTypeInteger, as.OrderAsc)
			}, "order-by bin name is not a valid bin name"},
			{"an unknown order-by type", func(s *as.Statement, _ *as.QueryPolicy) {
				s.SetOrderBy("score", as.BinDataType(9), as.OrderAsc)
			}, "order-by type 9 invalid - must be 1 (integer), 2 (double), 3 (string) or 4 (bytes)"},
			{"an unknown order-by direction", func(s *as.Statement, _ *as.QueryPolicy) {
				s.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDirection(5))
			}, "order-by direction 5 invalid - must be 0 (ascending) or 1 (descending)"},
			{"unknown order-by flags", func(s *as.Statement, _ *as.QueryPolicy) {
				s.SetOrderByWithFlags("score", as.BinDataTypeString, as.OrderAsc, as.OrderByFlags(4))
			}, "order-by flags 0x4 invalid - only 0x1 (case-insensitive) is defined"},
			{"case folding on a non-string bin", func(s *as.Statement, _ *as.QueryPolicy) {
				s.SetOrderByWithFlags("score", as.BinDataTypeInteger, as.OrderAsc, as.OrderByFlagCaseInsensitive)
				s.SetTopK(10)
			}, "order-by case-insensitive flag applies only to string ordering"},
			{"an order-by bin missing from BinNames", func(s *as.Statement, _ *as.QueryPolicy) {
				s.BinNames = []string{"id"}
			}, "order-by bin 'score' is not in the query's bin list - add it to the bins selected"},
			{"an order-by bin no operation returns", func(s *as.Statement, _ *as.QueryPolicy) {
				s.Operations = []*as.Operation{as.GetBinOp("id")}
			}, "order-by bin 'score' is not produced by any of the query's read ops"},
			{"a repeated order-by bin", func(s *as.Statement, _ *as.QueryPolicy) {
				s.ThenOrderBy("score", as.BinDataTypeInteger, as.OrderAsc)
			}, "order-by names bin 'score' more than once"},
			{"case folding on a second non-string bin", func(s *as.Statement, _ *as.QueryPolicy) {
				s.ThenOrderByWithFlags("id", as.BinDataTypeInteger, as.OrderAsc, as.OrderByFlagCaseInsensitive)
			}, "order-by case-insensitive flag applies only to string ordering"},
			{"a second order-by bin missing from BinNames", func(s *as.Statement, _ *as.QueryPolicy) {
				s.ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc)
			}, "order-by bin 'ts' is not in the query's bin list - add it to the bins selected"},
			{"a second order-by bin name over 15 bytes", func(s *as.Statement, _ *as.QueryPolicy) {
				s.BinNames = append(s.BinNames, "a_bin_name_16_by")
				s.ThenOrderBy("a_bin_name_16_by", as.BinDataTypeInteger, as.OrderAsc)
			}, "order-by bin name length 16 invalid - must be 1 to 15"},
			{"an unknown second order-by direction", func(s *as.Statement, _ *as.QueryPolicy) {
				s.ThenOrderBy("id", as.BinDataTypeInteger, as.OrderDirection(5))
			}, "order-by direction 5 invalid - must be 0 (ascending) or 1 (descending)"},
			{"an aggregate function", func(s *as.Statement, _ *as.QueryPolicy) {
				s.SetAggregateFunction("pkg", "fn", nil, true)
			}, "top-k/order-by only valid on foreground (basic) queries"},
			{"MaxRecords", func(_ *as.Statement, p *as.QueryPolicy) { p.MaxRecords = 100 }, "top-k incompatible with max-records/sample-max"},
			{"IncludeBinData off", func(_ *as.Statement, p *as.QueryPolicy) { p.IncludeBinData = false }, "top-k incompatible with no-bin-data (metadata-only) queries"},
			{"an order-by bin two operations return", func(s *as.Statement, _ *as.QueryPolicy) {
				s.Operations = []*as.Operation{
					as.ExpReadOp("score", as.ExpIntBin("price"), as.ExpReadFlagDefault),
					as.ExpReadOp("score", as.ExpIntBin("rating"), as.ExpReadFlagDefault),
				}
			}, "Top-K order-by bin must be produced by exactly one projection operation"},
			{"a second order-by bin two operations return", func(s *as.Statement, _ *as.QueryPolicy) {
				s.ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc)
				s.Operations = []*as.Operation{
					as.GetBinOp("score"),
					as.ExpReadOp("ts", as.ExpIntBin("created"), as.ExpReadFlagDefault),
					as.ExpReadOp("ts", as.ExpIntBin("updated"), as.ExpReadFlagDefault),
				}
			}, "Top-K then-order-by bin must be produced by exactly one projection operation"},
		}

		for _, testCase := range invalid {
			gg.It("refuses "+testCase.name+" before contacting the cluster", func() {
				statement, policy := topTen(), as.NewQueryPolicy()
				testCase.change(statement, policy)

				_, err := client.Query(policy, statement)
				expectParameterError(err, testCase.messagePart)
			})
		}

		// A filter that resumes an earlier query would make Top-K rank only the
		// records after each resume point.
		resumedFilters := []struct {
			name   string
			filter func() *as.PartitionFilter
		}{
			{"a key's digest", func() *as.PartitionFilter {
				resumeKey, err := as.NewKey(ns, set, 1)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				return as.NewPartitionFilterByKey(resumeKey)
			}},
			{"the last digest of a paged query", func() *as.PartitionFilter {
				putScores(10, 20, 30)
				filter := as.NewPartitionFilterAll()
				policy := as.NewQueryPolicy()
				policy.MaxRecords = 1
				recordset, err := client.QueryPartitions(policy, as.NewStatement(ns, set), filter)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				for result := range recordset.Results() {
					gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				}

				resumed := 0
				for _, partition := range filter.Partitions {
					if partition.Digest != nil {
						resumed++
					}
				}
				gm.Expect(resumed).To(gm.Equal(1))
				return filter
			}},
			{"a partition's last index value", func() *as.PartitionFilter {
				filter := as.NewPartitionFilterById(0)
				filter.Partitions = []*as.PartitionStatus{{Id: 0, BVal: 7}}
				return filter
			}},
		}

		for _, resumed := range resumedFilters {
			gg.It("refuses a partition filter that resumes from "+resumed.name+" before contacting the cluster", func() {
				// A cluster without Top-K refuses any Top-K query first.
				if !client.GetNodes()[0].SupportsQueryOrderBy() {
					gg.Skip("Top-K ranking requires a server with Top-K support")
				}

				recordset, err := client.QueryPartitions(nil, topTen(), resumed.filter())
				gm.Expect(recordset).To(gm.BeNil())
				expectParameterError(err, "Top-K cannot use a partition filter that carries resume points from an earlier query")
			})
		}
	})

	gg.Context("commands without Top-K support", func() {
		gg.It("are refused for a Top-K statement", func() {
			_, err := client.QueryExecute(nil, nil, topTen(), as.PutOp(as.NewBin("seen", 1)))
			expectParameterError(err, "only supported by Query, QueryPartitions and QueryNode")

			_, err = client.ExecuteUDF(nil, topTen(), "pkg", "fn")
			expectParameterError(err, "only supported by Query, QueryPartitions and QueryNode")
		})
	})

	gg.Context("on a cluster where a node lacks Top-K support", func() {
		// A client of its own, so the shared client's view of the cluster is
		// never changed.
		var mixedCluster *as.Client

		gg.BeforeEach(func() {
			var err as.Error
			mixedCluster, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			mixedCluster.Cluster().SetQueryOrderBySupportForTest(false)
		})

		gg.AfterEach(func() {
			mixedCluster.Close()
		})

		// expectRefused asserts the query fails before it starts: no recordset
		// is returned, so nothing is sent.
		expectRefused := func(recordset *as.Recordset, err as.Error) {
			gm.ExpectWithOffset(1, recordset).To(gm.BeNil())
			expectParameterError(err, "Top-K is not supported by all nodes in the cluster")
		}

		gg.It("refuses Top-K through Query, QueryPartitions and QueryNode before anything is sent", func() {
			expectRefused(mixedCluster.Query(nil, topTen()))
			expectRefused(mixedCluster.QueryPartitions(nil, topTen(), as.NewPartitionFilterAll()))
			expectRefused(mixedCluster.QueryNode(nil, mixedCluster.GetNodes()[0], topTen()))
		})

		gg.It("refuses an order-by without SetTopK", func() {
			expectRefused(mixedCluster.Query(nil, rankByScore()))
		})

		gg.It("still runs a query without Top-K settings", func() {
			putScores(1, 5, 3)

			recordset, err := mixedCluster.Query(nil, as.NewStatement(ns, set, "score", "id"))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			var ids []int
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				ids = append(ids, result.Record.Bins["id"].(int))
			}
			gm.Expect(ids).To(gm.ConsistOf(0, 1, 2))
		})
	})

	gg.Context("ranking", func() {
		// Records by id: score (one missing, one a string), and a vector
		// {id, 9-id} so distances order cleanly by id.
		scores := []any{50, 90, 70, 90, 10, nil, 30, 70, "x", 60}
		var keyByID []*as.Key

		gg.BeforeEach(func() {
			if !client.GetNodes()[0].SupportsQueryOrderBy() {
				gg.Skip("Top-K ranking requires a server with Top-K support")
			}

			keyByID = make([]*as.Key, len(scores))
			for id, score := range scores {
				key, err := as.NewKey(ns, set, id)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				bins := []*as.Bin{
					as.NewBin("id", id),
					as.NewBin("emb", mustVector(as.NewVectorFloat32([]float32{float32(id), float32(9 - id)}))),
				}
				if score != nil {
					bins = append(bins, as.NewBin("score", score))
				}
				gm.Expect(client.PutBins(nil, key, bins...)).To(gm.Succeed())
				keyByID[id] = key
				keys = append(keys, key)
			}
		})

		topK := func(direction as.OrderDirection, k int) *as.Statement {
			statement := as.NewStatement(ns, set, "score", "id")
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, direction)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(k)).To(gm.Succeed())
			return statement
		}

		// expectTie asserts two tied records come back in digest order.
		expectTie := func(first, second int) {
			gm.ExpectWithOffset(1, bytes.Compare(keyByID[first].Digest(), keyByID[second].Digest())).To(gm.Equal(-1))
		}

		gg.It("returns the k highest values, best first, with ties in digest order", func() {
			ids := queryIDs(topK(as.OrderDesc, 3))
			gm.Expect(ids).To(gm.HaveLen(3))
			gm.Expect(ids[:2]).To(gm.ConsistOf(1, 3))
			gm.Expect(ids[2]).To(gm.BeElementOf(2, 7))
			expectTie(ids[0], ids[1])
		})

		gg.It("returns the k lowest values, best first", func() {
			gm.Expect(queryIDs(topK(as.OrderAsc, 3))).To(gm.Equal([]int{4, 6, 0}))
		})

		gg.It("ranks missing and wrong-type values last and returns at most the matching records", func() {
			ids := queryIDs(topK(as.OrderDesc, 1000))
			gm.Expect(ids).To(gm.HaveLen(len(scores)))
			gm.Expect(ids[:8]).To(gm.ConsistOf(1, 3, 2, 7, 9, 0, 6, 4))
			gm.Expect(ids[8:]).To(gm.ConsistOf(5, 8))
		})

		gg.It("ranks the same with BinNames, Operations or the whole record", func() {
			want := queryIDs(topK(as.OrderDesc, 3))

			withOperations := topK(as.OrderDesc, 3)
			withOperations.BinNames = nil
			withOperations.Operations = []*as.Operation{as.GetBinOp("score"), as.GetBinOp("id")}
			gm.Expect(queryIDs(withOperations)).To(gm.Equal(want))

			wholeRecord := topK(as.OrderDesc, 3)
			wholeRecord.BinNames = nil
			gm.Expect(queryIDs(wholeRecord)).To(gm.Equal(want))
		})

		gg.It("ranks only the records a filter expression keeps", func() {
			policy := as.NewQueryPolicy()
			policy.FilterExpression = as.ExpLess(as.ExpIntBin("id"), as.ExpIntVal(3))

			recordset, err := client.Query(policy, topK(as.OrderDesc, 2))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			var ids []int
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				ids = append(ids, result.Record.Bins["id"].(int))
			}
			gm.Expect(ids).To(gm.Equal([]int{1, 2}))
		})

		gg.It("ranks only the records a secondary index filter matches", func() {
			indexName := set + "_id"
			createIndex(nil, ns, set, indexName, "id", as.NUMERIC)
			defer dropIndex(nil, ns, set, indexName)

			statement := topK(as.OrderDesc, 2)
			gm.Expect(statement.SetFilter(as.NewRangeFilter("id", 4, 9))).To(gm.Succeed())
			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{7, 9}))
		})

		gg.It("ranks by a projected vector distance with each metric", func() {
			nearest := func(metric as.VectorDistanceMetric, query []float32, direction as.OrderDirection, k int) []int {
				statement := as.NewStatement(ns, set)
				statement.Operations = []*as.Operation{
					as.GetBinOp("id"),
					as.ExpReadOp("dist", as.ExpVectorDistance(metric, mustVector(as.NewVectorFloat32(query)), as.ExpVectorBin("emb")), as.ExpReadFlagEvalNoFail),
				}
				gm.Expect(statement.SetOrderBy("dist", as.BinDataTypeDouble, direction)).To(gm.Succeed())
				gm.Expect(statement.SetTopK(k)).To(gm.Succeed())
				return queryIDs(statement)
			}

			gm.Expect(nearest(as.VectorEuclideanSquared, []float32{0, 9}, as.OrderAsc, 3)).To(gm.Equal([]int{0, 1, 2}))
			gm.Expect(nearest(as.VectorDotProduct, []float32{1, 0}, as.OrderDesc, 3)).To(gm.Equal([]int{9, 8, 7}))
			gm.Expect(nearest(as.VectorCosineSimilarity, []float32{1, 0}, as.OrderDesc, 3)).To(gm.Equal([]int{9, 8, 7}))
		})

		gg.It("returns a record missing the order-by bin last when the query returns only that bin", func() {
			byBinName := func() *as.Statement { return as.NewStatement(ns, set, "score") }
			byOperation := func() *as.Statement {
				statement := as.NewStatement(ns, set)
				statement.Operations = []*as.Operation{as.GetBinOp("score")}
				return statement
			}

			for _, newStatement := range []func() *as.Statement{byBinName, byOperation} {
				statement := newStatement()
				gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
				gm.Expect(statement.SetTopK(len(scores))).To(gm.Succeed())

				recordset, err := client.Query(nil, statement)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				var ranked []any
				for result := range recordset.Results() {
					gm.Expect(result.Err).ToNot(gm.HaveOccurred())
					ranked = append(ranked, result.Record.Bins["score"])
				}

				// The record with no score and the one with a string score rank last.
				gm.Expect(ranked).To(gm.HaveLen(len(scores)))
				gm.Expect(ranked[:8]).To(gm.Equal([]any{90, 90, 70, 70, 60, 50, 30, 10}))
				gm.Expect(ranked[8:]).To(gm.ConsistOf(nil, "x"))
			}
		})

		gg.It("returns a record whose distance can't be computed last when the query returns only the distance", func() {
			for id, bins := range map[int]as.BinMap{
				99:  {"emb": mustVector(as.NewVectorFloat32([]float32{0, 9, 0}))},
				100: {"other": 1},
			} {
				key, err := as.NewKey(ns, set, id)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(client.Put(nil, key, bins)).To(gm.Succeed())
				keys = append(keys, key)
			}

			distances := func(flags as.ExpReadFlags) []any {
				statement := as.NewStatement(ns, set)
				statement.Operations = []*as.Operation{
					as.ExpReadOp("dist", as.ExpVectorDistance(as.VectorEuclideanSquared, mustVector(as.NewVectorFloat32([]float32{0, 9})), as.ExpVectorBin("emb")), flags),
				}
				gm.Expect(statement.SetOrderBy("dist", as.BinDataTypeDouble, as.OrderAsc)).To(gm.Succeed())
				gm.Expect(statement.SetTopK(1000)).To(gm.Succeed())

				recordset, err := client.Query(nil, statement)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				var ranked []any
				for result := range recordset.Results() {
					gm.Expect(result.Err).ToNot(gm.HaveOccurred())
					ranked = append(ranked, result.Record.Bins["dist"])
				}
				return ranked
			}

			// Record i holds {i, 9-i}, so its distance to {0, 9} is 2*i*i.
			nearestFirst := []any{0.0, 2.0, 8.0, 18.0, 32.0, 50.0, 72.0, 98.0, 128.0, 162.0}

			withEvalNoFail := distances(as.ExpReadFlagEvalNoFail)
			gm.Expect(withEvalNoFail).To(gm.HaveLen(len(nearestFirst) + 2))
			gm.Expect(withEvalNoFail[:len(nearestFirst)]).To(gm.Equal(nearestFirst))
			gm.Expect(withEvalNoFail[len(nearestFirst):]).To(gm.Equal([]any{nil, nil}))

			gm.Expect(distances(as.ExpReadFlagDefault)).To(gm.Equal(nearestFirst))
		})

		gg.It("ranks a query sent with a short-query hint", func() {
			policy := as.NewQueryPolicy()
			policy.ExpectedDuration = as.SHORT

			recordset, err := client.Query(policy, topK(as.OrderAsc, 3))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			var ids []int
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				ids = append(ids, result.Record.Bins["id"].(int))
			}
			gm.Expect(ids).To(gm.Equal([]int{4, 6, 0}))
		})

		gg.It("ranks only records within the partition filter", func() {
			// Keys land in partitions by digest, so the range is taken from the data:
			// partitions 0 up to the fifth-lowest partition holding a score.
			var scoredPartitions []int
			for id, score := range scores {
				if _, ok := score.(int); ok {
					scoredPartitions = append(scoredPartitions, keyByID[id].PartitionId())
				}
			}
			sort.Ints(scoredPartitions)
			begin, count := 0, scoredPartitions[4]+1

			inRange := func(id int) bool {
				partition := keyByID[id].PartitionId()
				return partition >= begin && partition < begin+count
			}

			// The highest score must be outside the queried partition range: pick
			// a key whose partition is past the range.
			var outsideKey *as.Key
			for attempt := 0; outsideKey == nil || outsideKey.PartitionId() < count; attempt++ {
				var err as.Error
				outsideKey, err = as.NewKey(ns, set, "outside the range "+strconv.Itoa(attempt))
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
			gm.Expect(client.PutBins(nil, outsideKey,
				as.NewBin("score", 1000),
				as.NewBin("id", 1000),
			)).To(gm.Succeed())
			keys = append(keys, outsideKey)

			var want []int
			for id, score := range scores {
				if value, ok := score.(int); ok && inRange(id) {
					want = append(want, value)
				}
			}

			sort.Sort(sort.Reverse(sort.IntSlice(want)))
			want = want[:min(3, len(want))]

			recordset, err := client.QueryPartitions(
				nil,
				topK(as.OrderDesc, 3),
				as.NewPartitionFilterByRange(begin, count),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			var got []int
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())

				id := result.Record.Bins["id"].(int)
				gm.Expect(id).ToNot(gm.Equal(1000))
				gm.Expect(inRange(id)).To(gm.BeTrue())

				got = append(got, result.Record.Bins["score"].(int))
			}

			gm.Expect(got).To(gm.Equal(want))
		})

		gg.It("ranks through QueryPartitions and per node through QueryNode", func() {
			recordset, err := client.QueryPartitions(nil, topK(as.OrderAsc, 3), as.NewPartitionFilterAll())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			var ids []int
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				ids = append(ids, result.Record.Bins["id"].(int))
			}
			gm.Expect(ids).To(gm.Equal([]int{4, 6, 0}))

			var fromNodes []int
			for _, node := range client.GetNodes() {
				recordset, err := client.QueryNode(nil, node, topK(as.OrderAsc, 3))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				var nodeScores []int
				for result := range recordset.Results() {
					gm.Expect(result.Err).ToNot(gm.HaveOccurred())
					fromNodes = append(fromNodes, result.Record.Bins["id"].(int))
					if score, ok := result.Record.Bins["score"].(int); ok {
						nodeScores = append(nodeScores, score)
					}
				}
				gm.Expect(len(nodeScores)).To(gm.BeNumerically("<=", 3))
				gm.Expect(sort.IntsAreSorted(nodeScores)).To(gm.BeTrue())
			}
			gm.Expect(fromNodes).To(gm.ContainElements(4, 6, 0))
		})

		// Top-K records arrive best first, not in digest order, so a digest
		// cursor would make a retried partition skip candidates.
		gg.It("leaves no resume cursor after a Top-K query, unlike a plain query", func() {
			runToEnd := func(statement *as.Statement) *as.PartitionFilter {
				filter := as.NewPartitionFilterAll()
				recordset, err := client.QueryPartitions(nil, statement, filter)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				for result := range recordset.Results() {
					gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				}
				return filter
			}
			withCursor := func(filter *as.PartitionFilter) int {
				count := 0
				for _, partition := range filter.Partitions {
					if partition.Digest != nil {
						count++
					}
				}
				return count
			}

			topKFilter := runToEnd(topK(as.OrderDesc, 3))
			gm.Expect(topKFilter.Partitions).ToNot(gm.BeEmpty())
			gm.Expect(withCursor(topKFilter)).To(gm.BeZero())

			gm.Expect(withCursor(runToEnd(as.NewStatement(ns, set, "score", "id")))).To(gm.BeNumerically(">", 0))
		})
	})

	// Each order-by type compares values the way the server does: a value of
	// another type ranks last, doubles put NaN above every number, blobs and
	// strings compare byte by byte with the shorter first on a common prefix,
	// and strings can ignore ASCII letter case.
	gg.Context("how each order-by type compares values", func() {
		gg.BeforeEach(func() {
			if !client.GetNodes()[0].SupportsQueryOrderBy() {
				gg.Skip("Top-K ranking requires a server with Top-K support")
			}
		})

		// putValues stores one record per value in bin "v", with its position as the id.
		putValues := func(values ...any) {
			for id, value := range values {
				key, err := as.NewKey(ns, set, id)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(client.PutBins(nil, key, as.NewBin("id", id), as.NewBin("v", value))).To(gm.Succeed())
				keys = append(keys, key)
			}
		}

		rank := func(orderType as.BinDataType, direction as.OrderDirection, flags as.OrderByFlags, k int) []int {
			statement := as.NewStatement(ns, set, "v", "id")
			gm.Expect(statement.SetOrderByWithFlags("v", orderType, direction, flags)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(k)).To(gm.Succeed())
			return queryIDs(statement)
		}

		gg.It("ranks doubles with NaN above every number and an integer value last", func() {
			putValues(1.5, -2.0, math.NaN(), 3.25, 7)

			gm.Expect(rank(as.BinDataTypeDouble, as.OrderDesc, as.OrderByFlagNone, 3)).To(gm.Equal([]int{2, 3, 0}))
			gm.Expect(rank(as.BinDataTypeDouble, as.OrderAsc, as.OrderByFlagNone, 5)).To(gm.Equal([]int{1, 0, 3, 2, 4}))
		})

		gg.It("ranks blobs byte by byte with the shorter first on a common prefix", func() {
			putValues([]byte{0x01, 0x00}, []byte{0x02}, []byte{0x01}, []byte{0x00, 0xff})

			gm.Expect(rank(as.BinDataTypeBytes, as.OrderAsc, as.OrderByFlagNone, 4)).To(gm.Equal([]int{3, 2, 0, 1}))
			gm.Expect(rank(as.BinDataTypeBytes, as.OrderDesc, as.OrderByFlagNone, 4)).To(gm.Equal([]int{1, 0, 2, 3}))
		})

		gg.It("compares strings by bytes by default and ignores ASCII case when asked", func() {
			putValues("Banana", "apple", "Cherry")

			gm.Expect(rank(as.BinDataTypeString, as.OrderAsc, as.OrderByFlagNone, 3)).To(gm.Equal([]int{0, 2, 1}))
			gm.Expect(rank(as.BinDataTypeString, as.OrderAsc, as.OrderByFlagCaseInsensitive, 3)).To(gm.Equal([]int{1, 0, 2}))
			gm.Expect(rank(as.BinDataTypeString, as.OrderDesc, as.OrderByFlagNone, 3)).To(gm.Equal([]int{1, 2, 0}))
			gm.Expect(rank(as.BinDataTypeString, as.OrderDesc, as.OrderByFlagCaseInsensitive, 3)).To(gm.Equal([]int{2, 0, 1}))
		})

		gg.It("ranks the largest and smallest integers at the ends and a string value last", func() {
			putValues(math.MaxInt64, -1, math.MinInt64, 0, "7")

			gm.Expect(rank(as.BinDataTypeInteger, as.OrderAsc, as.OrderByFlagNone, 5)).To(gm.Equal([]int{2, 1, 3, 0, 4}))
			gm.Expect(rank(as.BinDataTypeInteger, as.OrderDesc, as.OrderByFlagNone, 5)).To(gm.Equal([]int{0, 3, 1, 2, 4}))
		})

		gg.It("treats -0.0 as equal to 0.0, so the digest decides their order in either direction", func() {
			putValues(0.0, math.Copysign(0, -1))

			byDigest := []int{0, 1}
			if bytes.Compare(keys[1].Digest(), keys[0].Digest()) < 0 {
				byDigest = []int{1, 0}
			}
			gm.Expect(rank(as.BinDataTypeDouble, as.OrderAsc, as.OrderByFlagNone, 2)).To(gm.Equal(byDigest))
			gm.Expect(rank(as.BinDataTypeDouble, as.OrderDesc, as.OrderByFlagNone, 2)).To(gm.Equal(byDigest))
		})

		gg.It("ranks by an integer key, then by a string key that ignores case", func() {
			for id, bins := range []as.BinMap{
				{"score": 5, "name": "Banana"},
				{"score": 5, "name": "apple"},
				{"score": 9, "name": "cherry"},
				{"score": 1, "name": "aardvark"},
			} {
				key, err := as.NewKey(ns, set, id)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				bins["id"] = id
				gm.Expect(client.Put(nil, key, bins)).To(gm.Succeed())
				keys = append(keys, key)
			}

			statement := as.NewStatement(ns, set, "score", "name", "id")
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
			gm.Expect(statement.ThenOrderByWithFlags("name", as.BinDataTypeString, as.OrderAsc, as.OrderByFlagCaseInsensitive)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(4)).To(gm.Succeed())

			// By bytes "Banana" sorts before "apple"; ignoring case, "apple" comes first.
			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{2, 1, 0, 3}))
		})
	})

	gg.Context("repeated setters", func() {
		gg.BeforeEach(func() {
			if !client.GetNodes()[0].SupportsQueryOrderBy() {
				gg.Skip("Top-K ranking requires a server with Top-K support")
			}
			putScores(1, 5, 3)
		})

		gg.It("keep the limit when SetOrderBy is called again", func() {
			statement := topTen()
			gm.Expect(statement.SetTopK(1)).To(gm.Succeed())
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())

			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{0}))
		})

		gg.It("replace the limit when SetTopK is called again", func() {
			statement := topTen()
			gm.Expect(statement.SetTopK(2)).To(gm.Succeed())

			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{1, 2}))
		})

		gg.It("replace both order-by bins when SetOrderBy is called again", func() {
			statement := topTen()
			gm.Expect(statement.ThenOrderBy("id", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())

			// With the second bin gone, a later ThenOrderBy on it is no repeat.
			gm.Expect(statement.ThenOrderBy("id", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{0, 2, 1}))
		})
	})

	// The server rejects a Top-K request it cannot run with a parameter error,
	// subcode 12 and a message naming the rule. The client's own checks stop
	// most of these first, so all but the order-by without a limit are sent
	// with those checks skipped.
	gg.Context("requests the server rejects", func() {
		gg.BeforeEach(func() {
			if !client.GetNodes()[0].SupportsQueryOrderBy() {
				gg.Skip("Top-K fields are sent only to a server with Top-K support")
			}
			putScores(1, 5, 3)
		})

		// withErrorDetails returns a query policy that asks for subcodes and messages.
		withErrorDetails := func() *as.QueryPolicy {
			policy := as.NewQueryPolicy()
			policy.ErrorDetailVerbosity = 2
			return policy
		}

		expectTopKRejected := func(recordset *as.Recordset, serverMessage string) {
			var queryErr as.Error
			records := 0
			for result := range recordset.Results() {
				if result.Err != nil {
					if queryErr == nil {
						queryErr = result.Err
					}
				} else {
					records++
				}
			}
			gm.ExpectWithOffset(1, records).To(gm.BeZero())
			gm.ExpectWithOffset(1, queryErr).To(gm.HaveOccurred())

			aerospikeErr := &as.AerospikeError{}
			gm.ExpectWithOffset(1, errors.As(queryErr, &aerospikeErr)).To(gm.BeTrue())
			gm.ExpectWithOffset(1, aerospikeErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
			gm.ExpectWithOffset(1, aerospikeErr.SubCode).To(gm.Equal(ast.SubCodeParamTopKInvalid))
			gm.ExpectWithOffset(1, aerospikeErr.ServerMessage).To(gm.ContainSubstring(serverMessage))
		}

		gg.It("refuses an order-by without SetTopK", func() {
			recordset, err := client.Query(withErrorDetails(), rankByScore())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			expectTopKRejected(recordset, "top-k needs both order-by and top-k fields")
		})

		// Each case starts from topTen (score DESC, k = 10, bins score and id)
		// and breaks one rule the server checks. An invalidation is the edit
		// that breaks it, applied to the case's statement or policy.
		type invalidation func(statement *as.Statement, policy *as.QueryPolicy)

		limit := func(k int) invalidation {
			return func(statement *as.Statement, _ *as.QueryPolicy) { statement.SetTopK(k) }
		}
		orderBy := func(binName string, orderType as.BinDataType, direction as.OrderDirection, flags as.OrderByFlags) invalidation {
			return func(statement *as.Statement, _ *as.QueryPolicy) {
				statement.SetOrderByWithFlags(binName, orderType, direction, flags)
			}
		}
		thenOrderBy := func(binName string) invalidation {
			return func(statement *as.Statement, _ *as.QueryPolicy) {
				statement.ThenOrderBy(binName, as.BinDataTypeInteger, as.OrderAsc)
			}
		}
		binNames := func(names ...string) invalidation {
			return func(statement *as.Statement, _ *as.QueryPolicy) { statement.BinNames = names }
		}
		operations := func(ops ...*as.Operation) invalidation {
			return func(statement *as.Statement, _ *as.QueryPolicy) { statement.Operations = ops }
		}
		maxRecords := func(records int64) invalidation {
			return func(_ *as.Statement, policy *as.QueryPolicy) { policy.MaxRecords = records }
		}
		noBinData := func() invalidation {
			return func(_ *as.Statement, policy *as.QueryPolicy) { policy.IncludeBinData = false }
		}

		const (
			integer = as.BinDataTypeInteger
			desc    = as.OrderDesc
			none    = as.OrderByFlagNone
		)

		rejected := []struct {
			name          string
			invalidate    invalidation
			serverMessage string
		}{
			// The limit.
			{"a limit of 0", limit(0), "top-k limit 0 out of range"},
			{"a limit over 1000", limit(1001), "top-k limit 1001 out of range"},

			// Policy settings Top-K cannot run with.
			{"MaxRecords", maxRecords(100), "top-k incompatible with max-records/sample-max"},
			{"IncludeBinData off", noBinData(), "top-k incompatible with no-bin-data"},

			// The order-by spec.
			{"a repeated order-by bin", thenOrderBy("score"), "order-by names bin 'score' more than once"},
			{"an unknown order-by type", orderBy("score", as.BinDataType(9), desc, none), "order-by type 9 invalid"},
			{"an unknown direction", orderBy("score", integer, as.OrderDirection(5), none), "order-by direction 5 invalid"},
			{"unknown flags", orderBy("score", as.BinDataTypeString, desc, as.OrderByFlags(2)), "order-by flags 0x2 invalid"},
			{"case folding on a non-string bin", orderBy("score", integer, desc, as.OrderByFlagCaseInsensitive), "case-insensitive flag applies only to string ordering"},

			// The order-by bin name.
			{"an empty order-by bin name", orderBy("", integer, desc, none), "order-by bin name length 0 invalid"},
			{"an order-by bin name over 15 bytes", orderBy("a_bin_name_16_by", integer, desc, none), "order-by bin name length 16 invalid"},
			{"a null byte in the order-by bin name", orderBy("sc\x00re", integer, desc, none), "order-by bin name is not a valid bin name"},

			// An order-by bin the query does not return. The client sends
			// BinNames as read operations, so the server names those for both.
			{"an order-by bin missing from BinNames", binNames("id"), "order-by bin 'score' is not produced by any of the query's read ops"},
			{"an order-by bin no operation returns", operations(as.GetBinOp("id")), "order-by bin 'score' is not produced by any of the query's read ops"},
		}

		for _, testCase := range rejected {
			gg.It("refuses "+testCase.name, func() {
				statement, policy := topTen(), withErrorDetails()
				testCase.invalidate(statement, policy)

				expectTopKRejected(client.QueryWithoutTopKChecksForTest(policy, statement), testCase.serverMessage)
			})
		}
	})

	// Records tie on score and are ranked by ts, the second order-by bin;
	// records with no score rank last and still by ts.
	gg.Context("ranking by two order-by bins", func() {
		type scoreAndTime struct{ score, ts any }
		records := []scoreAndTime{{90, 5}, {90, 2}, {80, 1}, {90, 9}, {70, 0}, {nil, 3}, {nil, 1}}
		var keyByID []*as.Key

		gg.BeforeEach(func() {
			if !client.GetNodes()[0].SupportsQueryOrderBy() {
				gg.Skip("Top-K ranking requires a server with Top-K support")
			}

			keyByID = make([]*as.Key, len(records))
			for id, record := range records {
				key, err := as.NewKey(ns, set, id)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				bins := []*as.Bin{as.NewBin("id", id), as.NewBin("ts", record.ts)}
				if record.score != nil {
					bins = append(bins, as.NewBin("score", record.score))
				}
				gm.Expect(client.PutBins(nil, key, bins...)).To(gm.Succeed())
				keyByID[id] = key
				keys = append(keys, key)
			}
		})

		scoreThenTime := func(k int) *as.Statement {
			statement := as.NewStatement(ns, set, "score", "ts", "id")
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
			gm.Expect(statement.ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(k)).To(gm.Succeed())
			return statement
		}

		gg.It("breaks ties on the first bin by the second, at the limit too", func() {
			gm.Expect(queryIDs(scoreThenTime(2))).To(gm.Equal([]int{1, 0}))
			gm.Expect(queryIDs(scoreThenTime(len(records)))).To(gm.Equal([]int{1, 0, 3, 2, 4, 6, 5}))
		})

		gg.It("ranks the same when SetTopK comes before ThenOrderBy", func() {
			statement := as.NewStatement(ns, set, "score", "ts", "id")
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(2)).To(gm.Succeed())
			gm.Expect(statement.ThenOrderBy("ts", as.BinDataTypeInteger, as.OrderAsc)).To(gm.Succeed())

			gm.Expect(queryIDs(statement)).To(gm.Equal([]int{1, 0}))
		})

		gg.It("ranks by the new order-by bin alone and uses the last limit after both are set again", func() {
			statement := scoreThenTime(2)
			gm.Expect(statement.SetOrderBy("score", as.BinDataTypeInteger, as.OrderDesc)).To(gm.Succeed())
			gm.Expect(statement.SetTopK(3)).To(gm.Succeed())

			ids := queryIDs(statement)
			gm.Expect(ids).To(gm.HaveLen(3))
			gm.Expect(ids).To(gm.ConsistOf(0, 1, 3))

			// ts no longer breaks the ties on score 90, so they come in digest order.
			gm.Expect(bytes.Compare(keyByID[ids[0]].Digest(), keyByID[ids[1]].Digest())).To(gm.Equal(-1))
			gm.Expect(bytes.Compare(keyByID[ids[1]].Digest(), keyByID[ids[2]].Digest())).To(gm.Equal(-1))
		})
	})
})

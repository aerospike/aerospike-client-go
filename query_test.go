// Copyright 2014-2021 Aerospike, Inc.
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
	"fmt"
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"

	gg "github.com/onsi/ginkgo"
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
	var keys map[string]*as.Key
	var indexName string
	var indexName2 string

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

	var createIndex = func(
		policy *as.WritePolicy,
		namespace string,
		setName string,
		indexName string,
		binName string,
		indexType as.IndexType,
	) {
		idxTask, err := client.CreateIndex(policy, namespace, setName, indexName, binName, indexType)
		if err != nil {
			if ae, ok := err.(ast.AerospikeError); ok && ae.ResultCode() != ast.INDEX_FOUND {
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
			return // index already exists
		}
		// wait until index is created
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())
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
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4, bin5, bin6)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		indexName = set + bin3.Name
		createIndex(wpolicy, ns, set, indexName, bin3.Name, as.NUMERIC)

		// queries only work on indices
		indexName2 = set + bin6.Name
		createIndex(wpolicy, ns, set, indexName2, bin6.Name, as.NUMERIC)
	})

	gg.AfterEach(func() {
		indexName = set + bin3.Name
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())

		indexName = set + bin6.Name
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())
	})

	for _, failOnClusterChange := range []bool{false, true} {
		var queryPolicy = as.NewQueryPolicy()
		queryPolicy.FailOnClusterChange = failOnClusterChange

		gg.It(fmt.Sprintf("must Query and get all records back for a specified node using Results() channelFailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must Query and get all partition records back for a specified key. channelFailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must Query and get all partition records back for a specified partition range. channelFailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must return error if query on non-indexed field. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("Non-Existing", 0, math.MaxInt16/2))

			recordset, err := client.Query(queryPolicy, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).To(gm.HaveOccurred())
			}
		})

		gg.It(fmt.Sprintf("must Query a range and get all records back. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			checkResults(recordset, 0)

			gm.Expect(len(keys)).To(gm.Equal(0))
		})

		gg.It(fmt.Sprintf("must Query a range and get all records back with policy.RecordsPerSecond set. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)

			policy := as.NewQueryPolicy()
			policy.RecordsPerSecond = keyCount - 100
			policy.FailOnClusterChange = queryPolicy.FailOnClusterChange
			recordset, err := client.Query(policy, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			checkResults(recordset, 0)

			gm.Expect(len(keys)).To(gm.Equal(0))
		})

		gg.It(fmt.Sprintf("must Query a range and get all records back without the Bin Data. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)
			qp := as.NewQueryPolicy()
			qp.IncludeBinData = false
			qp.FailOnClusterChange = queryPolicy.FailOnClusterChange
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

		gg.It(fmt.Sprintf("must Cancel Query abruptly. FailOnClusterChange: %v", failOnClusterChange), func() {
			stm := as.NewStatement(ns, set)
			recordset, err := client.Query(queryPolicy, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			checkResults(recordset, keyCount/2)

			gm.Expect(len(keys)).To(gm.BeNumerically("<=", keyCount/2))
		})

		gg.It(fmt.Sprintf("must Query a specific range and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must Query a specific range by applying a udf filter and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
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
			for rec := range recordset.Records {
				results := rec.Bins["SUCCESS"].(map[interface{}]interface{})
				gm.Expect(results["bin4"]).To(gm.Equal("constValue"))
				// gm.Expect(results["bin5"]).To(gm.Equal(-1))
				cnt++
			}

			gm.Expect(cnt).To(gm.BeNumerically(">", 0))
		})

		gg.It(fmt.Sprintf("must Query specific equality filters and get only relevant records back. FailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must Query specific equality filters and apply operations on the records. FailOnClusterChange: %v", failOnClusterChange), func() {
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

		gg.It(fmt.Sprintf("must Query specific equality filters and apply operations on the records without filters. FailOnClusterChange: %v", failOnClusterChange), func() {
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
	}
})

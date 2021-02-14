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
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

const udfBody = `function testFunc1(rec, div)
   local ret = map()                     -- Initialize the return value (a map)

   local x = rec['bin1']                 -- Get the value from record bin named "bin1"

   rec['bin2'] = math.floor(x / div)     -- Set the value in record bin named "bin2"

   aerospike:update(rec)                 -- Update the main record

   ret['status'] = 'OK'                   -- Populate the return status
   return ret                             -- Return the Return value and/or status
end`

const udfDelete = `function deleteRecord(rec)
   aerospike:remove(rec)                   -- Delete main record, Populate the return status
end`

const udfEcho = `function echo(rec, param)
   local ret = map()
   ret['val'] = param
   ret['str_val'] = tostring(param)
   return ret 		-- return the same value to make sure serializations are working well
end`

const udfCreateWithSendKey = `function createRecWithSendKey(rec)
   rec['otherBin'] = 1
   aerospike:create(rec)
end

function getRecordKeyValue(rec)
	local r = record.key(rec)
	aerospike:remove(rec)
	return r
end
`

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("UDF/Query tests", func() {

	var err error
	var ns = *namespace
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)

	const keyCount = 1000
	bin1 := as.NewBin("bin1", rand.Intn(math.MaxInt16))
	bin2 := as.NewBin("bin2", 1)

	gg.It("must Register a UDF", func() {
		regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udf1.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until UDF is created
		gm.Expect(<-regTask.OnComplete()).NotTo(gm.HaveOccurred())
	})

	gg.It("must run a UDF on a single record", func() {
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		err = client.PutBins(wpolicy, key, bin1, bin2)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		wpolicy := as.NewWritePolicy(0, 1000)
		res, err := client.Execute(wpolicy, key, "udf1", "testFunc1", as.NewValue(2))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(res).To(gm.Equal(map[interface{}]interface{}{"status": "OK"}))

		time.Sleep(3 * time.Second)

		// read all data and make sure it is consistent
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Expiration).To(gm.BeNumerically("<=", 997))
		gm.Expect(rec.Expiration).To(gm.BeNumerically(">", 900)) // give a bit of leeway for slow testing VMs

		gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
		gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin1.Value.GetObject().(int) / 2))
	})

	gg.It("must run a UDF to create single record and persist the original key value", func() {
		regTask, err := client.RegisterUDF(wpolicy, []byte(udfCreateWithSendKey), "sendKey.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until UDF is created
		gm.Expect(<-regTask.OnComplete()).NotTo(gm.HaveOccurred())

		tSet := randString(50)
		key, err := as.NewKey(ns, tSet, -1)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// make sure the record doesn't exist yet
		_, err = client.Delete(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		exists, err := client.Exists(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(exists).To(gm.BeFalse())

		wp := as.NewWritePolicy(0, 0)
		wp.SendKey = true
		_, err = client.Execute(wp, key, "sendKey", "createRecWithSendKey")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// read all data and make sure it is consistent
		exists, err = client.Exists(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(exists).To(gm.BeTrue())

		res, err := client.Execute(nil, key, "sendKey", "getRecordKeyValue")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(res).To(gm.Equal(-1))
	})

	gg.It("must list all udfs on the server", func() {
		udfList, err := client.ListUDF(nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(udfList)).To(gm.BeNumerically(">", 0))
	})

	gg.It("must drop a udf on the server", func() {
		regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udfToBeDropped.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until UDF is created
		err = <-regTask.OnComplete()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		delTask, err := client.RemoveUDF(wpolicy, "udfToBeDropped.lua")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until UDF is deleted
		gm.Expect(<-delTask.OnComplete()).NotTo(gm.HaveOccurred())
	})

	gg.Context("must run the UDF on all records", func() {

		for _, failOnClusterChange := range []bool{false, true} {
			var scanPolicy = as.NewScanPolicy()
			scanPolicy.FailOnClusterChange = failOnClusterChange

			var queryPolicy = as.NewQueryPolicy()
			queryPolicy.FailOnClusterChange = failOnClusterChange

			gg.BeforeEach(func() {
				set = randString(50)
				for i := 0; i < keyCount; i++ {
					key, err = as.NewKey(ns, set, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					err = client.PutBins(wpolicy, key, bin1, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}
			})

			gg.It("must run a UDF on all records", func() {
				// run the UDF 3 times consecutively
				for i := 1; i <= 3; i++ {
					statement := as.NewStatement(ns, set)
					exTask, err := client.ExecuteUDF(queryPolicy, statement, "udf1", "testFunc1", as.NewValue(i*2))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// wait until UDF is run on all records
					err = <-exTask.OnComplete()
					gm.Expect(err).ToNot(gm.HaveOccurred())

					time.Sleep(3 * time.Second)

					// read all data and make sure it is consistent
					recordset, err := client.ScanAll(scanPolicy, ns, set)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					for fullRec := range recordset.Results() {
						gm.Expect(fullRec.Err).ToNot(gm.HaveOccurred())
						gm.Expect(fullRec.Record.Bins[bin2.Name]).To(gm.Equal(bin1.Value.GetObject().(int) / (i * 2)))
					}
				}
			})

			gg.It("must run a DeleteUDF on a range of records", func() {
				idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.NUMERIC)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.DropIndex(nil, ns, set, set+bin1.Name)

				gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

				regTask, err := client.RegisterUDF(wpolicy, []byte(udfDelete), "udfDelete.lua", as.LUA)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until UDF is created
				gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())

				// a new record that is not in the range
				key, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				err = client.PutBins(wpolicy, key, as.NewBin(bin1.Name, math.MaxInt16+1))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				statement := as.NewStatement(ns, set)
				statement.SetFilter(as.NewRangeFilter(bin1.Name, 0, math.MaxInt16))
				exTask, err := client.ExecuteUDF(queryPolicy, statement, "udfDelete", "deleteRecord")
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until UDF is run on all records
				gm.Expect(<-exTask.OnComplete()).ToNot(gm.HaveOccurred())

				// read all data and make sure it is consistent
				recordset, err := client.ScanAll(scanPolicy, ns, set)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				i := 0
				for fullRec := range recordset.Results() {
					gm.Expect(fullRec.Err).ToNot(gm.HaveOccurred())
					i++
					// only one record should be returned
					gm.Expect(fullRec.Record.Bins[bin1.Name]).To(gm.Equal(math.MaxInt16 + 1))
				}
				gm.Expect(i).To(gm.Equal(1))
			})
		}
	}) // context

	gg.Context("must serialize parameters and return values sensibly", func() {

		var udfReg sync.Once

		gg.BeforeEach(func() {
			udfReg.Do(func() {
				regTask, err := client.RegisterUDF(wpolicy, []byte(udfEcho), "udfEcho.lua", as.LUA)
				if err != nil {
					panic(err)
				}
				// wait until UDF is created
				<-regTask.OnComplete()
				// a new record that is not in the range
				key, err = as.NewKey(ns, set, randString(50))
				if err != nil {
					panic(err)
				}
			})
		})

		testMatrix := map[interface{}]interface{}{
			math.MinInt64: math.MinInt64,
			// math.MaxInt64:  int64(math.MaxInt64), // TODO: Wrong serialization on server - sign-bit is wrong
			math.MinInt32:               math.MinInt32, // TODO: Wrong serialization type on server
			math.MaxUint32:              math.MaxUint32,
			math.MinInt16:               math.MinInt16,
			math.MaxInt16:               math.MaxInt16,
			math.MaxUint16:              math.MaxUint16,
			math.MinInt8:                math.MinInt8,
			math.MaxInt8:                math.MaxInt8,
			math.MaxUint8:               math.MaxUint8,
			-1:                          -1,
			0:                           0,
			"":                          "",
			strings.Repeat("s", 1):      strings.Repeat("s", 1),
			strings.Repeat("s", 10):     strings.Repeat("s", 10),
			strings.Repeat("s", 100):    strings.Repeat("s", 100),
			strings.Repeat("s", 1000):   strings.Repeat("s", 1000),
			strings.Repeat("s", 10000):  strings.Repeat("s", 10000),
			strings.Repeat("s", 33781):  strings.Repeat("s", 33781),
			strings.Repeat("s", 100000): strings.Repeat("s", 100000),
			"Hello, 世界":                 "Hello, 世界",
		}

		gg.It("must serialize nil values to echo function and get the same value back", func() {

			res, err := client.Execute(nil, key, "udfEcho", "echo", as.NewValue(nil))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(res.(map[interface{}]interface{})["val"]).To(gm.BeNil())

		}) // it

		gg.It("must serialize values to echo function and get the same value back", func() {

			for k, v := range testMatrix {
				res, err := client.Execute(nil, key, "udfEcho", "echo", as.NewValue(k))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(res.(map[interface{}]interface{})["val"]).To(gm.Equal(v))
			}

		}) // it

		gg.It("must serialize list values to echo function and get the same value back", func() {

			v := []interface{}{
				nil,
				math.MinInt64,
				math.MinInt32,
				math.MinInt16,
				math.MinInt8,
				-1,
				0,
				1,
				math.MaxInt8,
				math.MaxUint8,
				math.MaxInt16,
				math.MaxUint16,
				math.MaxInt32,
				math.MaxUint32,
				math.MaxInt64,
				// uint64(math.MaxUint64),// TODO: Wrong serialization on server side
				"",
				"Hello, 世界",
			}

			vExpected := []interface{}{
				nil,
				int(math.MinInt64),
				int(math.MinInt32),
				int(math.MinInt16),
				int(math.MinInt8),
				int(-1),
				int(0),
				int(1),
				int(math.MaxInt8),
				int(math.MaxUint8),
				int(math.MaxInt16),
				int(math.MaxUint16),
				int(math.MaxInt32),
				int(math.MaxUint32),
				uint64(math.MaxInt64), // TODO: Wrong serialization on server
				// uint64(math.MaxUint64), // TODO: Wrong serialization on server side
				"",
				"Hello, 世界",
			}

			res, err := client.Execute(nil, key, "udfEcho", "echo", as.NewValue(v))

			// for i := range v {
			// 	fmt.Printf("%v => %T\n", res.(map[interface{}]interface{})["val"].([]interface{})[i], res.(map[interface{}]interface{})["val"].([]interface{})[i])
			// 	fmt.Printf("%v => %T\n", vExpected[i], vExpected[i])
			// }

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(res.(map[interface{}]interface{})["val"]).To(gm.Equal(vExpected))

		}) // it

		gg.It("must serialize map values to echo function and get the same value back", func() {

			v := map[interface{}]interface{}{
				nil:            nil,
				math.MinInt64:  math.MinInt64,
				math.MinInt32:  math.MinInt32,
				math.MinInt16:  math.MinInt16,
				math.MinInt8:   math.MinInt8,
				-1:             -1,
				0:              0,
				1:              1,
				math.MaxInt8:   math.MaxInt8,
				math.MaxUint8:  math.MaxUint8,
				math.MaxInt16:  math.MaxInt16,
				math.MaxUint16: math.MaxUint16,
				math.MaxInt32:  math.MaxInt32,
				math.MaxUint32: math.MaxUint32,
				math.MaxInt64:  math.MaxInt64,
				"":             "",
				"Hello, 世界":    "Hello, 世界",
			}

			vExpected := map[interface{}]interface{}{
				nil:                   nil,
				math.MinInt64:         math.MinInt64,
				math.MinInt32:         math.MinInt32,
				math.MinInt16:         math.MinInt16,
				math.MinInt8:          math.MinInt8,
				-1:                    -1,
				0:                     0,
				1:                     1,
				math.MaxInt8:          math.MaxInt8,
				math.MaxUint8:         math.MaxUint8,
				math.MaxInt16:         math.MaxInt16,
				math.MaxUint16:        math.MaxUint16,
				math.MaxInt32:         math.MaxInt32,
				math.MaxUint32:        math.MaxUint32,
				uint64(math.MaxInt64): uint64(math.MaxInt64),
				"":                    "",
				"Hello, 世界":           "Hello, 世界",
			}

			res, err := client.Execute(nil, key, "udfEcho", "echo", as.NewValue(v))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			resMap := res.(map[interface{}]interface{})["val"].(map[interface{}]interface{})
			// for k := range resMap {
			// 	fmt.Printf("%v : %v => %T: %T\n", k, k, resMap[k], resMap[k])
			// 	fmt.Printf("%v => %T\n", vExpected[k], vExpected[k])
			// }

			gm.Expect(resMap).To(gm.Equal(vExpected))

		}) // it

	}) // context

})

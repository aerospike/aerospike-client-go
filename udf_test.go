// Copyright 2013-2014 Aerospike, Inc.
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
	"flag"
	"math"
	"math/rand"
	"strings"
	"time"

	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const udfBody = `function testFunc1(rec, div)
   local ret = map()                     -- Initialize the return value (a map)

   local x = rec['bin1']                 -- Get the value from record bin named "bin1"

   rec['bin2'] = (x / div)               -- Set the value in record bin named "bin2"

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

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("UDF/Query tests", func() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// connection data
	var client *Client
	var err error
	var ns = "test"
	var set = randString(50)
	var key *Key
	var wpolicy = NewWritePolicy(0, 0)

	const keyCount = 1000
	bin1 := NewBin("bin1", rand.Intn(math.MaxInt16))
	bin2 := NewBin("bin2", 1)

	client, _ = NewClient(*host, *port)

	It("must Register a UDF", func() {
		regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udf1.lua", LUA)
		Expect(err).ToNot(HaveOccurred())

		// wait until UDF is created
		for {
			if err := <-regTask.OnComplete(); err == nil {
				break
			}
		}
	})

	It("must run a UDF on a single record", func() {
		key, err = NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())
		err = client.PutBins(wpolicy, key, bin1, bin2)
		Expect(err).ToNot(HaveOccurred())

		res, err := client.Execute(nil, key, "udf1", "testFunc1", NewValue(2))
		Expect(err).ToNot(HaveOccurred())
		Expect(res).To(Equal(map[interface{}]interface{}{"status": "OK"}))

		// read all data and make sure it is consistent
		rec, err := client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())

		Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
		Expect(rec.Bins[bin2.Name]).To(Equal(bin1.Value.GetObject().(int) / 2))
	})

	It("must list all udfs on the server", func() {
		udfList, err := client.ListUDF(nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(udfList)).To(BeNumerically(">", 0))
	})

	It("must drop a udf on the server", func() {
		regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udfToBeDropped.lua", LUA)
		Expect(err).ToNot(HaveOccurred())

		// wait until UDF is created
		err = <-regTask.OnComplete()
		Expect(err).ToNot(HaveOccurred())

		delTask, err := client.RemoveUDF(wpolicy, "udfToBeDropped.lua")
		Expect(err).ToNot(HaveOccurred())

		// wait until UDF is deleted
		for {
			if err := <-delTask.OnComplete(); err == nil {
				break
			}
		}

		_, err = client.RemoveUDF(wpolicy, "udfToBeDropped.lua")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("error=file_not_found"))
	})

	Context("must run the UDF on all records", func() {

		BeforeEach(func() {
			for i := 0; i < keyCount; i++ {
				key, err = NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				err = client.PutBins(wpolicy, key, bin1, bin2)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		It("must run a UDF on all records", func() {
			// run the UDF 3 times consecutively
			for i := 1; i <= 3; i++ {
				statement := NewStatement(ns, set)
				exTask, err := client.ExecuteUDF(nil, statement, "udf1", "testFunc1", NewValue(i*2))
				Expect(err).ToNot(HaveOccurred())

				// wait until UDF is run on all records
				for {
					if err := <-exTask.OnComplete(); err == nil {
						break
					} else {
						panic(err)
					}
				}

				// read all data and make sure it is consistent
				recordset, err := client.ScanAll(nil, ns, set)
				Expect(err).ToNot(HaveOccurred())

				for fullRec := range recordset.Records {
					Expect(fullRec.Bins[bin2.Name]).To(Equal(bin1.Value.GetObject().(int) / (i * 2)))
				}
			}
		})

		It("must run a DeleteUDF on a range of records", func() {
			idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, NUMERIC)
			Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())

			regTask, err := client.RegisterUDF(wpolicy, []byte(udfDelete), "udfDelete.lua", LUA)
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is created
			Expect(<-regTask.OnComplete()).ToNot(HaveOccurred())

			statement := NewStatement(ns, set)
			statement.Addfilter(NewRangeFilter(bin1.Name, 0, math.MaxInt16))
			exTask, err := client.ExecuteUDF(nil, statement, "udfDelete", "deleteRecord")
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is run on all records
			Expect(<-exTask.OnComplete()).ToNot(HaveOccurred())

			// a new record that is not in the range
			key, err = NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())
			err = client.PutBins(wpolicy, key, NewBin(bin1.Name, math.MaxInt16+1))
			Expect(err).ToNot(HaveOccurred())

			// read all data and make sure it is consistent
			recordset, err := client.ScanAll(nil, ns, set)
			Expect(err).ToNot(HaveOccurred())

			i := 0
			for fullRec := range recordset.Records {
				i++
				// only one record should be returned
				Expect(fullRec.Bins[bin1.Name]).To(Equal(math.MaxInt16 + 1))
			}
			Expect(i).To(Equal(1))
		})

	}) // context

	Context("must serialize parameters and return values sensibly", func() {

		regTask, _ := client.RegisterUDF(wpolicy, []byte(udfEcho), "udfEcho.lua", LUA)
		// wait until UDF is created
		<-regTask.OnComplete()
		// a new record that is not in the range
		key, err = NewKey(ns, set, randString(50))

		testMatrix := map[interface{}]interface{}{
			math.MinInt64: math.MinInt64,
			// math.MaxInt64:  int64(math.MaxInt64), // TODO: Wrong serialization on server - sign-bit is wrong
			math.MinInt32:  math.MinInt32, // TODO: Wrong serialization type on server
			math.MaxUint32: math.MaxUint32,
			math.MinInt16:  math.MinInt16,
			math.MaxInt16:  math.MaxInt16,
			math.MaxUint16: math.MaxUint16,
			math.MinInt8:   math.MinInt8,
			math.MaxInt8:   math.MaxInt8,
			math.MaxUint8:  math.MaxUint8,
			-1:             -1,
			0:              0,
			"":             "",
			strings.Repeat("s", 1):      strings.Repeat("s", 1),
			strings.Repeat("s", 10):     strings.Repeat("s", 10),
			strings.Repeat("s", 100):    strings.Repeat("s", 100),
			strings.Repeat("s", 1000):   strings.Repeat("s", 1000),
			strings.Repeat("s", 10000):  strings.Repeat("s", 10000),
			strings.Repeat("s", 33781):  strings.Repeat("s", 33781),
			strings.Repeat("s", 100000): strings.Repeat("s", 100000),
			"Hello, 世界":                 "Hello, 世界",
		}

		It("must serialize nil values to echo function and get the same value back", func() {

			res, err := client.Execute(nil, key, "udfEcho", "echo", NewValue(nil))
			Expect(err).ToNot(HaveOccurred())
			Expect(res.(map[interface{}]interface{})["val"]).To(BeNil())

		}) // it

		It("must serialize values to echo function and get the same value back", func() {

			for k, v := range testMatrix {
				res, err := client.Execute(nil, key, "udfEcho", "echo", NewValue(k))
				Expect(err).ToNot(HaveOccurred())
				Expect(res.(map[interface{}]interface{})["val"]).To(Equal(v))
			}

		}) // it

		It("must serialize list values to echo function and get the same value back", func() {

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

			res, err := client.Execute(nil, key, "udfEcho", "echo", NewValue(v))

			// for i := range v {
			// 	fmt.Printf("%v => %T\n", res.(map[interface{}]interface{})["val"].([]interface{})[i], res.(map[interface{}]interface{})["val"].([]interface{})[i])
			// 	fmt.Printf("%v => %T\n", vExpected[i], vExpected[i])
			// }

			Expect(err).ToNot(HaveOccurred())
			Expect(res.(map[interface{}]interface{})["val"]).To(Equal(vExpected))

		}) // it

		It("must serialize map values to echo function and get the same value back", func() {

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
				"":          "",
				"Hello, 世界": "Hello, 世界",
			}

			res, err := client.Execute(nil, key, "udfEcho", "echo", NewValue(v))
			Expect(err).ToNot(HaveOccurred())

			resMap := res.(map[interface{}]interface{})["val"].(map[interface{}]interface{})
			// for k := range resMap {
			// 	fmt.Printf("%v : %v => %T: %T\n", k, k, resMap[k], resMap[k])
			// 	fmt.Printf("%v => %T\n", vExpected[k], vExpected[k])
			// }

			Expect(resMap).To(Equal(vExpected))

		}) // it

	}) // context

})

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
	"fmt"
	"math"
	"math/rand"
	// "strings"
	"time"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"

	// . "github.com/aerospike/aerospike-client-go/utils/buffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	fmt.Println("Testing")
	Logger.SetLevel(ERR)
}

const udfBody = `function testFunc1(rec)
   local ret = map()                     -- Initialize the return value (a map)

   -- if not aerospike:exists(rec) then     -- Check to see that the record exists
     --ret['status'] = 'DOES NOT EXIST'    -- Set the return status
   -- else
     local x = rec['bin1']               -- Get the value from record bin named "bin1"

     rec['bin2'] = (x / 2)               -- Set the value in record bin named "bin2"

    aerospike:update(rec)                -- Update the main record
  -- end

  ret['status'] = 'OK'                   -- Populate the return status
  return ret                             -- Return the Return value and/or status
end`

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("UDF/Query tests", func() {
	rand.Seed(time.Now().UnixNano())

	Describe("Register UDF", func() {
		// connection data
		var client *Client
		var err error
		var ns = "test"
		var set = randString(50)
		var key *Key
		var wpolicy = NewWritePolicy(0, 0)

		const keyCount = 100
		bin1 := NewBin("bin1", rand.Intn(math.MaxInt16))
		bin2 := NewBin("bin2", 1)

		BeforeEach(func() {
			client, err = NewClient("127.0.0.1", 3000)
			Expect(err).ToNot(HaveOccurred())

			for i := 0; i < keyCount; i++ {
				key, err = NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				err = client.PutBins(wpolicy, key, bin1, bin2)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		It("must Register a UDF", func() {
			regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udf1.lua", LUA)
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is created
			<-regTask.OnComplete()
		})

		It("must run a UDF on all records", func() {
			statement := NewStatement(ns, set)
			exTask, err := client.ExecuteUDF(nil, statement, "udf1.lua", "testFunc1")
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is run on all records
			<-exTask.OnComplete()

			// read all data and make sure it is consistent
			results, err := client.ScanAll(nil, ns, set)
			Expect(err).ToNot(HaveOccurred())

			for fullRec := range results {
				Expect(fullRec.Bins[bin2.Name]).To(Equal(bin1.Value.GetObject().(int) / 2))
			}

			It("must run a UDF on a single record", func() {
				// statement := NewStatement(ns, set)
				// exTask, err := client.Execute(nil, key, "udf1.lua", "testFunc1")
				// Expect(err).ToNot(HaveOccurred())

				// // wait until UDF is run on all records
				// <-exTask.OnComplete()

				// // read all data and make sure it is consistent
				// results, err := client.ScanAll(nil, ns, set)
				// Expect(err).ToNot(HaveOccurred())

				// for fullRec := range results {
				// 	Expect(fullRec.Bins[bin2.Name]).To(Equal(bin1.Value.GetObject().(int) / 2))
				// }

			})
		})

	})
})

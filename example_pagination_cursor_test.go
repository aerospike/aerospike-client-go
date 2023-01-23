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
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v6"
)

func ExamplePartitionFilter_EncodeCursor() {
	// Setup the client here
	// client, err := as.NewClient("127.0.0.1", 3000)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	var ns = *namespace
	var set = randString(50)

	// initialize the records
	keyCount := 1000
	for i := 0; i < keyCount; i++ {
		key, err := as.NewKey(ns, set, i)
		if err != nil {
			log.Fatal(err)
		}

		err = client.Put(nil, key, as.BinMap{"bin": i})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Set up the scan policy
	spolicy := as.NewScanPolicy()
	spolicy.MaxRecords = 30

	received := 0
	var buf []byte
	for received < keyCount {
		pf := as.NewPartitionFilterAll()

		if len(buf) > 0 {
			err = pf.DecodeCursor(buf)
			if err != nil {
				log.Fatal(err)
			}
		}

		recordset, err := client.ScanPartitions(spolicy, pf, ns, set)
		if err != nil {
			log.Fatal(err)
		}

		counter := 0
		for range recordset.Results() {
			counter++
			received++
		}

		if counter > 30 {
			log.Fatal("More records received than requested.")
		}

		buf, err = pf.EncodeCursor()
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println(received)
	// Output:
	// 1000
}

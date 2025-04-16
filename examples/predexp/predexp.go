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

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go"
	shared "github.com/aerospike/aerospike-client-go/examples/shared"
)

func main() {
	runExample(shared.Client)
	log.Println("Example finished successfully.")
}

func runExample(client *as.Client) {
	for i := 0; i < 5; i++ {
		key, err := as.NewKey(*shared.Namespace, *shared.Set, i) // user key can be of any supported type
		shared.PanicOnError(err)

		// define some bins
		bins := as.BinMap{
			"valueBin":   42, // you can pass any supported type as bin value
			"versionBin": i,
			"id":         i,
		}

		// write the bins
		writePolicy := as.NewWritePolicy(0, 0)
		err = client.Put(writePolicy, key, bins)
		shared.PanicOnError(err)
	}

	readRecord(client, 2)

	// want to update to this
	value := int64(123)
	version := int64(3)

	bins := as.BinMap{
		"valueBin":   value,
		"versionBin": version,
	}

	// set a predicate expression to only apply if version < 3
	// leave the RecordExistsAction at the default value UPDATE - if record isn't found, a new one is created
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.PredExp = []as.PredExp{
		as.NewPredExpIntegerBin("versionBin"),
		as.NewPredExpIntegerValue(version),
		as.NewPredExpIntegerLess(),
	}

	// this should update the record
	key, err := as.NewKey(*shared.Namespace, *shared.Set, 2)
	err = client.Put(writePolicy, key, bins)
	log.Printf("Put key 2: %s\n", err)

	// version is not less than desired value; no effect
	key, err = as.NewKey(*shared.Namespace, *shared.Set, 3)
	err = client.Put(writePolicy, key, bins)
	log.Printf("Put key 3: %s\n", err)

	// version is not less than desired value; no effect
	key, err = as.NewKey(*shared.Namespace, *shared.Set, 4)
	err = client.Put(writePolicy, key, bins)
	log.Printf("Put key 4: %s\n", err)

	// key does not exist; new record added
	key, err = as.NewKey(*shared.Namespace, *shared.Set, 5)
	err = client.Put(writePolicy, key, bins)
	log.Printf("Put key 5: %s\n", err)

	readRecord(client, 2)
	readRecord(client, 3)
	readRecord(client, 4)
	readRecord(client, 5)

	// clean up
	for i := 0; i < 6; i++ {
		deleteRecord(client, i)
	}
}

func readRecord(client *as.Client, id int) {
	// read one record
	readPolicy := as.NewPolicy()
	key, err := as.NewKey(*shared.Namespace, *shared.Set, id)
	rec, err := client.Get(readPolicy, key)
	shared.PanicOnError(err)

	log.Printf("%#v\n", *rec)
}

func deleteRecord(client *as.Client, id int) {
	// read one record
	writePolicy := as.NewWritePolicy(0, 0)
	key, err := as.NewKey(*shared.Namespace, *shared.Set, id)
	existed, err := client.Delete(writePolicy, key)
	shared.PanicOnError(err)
	log.Printf("did key exist before delete: %#v\n", existed)
}

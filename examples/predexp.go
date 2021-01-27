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

package main

import (
	"fmt"

	. "github.com/aerospike/aerospike-client-go"
)

var namespace = "test"
var setName = "aerospike"

func main() {
	// define a client to connect to
	client, err := NewClient("127.0.0.1", 3000)
	panicOnError(err)

	for i := 0; i < 5; i++ {
		key, err := NewKey(namespace, setName, i) // user key can be of any supported type
		panicOnError(err)

		// define some bins
		bins := BinMap{
			"valueBin":   42, // you can pass any supported type as bin value
			"versionBin": i,
			"id":         i,
		}

		// write the bins
		writePolicy := NewWritePolicy(0, 0)
		err = client.Put(writePolicy, key, bins)
		panicOnError(err)
	}

	readRecord(client, 2)

	// want to update to this
	value := int64(123)
	version := int64(3)

	bins := BinMap{
		"valueBin":   value,
		"versionBin": version,
	}

	// set a predicate expression to only apply if version < 3
	// leave the RecordExistsAction at the default value UPDATE - if record isn't found, a new one is created
	writePolicy := NewWritePolicy(0, 0)
	writePolicy.PredExp = []PredExp{
		NewPredExpIntegerBin("versionBin"),
		NewPredExpIntegerValue(version),
		NewPredExpIntegerLess(),
	}

	// this should update the record
	key, err := NewKey(namespace, setName, 2)
	err = client.Put(writePolicy, key, bins)
	fmt.Printf("Put key 2: %s\n", err)

	// version is not less than desired value; no effect
	key, err = NewKey(namespace, setName, 3)
	err = client.Put(writePolicy, key, bins)
	fmt.Printf("Put key 3: %s\n", err)

	// version is not less than desired value; no effect
	key, err = NewKey(namespace, setName, 4)
	err = client.Put(writePolicy, key, bins)
	fmt.Printf("Put key 4: %s\n", err)

	// key does not exist; new record added
	key, err = NewKey(namespace, setName, 5)
	err = client.Put(writePolicy, key, bins)
	fmt.Printf("Put key 5: %s\n", err)

	readRecord(client, 2)
	readRecord(client, 3)
	readRecord(client, 4)
	readRecord(client, 5)

	// clean up
	for i := 0; i < 6; i++ {
		deleteRecord(client, i)
	}
}

func readRecord(client *Client, id int) {
	// read one record
	readPolicy := NewPolicy()
	key, err := NewKey(namespace, setName, id)
	rec, err := client.Get(readPolicy, key)
	panicOnError(err)

	fmt.Printf("%#v\n", *rec)
}

func deleteRecord(client *Client, id int) {
	// read one record
	writePolicy := NewWritePolicy(0, 0)
	key, err := NewKey(namespace, setName, id)
	existed, err := client.Delete(writePolicy, key)
	panicOnError(err)
	fmt.Printf("did key exist before delete: %#v\n", existed)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

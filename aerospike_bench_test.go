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
	. "github.com/citrusleaf/go-client"

	"testing"
)

var r *Record
var err error

func benchGet(times int, client *Client, key *Key) {
	for i := 0; i < times; i++ {
		if r, err = client.Get(nil, key); err != nil {
			panic(err)
		}
	}
}

func benchPut(times int, client *Client, key *Key, bins []*Bin, wp *WritePolicy) {
	for i := 0; i < times; i++ {
		if err = client.PutBins(wp, key, bins...); err != nil {
			panic(err)
		}
	}
}

func Benchmark_Get(b *testing.B) {
	client, err := NewClient("localhost", 3000)
	if err != nil {
		b.Fail()
	}

	key, _ := NewKey("test", "databases", "Aerospike")

	b.ResetTimer()
	benchGet(b.N, client, key)
}

func Benchmark_Put(b *testing.B) {
	client, err := NewClient("localhost", 3000)
	if err != nil {
		b.Fail()
	}

	key, _ := NewKey("test", "databases", "Aerospike")
	writepolicy := NewWritePolicy(0, 0)

	dbName := NewBin("dbname", "CouchDB")
	price := NewBin("price", 0)
	// keywords := NewBin("keywords", []string{"concurrent", "fast"})
	// speeds := NewBin("keywords", []int{18, 251})

	b.ResetTimer()
	benchPut(b.N, client, key, []*Bin{dbName, price}, writepolicy)
}

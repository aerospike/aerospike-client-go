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
	"runtime"
	"testing"

	// "time"
	_ "net/http/pprof"

	as "github.com/aerospike/aerospike-client-go"
	// _ "github.com/influxdata/influxdb/client"
)

func doScan(b *testing.B) {
	runtime.GC()
	b.ResetTimer()
	b.SetBytes(0)

	policy := as.NewScanPolicy()

	n := 0
	for i := 0; i < 10; i++ {
		results, err := client.ScanAll(policy, *namespace, "test")
		if err != nil {
			b.Errorf("Scan error: %s", err)
		}

		for range results.Results() {
			n++
		}
	}
	b.N = n
}

func doQuery(b *testing.B) {
	var err error

	runtime.GC()
	b.ResetTimer()
	b.SetBytes(0)

	n := 0
	//queries to aerospike
	policy := as.NewQueryPolicy()
	stmt := as.NewStatement(*namespace, "test")

	b.ResetTimer()
	b.SetBytes(0)
	results, err := client.Query(policy, stmt)
	if err != nil {
		b.Errorf("Query error: %s", err)
	}

	for range results.Results() {
		n++
	}

	b.N = n
}

func Benchmark_Scan(b *testing.B) {
	initTestVars()
	go doScan(b)
}

func Benchmark_Query(b *testing.B) {
	initTestVars()
	doQuery(b)
}

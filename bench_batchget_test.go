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
	"math/rand"
	"runtime"
	"strings"
	"testing"

	_ "net/http/pprof"

	as "github.com/aerospike/aerospike-client-go"
)

func makeDataForBatchGetBench(set string, bins []*as.Bin) {
	for i := 0; i < 1000; i++ {
		key, _ := as.NewKey(*namespace, set, i)
		client.PutBins(nil, key, bins...)
	}
}

func doBatchGet(policy *as.BatchPolicy, set string, b *testing.B) {
	var err error
	var keys []*as.Key
	for i := 0; i < 1000; i++ {
		key, _ := as.NewKey(*namespace, set, i)
		keys = append(keys, key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = client.BatchGet(policy, keys)
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_BatchGet_________Int64(b *testing.B) {
	set := "batch_get_bench_integer"
	bins := []*as.Bin{as.NewBin("b", rand.Int63())}
	makeDataForBatchGetBench(set, bins)
	b.N = 1000
	runtime.GC()
	b.ResetTimer()

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_________Int32(b *testing.B) {
	set := "batch_get_bench_integer"
	bins := []*as.Bin{as.NewBin("b", rand.Int31())}
	makeDataForBatchGetBench(set, bins)
	b.N = 1000
	runtime.GC()
	b.ResetTimer()

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String______1(b *testing.B) {
	set := "batch_get_bench_str_1"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 1))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String_____10(b *testing.B) {
	set := "batch_get_bench_str_10"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String____100(b *testing.B) {
	set := "batch_get_bench_str_100"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 100))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String___1000(b *testing.B) {
	set := "batch_get_bench_str_1000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 1000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String__10000(b *testing.B) {
	set := "batch_get_bench_str_10000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_String_100000(b *testing.B) {
	set := "batch_get_bench_str_10000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_Complex_Array(b *testing.B) {
	set := "batch_get_bench_str_10000"
	// bins := []*as.Bin{as.NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*as.Bin{as.NewBin("b", []interface{}{rand.Int63()})}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

func Benchmark_BatchGet_Complex_Map(b *testing.B) {
	set := "batch_get_bench_str_10000"
	// bins := []*as.Bin{as.NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*as.Bin{as.NewBin("b", map[interface{}]interface{}{rand.Int63(): rand.Int63()})}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForBatchGetBench(set, bins)

	policy := as.NewBatchPolicy()
	doBatchGet(policy, set, b)
}

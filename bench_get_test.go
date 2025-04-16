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

func makeDataForGetBench(set string, bins []*as.Bin) {
	key, _ := as.NewKey(*namespace, set, 0)
	client.PutBins(nil, key, bins...)
}

func doGet(set string, b *testing.B) {
	var err error
	key, _ := as.NewKey(*namespace, set, 0)
	for i := 0; i < b.N; i++ {
		_, err = client.Get(nil, key)
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_Get_________Int64(b *testing.B) {
	set := "get_bench_integer"
	bins := []*as.Bin{as.NewBin("b", rand.Int63())}
	makeDataForGetBench(set, bins)
	runtime.GC()
	b.ResetTimer()
	doGet(set, b)
}

func Benchmark_Get_________Int32(b *testing.B) {
	set := "get_bench_integer"
	bins := []*as.Bin{as.NewBin("b", rand.Int31())}
	makeDataForGetBench(set, bins)
	runtime.GC()
	b.ResetTimer()
	doGet(set, b)
}

func Benchmark_Get_String______1(b *testing.B) {
	set := "get_bench_str_1"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 1))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String_____10(b *testing.B) {
	set := "get_bench_str_10"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String____100(b *testing.B) {
	set := "get_bench_str_100"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 100))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String___1000(b *testing.B) {
	set := "get_bench_str_1000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 1000))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String__10000(b *testing.B) {
	set := "get_bench_str_10000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10000))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String_100000(b *testing.B) {
	set := "get_bench_str_10000"
	bins := []*as.Bin{as.NewBin("b", strings.Repeat("s", 10000))}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_Complex_Array(b *testing.B) {
	set := "get_bench_str_10000"
	// bins := []*as.Bin{as.NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*as.Bin{as.NewBin("b", []interface{}{rand.Int63()})}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_Complex_Map(b *testing.B) {
	set := "get_bench_str_10000"
	// bins := []*as.Bin{as.NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*as.Bin{as.NewBin("b", map[interface{}]interface{}{rand.Int63(): rand.Int63()})}
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func doPut(set string, value interface{}, b *testing.B) {
	var err error
	// key, _ := as.NewKey(*namespace, set, 0)
	for i := 0; i < b.N; i++ {
		bin := as.NewBin("b", value)
		// err = client.PutBins(nil, key, bin)
		if err != nil || bin == nil {
			panic(err)
		}
	}
}

func Benchmark_Put_Complex_ArrayFloat64(b *testing.B) {
	set := "Benchmark_Put_Complex_ArrayInt2Int"
	a := make([]float64, 1000)
	for i := range a {
		a[i] = float64(i)
	}

	runtime.GC()
	b.ResetTimer()
	doPut(set, a, b)
}

func Benchmark_Put_Complex_MapFloat642Float64(b *testing.B) {
	set := "Benchmark_Put_Complex_MapFloat642Float64"
	a := make(map[float64]float64, 1000)
	for i := 0; i < 1000; i++ {
		a[float64(i)] = float64(i)
	}
	runtime.GC()
	b.ResetTimer()
	doPut(set, a, b)
}

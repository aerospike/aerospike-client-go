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
	"log"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"net/http"
	_ "net/http/pprof"

	. "github.com/aerospike/aerospike-client-go"
)

var benchGetClient, _ = NewClient("127.0.0.1", 3000)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func makeDataForGetBench(set string, bins []*Bin) {
	key, _ := NewKey("test", set, 0)
	benchGetClient.PutBins(nil, key, bins...)
}

func doGet(set string, b *testing.B) {
	key, _ := NewKey("test", set, 0)
	for i := 0; i < b.N; i++ {
		benchGetClient.Get(nil, key)
	}
}

func Benchmark_Get_________Int64(b *testing.B) {
	set := "get_bench_integer"
	bins := []*Bin{NewBin("b", rand.Int63())}
	makeDataForGetBench(set, bins)
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	doGet(set, b)
}

func Benchmark_Get_________Int32(b *testing.B) {
	set := "get_bench_integer"
	bins := []*Bin{NewBin("b", rand.Int31())}
	makeDataForGetBench(set, bins)
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	doGet(set, b)
}

func Benchmark_Get_String______1(b *testing.B) {
	set := "get_bench_str_1"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 1))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String_____10(b *testing.B) {
	set := "get_bench_str_10"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 10))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String____100(b *testing.B) {
	set := "get_bench_str_100"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 100))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String___1000(b *testing.B) {
	set := "get_bench_str_1000"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 1000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String__10000(b *testing.B) {
	set := "get_bench_str_10000"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 10000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_String_100000(b *testing.B) {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	set := "get_bench_str_10000"
	bins := []*Bin{NewBin("b", strings.Repeat("s", 10000))}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_Complex_Array(b *testing.B) {
	set := "get_bench_str_10000"
	// bins := []*Bin{NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*Bin{NewBin("b", []interface{}{rand.Int63()})}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

func Benchmark_Get_Complex_Map(b *testing.B) {
	set := "get_bench_str_10000"
	// bins := []*Bin{NewBin("b", []interface{}{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}})}
	bins := []*Bin{NewBin("b", map[interface{}]interface{}{rand.Int63(): rand.Int63()})}
	b.N = 1000
	runtime.GC()
	b.ResetTimer()
	makeDataForGetBench(set, bins)
	doGet(set, b)
}

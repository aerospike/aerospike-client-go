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
	"strings"
	"testing"
)

type T interface {
	size() int
}

//////////////////////////////////
type S string

func (s *S) size() int {
	return len(*s)
}

//////////////////////////////////
type I int

func (i I) size() int {
	return 8
}

// ///////////////////////////////////////////////

func estimateSize(v T) int {
	return v.size()
}

func estimateSizeStr(v S) int {
	return v.size()
}

func estimateSizeInt(v I) int {
	return v.size()
}

func estimateSizePtr(v *string) int {
	return len(*v)
}

// ///////////////////////////////////////////////

var n int

func Benchmark_EstimateSizeStringGeneric(b *testing.B) {
	value := S(strings.Repeat("s", 16))
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n += estimateSize(&value)
	}
}

func Benchmark_EstimateSizeStringSpecialized(b *testing.B) {
	value := S(strings.Repeat("s", 16))
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n += estimateSizeStr(value)
	}
}

func Benchmark_EstimateSizeStringPtr(b *testing.B) {
	value := strings.Repeat("s", 16)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n += estimateSizePtr(&value)
	}
}

func Benchmark_EstimateSizeIntGeneric(b *testing.B) {
	value := I(16)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n += estimateSize(value)
	}
}

func Benchmark_EstimateSizeIntSpecialized(b *testing.B) {
	value := I(16)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n += estimateSizeInt(value)
	}
}

// Copyright 2013-2016 Aerospike, Inc.
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

package aerospike

import (
	"math/rand"
	"testing"
)

func Benchmark_PackInt32(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackInt(0, int32(i))
	}
}

func Benchmark_PackInt64(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackLong(0, int64(i))
	}
}

func Benchmark_PackInt16(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackShort(0, int16(i))
	}
}

func Benchmark_PackFloat32(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackFloat32(float32(i))
	}
}

func Benchmark_PackFloat64(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackFloat64(float64(i))
	}
}

func Benchmark_PackArray(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	a := []interface{}{
		rand.Int63(),
		"s",
	}
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackList(a)
	}
}

func Benchmark_PackMap(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	m := map[interface{}]interface{}{
		rand.Int63(): rand.Int63(),
		"s":          "s",
	}
	for i := 0; i < b.N; i++ {
		testPacker.buffer.Reset()
		testPacker.PackMap(m)
	}
}

func Benchmark_UnPackArray(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	a := []interface{}{
		rand.Int63(),
		"s",
	}
	testPacker.PackList(a)
	testUnPacker := newUnpacker(testPacker.buffer.Bytes(), 0, testPacker.buffer.Len())

	for i := 0; i < b.N; i++ {
		testUnPacker.offset = 0
		a, _ = testUnPacker.UnpackList()
	}
}

func Benchmark_UnPackMap(b *testing.B) {
	b.N = 1e6
	testPacker := newPacker()
	m := map[interface{}]interface{}{
		rand.Int63(): rand.Int63(),
		"s":          "s",
		"s2":         "s2",
		nil:          []byte{18},
	}
	testPacker.PackMap(m)
	testUnPacker := newUnpacker(testPacker.buffer.Bytes(), 0, testPacker.buffer.Len())

	for i := 0; i < b.N; i++ {
		testUnPacker.offset = 0
		m, _ = testUnPacker.UnpackMap()
	}
}

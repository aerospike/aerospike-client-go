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
	"bytes"
	"strings"
	"testing"
)

var sv StringValue
var iv IntegerValue
var lv LongValue
var bav BytesValue

func Benchmark_StringValue(b *testing.B) {
	b.N = 1e6
	str := strings.Repeat("a", 1000)
	for i := 0; i < b.N; i++ {
		sv = NewStringValue(str)
	}
}

func Benchmark_IntegerValue(b *testing.B) {
	b.N = 1e6
	in := 1091
	for i := 0; i < b.N; i++ {
		iv = NewIntegerValue(in)
	}
}

func Benchmark_LongValue(b *testing.B) {
	b.N = 1e6
	in := int64(10916927583729485)
	for i := 0; i < b.N; i++ {
		lv = NewLongValue(in)
	}
}

func Benchmark_BytesValue(b *testing.B) {
	b.N = 1e6
	barr := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}, 1000)
	for i := 0; i < b.N; i++ {
		bav = NewBytesValue(barr)
	}
}

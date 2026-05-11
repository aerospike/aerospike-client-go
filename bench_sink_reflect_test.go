//go:build !as_performance

// Copyright 2014-2026 Aerospike, Inc.
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
	"reflect"
	"testing"
)

// BenchmarkRead_Reflect measures the existing Client.GetObject path:
// wire → reflect-based field assignment into a typed struct. Lives in a
// build-tagged file because objectMappings and objectParser are only
// compiled when !as_performance.
func BenchmarkRead_Reflect(b *testing.B) {
	ops := buildBenchOps()
	wire := encodeOps(ops)

	dst := &benchRecord{}
	rv := reflect.ValueOf(dst)
	brc := &baseReadCommand{}
	brc.dataBuffer = wire
	brc.object = &rv

	// Warm the type cache so the first iteration isn't an outlier.
	objectMappings.getMapping(rv.Type())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brc.dataOffset = 0
		_ = objectParser(brc, len(ops), 0, 0, 0)
	}
}

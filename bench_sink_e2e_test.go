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

package aerospike_test

import (
	"runtime"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// e2eRecord is the user-facing struct for the end-to-end benchmarks.
// Matches what the codegen tool would emit for these tags.
type e2eRecord struct {
	I1 int    `as:"i1"`
	I2 int    `as:"i2"`
	I3 int    `as:"i3"`
	I4 int    `as:"i4"`
	S1 string `as:"s1"`
	S2 string `as:"s2"`
	S3 string `as:"s3"`
	S4 string `as:"s4"`
	B1 []byte `as:"b1"`
	B2 []byte `as:"b2"`
	B3 []byte `as:"b3"`
	B4 []byte `as:"b4"`
}

func (r *e2eRecord) AerospikeBin(name string, value any) as.Error {
	switch name {
	case "i1":
		r.I1 = value.(int)
	case "i2":
		r.I2 = value.(int)
	case "i3":
		r.I3 = value.(int)
	case "i4":
		r.I4 = value.(int)
	case "s1":
		r.S1 = value.(string)
	case "s2":
		r.S2 = value.(string)
	case "s3":
		r.S3 = value.(string)
	case "s4":
		r.S4 = value.(string)
	case "b1":
		r.B1 = value.([]byte)
	case "b2":
		r.B2 = value.([]byte)
	case "b3":
		r.B3 = value.([]byte)
	case "b4":
		r.B4 = value.([]byte)
	}
	return nil
}

func (r *e2eRecord) AerospikeBinNames() []string {
	return []string{"i1", "i2", "i3", "i4", "s1", "s2", "s3", "s4", "b1", "b2", "b3", "b4"}
}

// e2eBins matches e2eRecord exactly so all three benchmarks read the same
// payload off the wire.
func e2eBins() []*as.Bin {
	return []*as.Bin{
		as.NewBin("i1", 1),
		as.NewBin("i2", 2),
		as.NewBin("i3", 3),
		as.NewBin("i4", 4),
		as.NewBin("s1", "alpha"),
		as.NewBin("s2", "beta"),
		as.NewBin("s3", "gamma"),
		as.NewBin("s4", "delta"),
		as.NewBin("b1", []byte{1, 2, 3, 4}),
		as.NewBin("b2", []byte{5, 6, 7, 8}),
		as.NewBin("b3", []byte{9, 10, 11, 12}),
		as.NewBin("b4", []byte{13, 14, 15, 16}),
	}
}

func e2eSetup(b *testing.B) *as.Key {
	b.Helper()
	key, err := as.NewKey(*namespace, "bench", "sink-vs-binmap")
	if err != nil {
		b.Fatalf("new key: %v", err)
	}
	if err := client.PutBins(nil, key, e2eBins()...); err != nil {
		b.Fatalf("put bins: %v", err)
	}
	return key
}

// BenchmarkE2E_Get exercises the existing BinMap-based API end-to-end. A
// real user has to read the record AND extract typed values from the
// returned BinMap; the benchmark mirrors that work so the comparison to
// GetSink is apples-to-apples.
func BenchmarkE2E_Get(b *testing.B) {
	key := e2eSetup(b)
	dst := &e2eRecord{}

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := client.Get(nil, key)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		// Extract each typed value the way real user code would.
		if v, ok := rec.Bins["i1"].(int); ok {
			dst.I1 = v
		}
		if v, ok := rec.Bins["i2"].(int); ok {
			dst.I2 = v
		}
		if v, ok := rec.Bins["i3"].(int); ok {
			dst.I3 = v
		}
		if v, ok := rec.Bins["i4"].(int); ok {
			dst.I4 = v
		}
		if v, ok := rec.Bins["s1"].(string); ok {
			dst.S1 = v
		}
		if v, ok := rec.Bins["s2"].(string); ok {
			dst.S2 = v
		}
		if v, ok := rec.Bins["s3"].(string); ok {
			dst.S3 = v
		}
		if v, ok := rec.Bins["s4"].(string); ok {
			dst.S4 = v
		}
		if v, ok := rec.Bins["b1"].([]byte); ok {
			dst.B1 = v
		}
		if v, ok := rec.Bins["b2"].([]byte); ok {
			dst.B2 = v
		}
		if v, ok := rec.Bins["b3"].([]byte); ok {
			dst.B3 = v
		}
		if v, ok := rec.Bins["b4"].([]byte); ok {
			dst.B4 = v
		}
	}
}

// BenchmarkE2E_GetObject exercises the reflection-based path end-to-end.
// Same destination as the sink benchmark — only the unmarshal mechanism
// differs.
func BenchmarkE2E_GetObject(b *testing.B) {
	key := e2eSetup(b)
	dst := &e2eRecord{}

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.GetObject(nil, key, dst); err != nil {
			b.Fatalf("get object: %v", err)
		}
	}
}

// BenchmarkE2E_GetSink exercises the new sink path end-to-end.
func BenchmarkE2E_GetSink(b *testing.B) {
	key := e2eSetup(b)
	dst := &e2eRecord{}

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.GetSink(nil, key, dst); err != nil {
			b.Fatalf("get sink: %v", err)
		}
	}
}

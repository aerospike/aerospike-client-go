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
	"strconv"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const batchSize = 50

func batchSetup(b *testing.B) []*as.Key {
	b.Helper()
	keys := make([]*as.Key, batchSize)
	bins := e2eBins()
	for i := 0; i < batchSize; i++ {
		key, err := as.NewKey(*namespace, "bench", "batch-"+strconv.Itoa(i))
		if err != nil {
			b.Fatalf("new key: %v", err)
		}
		if err := client.PutBins(nil, key, bins...); err != nil {
			b.Fatalf("put bins: %v", err)
		}
		keys[i] = key
	}
	return keys
}

// BenchmarkE2E_BatchGet measures the existing BinMap-based batch read.
// The post-read extraction loop mirrors what real callers must do.
func BenchmarkE2E_BatchGet(b *testing.B) {
	keys := batchSetup(b)
	dsts := make([]e2eRecord, batchSize)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recs, err := client.BatchGet(nil, keys)
		if err != nil {
			b.Fatalf("batch get: %v", err)
		}
		for j, rec := range recs {
			if rec == nil {
				continue
			}
			if v, ok := rec.Bins["i1"].(int); ok {
				dsts[j].I1 = v
			}
			if v, ok := rec.Bins["i2"].(int); ok {
				dsts[j].I2 = v
			}
			if v, ok := rec.Bins["i3"].(int); ok {
				dsts[j].I3 = v
			}
			if v, ok := rec.Bins["i4"].(int); ok {
				dsts[j].I4 = v
			}
			if v, ok := rec.Bins["s1"].(string); ok {
				dsts[j].S1 = v
			}
			if v, ok := rec.Bins["s2"].(string); ok {
				dsts[j].S2 = v
			}
			if v, ok := rec.Bins["s3"].(string); ok {
				dsts[j].S3 = v
			}
			if v, ok := rec.Bins["s4"].(string); ok {
				dsts[j].S4 = v
			}
			if v, ok := rec.Bins["b1"].([]byte); ok {
				dsts[j].B1 = v
			}
			if v, ok := rec.Bins["b2"].([]byte); ok {
				dsts[j].B2 = v
			}
			if v, ok := rec.Bins["b3"].([]byte); ok {
				dsts[j].B3 = v
			}
			if v, ok := rec.Bins["b4"].([]byte); ok {
				dsts[j].B4 = v
			}
		}
	}
}

// BenchmarkE2E_BatchGetSink measures the new batch sink path.
func BenchmarkE2E_BatchGetSink(b *testing.B) {
	keys := batchSetup(b)
	dsts := make([]e2eRecord, batchSize)
	sinks := make([]as.BinReceiver, batchSize)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset destinations and rewire sinks each iteration so the
		// previous read's residue doesn't taint subsequent ones.
		for j := range dsts {
			dsts[j] = e2eRecord{}
			sinks[j] = &dsts[j]
		}
		if _, err := client.BatchGetSink(nil, keys, sinks); err != nil {
			b.Fatalf("batch get sink: %v", err)
		}
	}
}

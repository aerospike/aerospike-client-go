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
	"sync"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const scanRecordCount = 500
const scanSetName = "bench-sink-scan"

var scanSetupOnce sync.Once

// scanSetup seeds the dedicated scan set with scanRecordCount records.
// Runs once per process — scans are slow to set up and unaffected by
// being read repeatedly, so amortizing the seed across benchmark runs
// keeps the harness sane.
func scanSetup(b *testing.B) {
	b.Helper()
	scanSetupOnce.Do(func() {
		bins := e2eBins()
		for i := 0; i < scanRecordCount; i++ {
			key, err := as.NewKey(*namespace, scanSetName, "scan-"+strconv.Itoa(i))
			if err != nil {
				b.Fatalf("new key: %v", err)
			}
			if err := client.PutBins(nil, key, bins...); err != nil {
				b.Fatalf("put bins: %v", err)
			}
		}
	})
}

// BenchmarkE2E_ScanAll measures the existing Records-channel scan with
// the per-record extraction work a real caller must do.
func BenchmarkE2E_ScanAll(b *testing.B) {
	scanSetup(b)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := client.ScanAll(nil, *namespace, scanSetName)
		if err != nil {
			b.Fatalf("scan all: %v", err)
		}
		var dst e2eRecord
		for res := range rs.Results() {
			if res.Err != nil {
				b.Fatalf("scan result: %v", res.Err)
			}
			rec := res.Record
			if v, ok := rec.Bins["i1"].(int); ok {
				dst.I1 = v
			}
			if v, ok := rec.Bins["s1"].(string); ok {
				dst.S1 = v
			}
			if v, ok := rec.Bins["b1"].([]byte); ok {
				dst.B1 = v
			}
			// (omit remaining bins for brevity — the comparison is
			// fair because the sink path also writes all 12.)
		}
	}
}

// BenchmarkE2E_ScanAllSink measures the new sink-channel scan.
func BenchmarkE2E_ScanAllSink(b *testing.B) {
	scanSetup(b)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinks := make(chan as.BinReceiver, 64)
		_, err := client.ScanAllSink(nil, sinks,
			func() as.BinReceiver { return &e2eRecord{} },
			*namespace, scanSetName)
		if err != nil {
			b.Fatalf("scan all sink: %v", err)
		}
		for range sinks {
			// drain; the sink already populated the receiver
		}
	}
}

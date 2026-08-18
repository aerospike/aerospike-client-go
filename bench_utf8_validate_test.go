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

// Benchmarks for the optional UTF-8 validation path added in CLIENT-4420.
//
// Goal: quantify the overhead a user takes on when they opt in to
// ClientPolicy.ValidateUTF8 — specifically, the cost of the validation
// hook added to Put / PutBins / Append / Prepend / Operate, e.g.:
//
//   func (clnt *Client) Put(policy *WritePolicy, key *Key, binMap BinMap) Error {
//       policy = clnt.getUsableWritePolicy(policy)
//
//       if clnt.utf8ValidationEnabled() {
//           if err := validateUTF8BinMap(binMap); err != nil {
//               return err
//           }
//       }
//       ...
//
// We exercise four layers:
//
//   1. utf8.ValidString itself, across string shapes and sizes. This is
//      the leaf cost everything else amortizes against. The Go runtime
//      implementation has a word-sized ASCII fast path (32 bytes/iter on
//      64-bit), so ASCII vs multibyte vs invalid show very different
//      profiles.
//
//   2. The validator entry points exposed in utf8_validate.go —
//      validateUTF8BinMap, validateUTF8Bins, validateUTF8Operations —
//      against realistic shapes (single string, many bins, nested
//      list/map). This is what a Put/PutBins/Operate caller actually
//      pays when validation is on.
//
//   3. The disabled path: utf8ValidationEnabled() returning false. This
//      is what every existing caller pays even if they never opt in.
//      Expect this to be effectively free, but it's worth confirming.
//
//   4. The rejection path: validating a BinMap whose first invalid byte
//      is at the end of a long string. This tells us the worst-case
//      latency when validation fails (relevant for SLO planning around
//      bad-input scenarios).
//
// To compare results before/after enabling validation, run twice with
// e.g.:
//
//   go test -run=^$ -bench=Benchmark_UTF8 -benchmem -benchtime=2s ./...
//
// All benchmarks live in `package aerospike` (not `aerospike_test`) so
// they can call the unexported validator functions directly without
// going through the network client.

import (
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"
)

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

// Three byte sequences that are not valid UTF-8 — same set used in
// utf8_validate_test.go so benchmark and correctness tests stay aligned.
var (
	benchBadRawBytes  = string([]byte{0xff, 0xfe, 0xfd})
	benchBadSurrogate = string([]byte{0xed, 0xa0, 0x80})
)

// Representative strings at multiple sizes. ASCII hits the word-sized
// fast path; the multibyte set forces per-rune decoding for every byte.
var (
	asciiShort  = strings.Repeat("a", 16)
	asciiMed    = strings.Repeat("a", 256)
	asciiLong   = strings.Repeat("a", 4096)
	asciiXLong  = strings.Repeat("a", 65536)
	multiShort  = strings.Repeat("é", 8)     // 16 bytes, 2-byte runes
	multiMed    = strings.Repeat("日", 86)    // 258 bytes, 3-byte runes
	multiLong   = strings.Repeat("🎉", 1024)  // 4096 bytes, 4-byte runes
	mixedMed    = strings.Repeat("aé日🎉", 64) // 640 bytes, mixed widths
	invalidLate = strings.Repeat("a", 4095) + string([]byte{0xff})
)

// Sink vars stop the compiler from eliding validation calls. Read by
// no-one; written every iteration. Per-benchmark sinks keep unrelated
// benchmarks from contending on the same cache line.
var (
	utfSinkBool bool
	utfSinkErr  Error
)

// ---------------------------------------------------------------------
// Layer 1: raw utf8.ValidString cost
// ---------------------------------------------------------------------
//
// These set a floor. Every validateUTF8* path eventually calls
// utf8.ValidString once per string leaf. If callers want to predict the
// cost of validation on a BinMap with N strings totaling M bytes, this
// is the per-byte / per-string number to extrapolate from.

func Benchmark_UTF8_ValidString_ASCII_16B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(asciiShort)
	}
	b.SetBytes(int64(len(asciiShort)))
}

func Benchmark_UTF8_ValidString_ASCII_256B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(asciiMed)
	}
	b.SetBytes(int64(len(asciiMed)))
}

func Benchmark_UTF8_ValidString_ASCII_4KB(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(asciiLong)
	}
	b.SetBytes(int64(len(asciiLong)))
}

func Benchmark_UTF8_ValidString_ASCII_64KB(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(asciiXLong)
	}
	b.SetBytes(int64(len(asciiXLong)))
}

func Benchmark_UTF8_ValidString_Multibyte_16B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(multiShort)
	}
	b.SetBytes(int64(len(multiShort)))
}

func Benchmark_UTF8_ValidString_Multibyte_258B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(multiMed)
	}
	b.SetBytes(int64(len(multiMed)))
}

func Benchmark_UTF8_ValidString_Multibyte_4KB(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(multiLong)
	}
	b.SetBytes(int64(len(multiLong)))
}

func Benchmark_UTF8_ValidString_Mixed_640B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(mixedMed)
	}
	b.SetBytes(int64(len(mixedMed)))
}

func Benchmark_UTF8_ValidString_InvalidShort_3B(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(benchBadRawBytes)
	}
	b.SetBytes(int64(len(benchBadRawBytes)))
}

// Worst case for rejection: the bad byte is at the end of a long
// ASCII run, so the validator pays full fast-path scan before failing.
func Benchmark_UTF8_ValidString_InvalidLate_4KB(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		utfSinkBool = utf8.ValidString(invalidLate)
	}
	b.SetBytes(int64(len(invalidLate)))
}

// ---------------------------------------------------------------------
// Layer 2: validator entry points (the actual hook cost)
// ---------------------------------------------------------------------
//
// These mirror what client.go calls inside the
// `if clnt.utf8ValidationEnabled() { ... }` block.

// Single-bin Put / PutBins shape. The most common shape in practice.
func Benchmark_UTF8_validateUTF8BinMap_1Bin_ASCII_64B(b *testing.B) {
	bm := BinMap{"s": strings.Repeat("a", 64)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

func Benchmark_UTF8_validateUTF8BinMap_1Bin_ASCII_4KB(b *testing.B) {
	bm := BinMap{"s": asciiLong}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

func Benchmark_UTF8_validateUTF8BinMap_1Bin_Multibyte_4KB(b *testing.B) {
	bm := BinMap{"s": multiLong}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// Non-string bins are a near-free walk: we want to see that cost too,
// since a user might enable ValidateUTF8 and write mostly numeric data.
func Benchmark_UTF8_validateUTF8BinMap_10Bins_Numeric(b *testing.B) {
	bm := BinMap{}
	for i := 0; i < 10; i++ {
		bm[stringsRepeatedKey(i)] = int64(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// 10 small string bins. Closer to a "wide record" workload.
func Benchmark_UTF8_validateUTF8BinMap_10Bins_ASCII_64B(b *testing.B) {
	bm := BinMap{}
	v := strings.Repeat("a", 64)
	for i := 0; i < 10; i++ {
		bm[stringsRepeatedKey(i)] = v
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// PutBins path uses []*Bin, not BinMap. Same scan, different surface.
func Benchmark_UTF8_validateUTF8Bins_10Bins_ASCII_64B(b *testing.B) {
	v := strings.Repeat("a", 64)
	bins := make([]*Bin, 10)
	for i := range bins {
		bins[i] = NewBin(stringsRepeatedKey(i), v)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8Bins(bins)
	}
}

// Nested list value. Recursion + per-element type switch dominates here,
// not utf8.ValidString itself.
func Benchmark_UTF8_validateUTF8BinMap_ListOf100Strings(b *testing.B) {
	list := make([]any, 100)
	v := strings.Repeat("a", 16)
	for i := range list {
		list[i] = v
	}
	bm := BinMap{"list": list}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// Map of 100 string->string. Worst case for the map[any]any branch
// (most realistic complex shape we'd see in a Put).
func Benchmark_UTF8_validateUTF8BinMap_MapOf100StringString(b *testing.B) {
	m := make(map[any]any, 100)
	v := strings.Repeat("a", 16)
	for i := 0; i < 100; i++ {
		m[stringsRepeatedKey(i)] = v
	}
	bm := BinMap{"m": m}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// Operate-path: validateUTF8Operations against a small batch of write ops.
func Benchmark_UTF8_validateUTF8Operations_5Ops_ASCII_64B(b *testing.B) {
	v := strings.Repeat("a", 64)
	ops := []*Operation{
		PutOp(NewBin("a", v)),
		PutOp(NewBin("b", v)),
		PutOp(NewBin("c", v)),
		AppendOp(NewBin("d", v)),
		PrependOp(NewBin("e", v)),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8Operations(ops)
	}
}

// ---------------------------------------------------------------------
// Layer 3: cost paid by callers who DO NOT enable validation
// ---------------------------------------------------------------------
//
// Every Put/PutBins/Operate now starts with
//   if clnt.utf8ValidationEnabled() { ... }
// We want this to be effectively free for the (default) off path. The
// real call goes through cluster.clientPolicy.Load() — the benchmark
// below isolates that hot read so we can quote a number.

func Benchmark_UTF8_utf8ValidationEnabled_Disabled(b *testing.B) {
	// utf8ValidationEnabled is a method on *Cluster. The method
	// short-circuits on nil, but for a fair "disabled" read we want to
	// exercise the atomic Load. Build the smallest viable cluster.
	clstr := newClusterForBench()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkBool = clstr.utf8ValidationEnabled()
	}
}

func Benchmark_UTF8_utf8ValidationEnabled_Enabled(b *testing.B) {
	clstr := newClusterForBench()
	p := clstr.clientPolicy.Load()
	pCopy := *p
	pCopy.ValidateUTF8 = true
	clstr.clientPolicy.Store(&pCopy)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkBool = clstr.utf8ValidationEnabled()
	}
}

// Nil-cluster short-circuit. The cheapest possible disabled-path read —
// included to document the floor.
func Benchmark_UTF8_utf8ValidationEnabled_NilCluster(b *testing.B) {
	var clstr *Cluster
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkBool = clstr.utf8ValidationEnabled()
	}
}

// ---------------------------------------------------------------------
// Layer 4: rejection latency
// ---------------------------------------------------------------------
//
// What does a Put pay when the BinMap contains an invalid string? The
// validator returns on the first failure, but if the bad string is long
// and the bad byte is at the end, the full string is scanned.

func Benchmark_UTF8_validateUTF8BinMap_Invalid_FirstByte(b *testing.B) {
	bm := BinMap{"s": benchBadRawBytes}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

func Benchmark_UTF8_validateUTF8BinMap_Invalid_Surrogate(b *testing.B) {
	bm := BinMap{"s": benchBadSurrogate}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

func Benchmark_UTF8_validateUTF8BinMap_Invalid_LateByte_4KB(b *testing.B) {
	bm := BinMap{"s": invalidLate}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utfSinkErr = validateUTF8BinMap(bm)
	}
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

// stringsRepeatedKey returns a short, deterministic, unique bin name.
// Cheaper than fmt.Sprintf for benchmark setup.
func stringsRepeatedKey(i int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz"
	if i < len(alpha) {
		return string(alpha[i])
	}
	return string(alpha[i%len(alpha)]) + string(alpha[(i/len(alpha))%len(alpha)])
}

// newClusterForBench wires just enough of a Cluster for
// utf8ValidationEnabled() to exercise its real path (atomic.Load on
// clientPolicy) without opening any sockets.
//
// utf8ValidationEnabled reads:   clstr.clientPolicy.Load()
// So we need a populated atomic pointer; nothing else is touched.
// This avoids dragging in NewClient (which dials).
func newClusterForBench() *Cluster {
	p := NewClientPolicy()
	ap := &atomic.Pointer[ClientPolicy]{}
	ap.Store(p)
	return &Cluster{
		clientPolicy: ap,
	}
}

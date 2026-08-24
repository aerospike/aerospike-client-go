//go:build go1.27

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

// Benchmarks measuring the marginal cost of accepting context.Context on the
// existing Get / InsertRows fast paths, against the existing no-context
// calls. Run against a live cluster at 127.0.0.1:3000, namespace "test".
//
//   go test ./sdk/ -run '^$' -bench 'BenchmarkCtx' -benchmem -benchtime 3s
//
// package sdk (internal), so it can call the unexported GetCtx/ExecuteCtx
// prototypes directly.

package sdk

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// fullyLoadedCtx builds a context carrying everything at once -- a deadline
// plus several request-scoped key/value pairs, the shape a real production
// context actually has (trace id, span id, request id, tenant id), not just
// a bare timeout. This is the exact "timeout, deadline, key-value, etc."
// scenario under dispute.
type traceIDKey struct{}
type spanIDKey struct{}
type requestIDKey struct{}
type tenantIDKey struct{}

func fullyLoadedCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx = context.WithValue(ctx, traceIDKey{}, "trace-4f9a2b7c")
	ctx = context.WithValue(ctx, spanIDKey{}, "span-19c3")
	ctx = context.WithValue(ctx, requestIDKey{}, "req-8827361")
	ctx = context.WithValue(ctx, tenantIDKey{}, "tenant-42")
	return ctx, cancel
}

// goroutineSample forces a GC (to let anything finished settle) then reports
// the live goroutine count -- used as a before/after pair inside each
// benchmark so every row in the results table gets its own actual number,
// not just the ns/op-B/op-allocs/op triple go test -benchmem reports
// natively (it does not track goroutines at all -- that's why this exists
// as an explicit, separate measurement).
func goroutineSample() int {
	runtime.GC()
	return runtime.NumGoroutine()
}

func benchCluster(b *testing.B) (*Session, *DataSet) {
	b.Helper()
	c, err := NewClusterDefinition("127.0.0.1", 3000).Connect()
	if err != nil {
		b.Skipf("no live cluster at 127.0.0.1:3000: %v", err)
	}
	b.Cleanup(c.Close)
	s, err := c.CreateSession(nil)
	if err != nil {
		b.Fatalf("CreateSession: %v", err)
	}
	ds, err := DataSetOf("test", fmt.Sprintf("zzbenchctx_%d", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("DataSetOf: %v", err)
	}
	return s, ds
}

// --- Get vs GetCtx ---

func BenchmarkCtx_Get_NoCtx(b *testing.B) {
	s, ds := benchCluster(b)
	key := ds.Key("bench-get-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		b.Fatalf("seed Put: %v", err)
	}
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Get(s, key, []string{"v"}); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_Get_Background(b *testing.B) {
	s, ds := benchCluster(b)
	key := ds.Key("bench-get-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		if _, err := GetCtx(s, ctx, key, []string{"v"}); err != nil {
			b.Fatalf("GetCtx: %v", err)
		}
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_Get_RealDeadline(b *testing.B) {
	s, ds := benchCluster(b)
	key := ds.Key("bench-get-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		b.Fatalf("seed Put: %v", err)
	}
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		if _, err := GetCtx(s, ctx, key, []string{"v"}); err != nil {
			cancel()
			b.Fatalf("GetCtx: %v", err)
		}
		cancel()
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_Get_FullyLoaded(b *testing.B) {
	s, ds := benchCluster(b)
	key := ds.Key("bench-get-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		b.Fatalf("seed Put: %v", err)
	}
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := fullyLoadedCtx()
		if _, err := GetCtx(s, ctx, key, []string{"v"}); err != nil {
			cancel()
			b.Fatalf("GetCtx: %v", err)
		}
		cancel()
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

// --- InsertRows vs ExecuteCtx ---

func BenchmarkCtx_InsertRows_NoCtx(b *testing.B) {
	s, ds := benchCluster(b)
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("row-%d", i)
		if _, err := s.InsertRows(ds).Bins("v").Row(id, i).Execute(); err != nil {
			b.Fatalf("InsertRows: %v", err)
		}
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_InsertRows_Background(b *testing.B) {
	s, ds := benchCluster(b)
	ctx := context.Background()
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("row-%d", i)
		if _, err := s.InsertRows(ds).Bins("v").Row(id, i).ExecuteCtx(ctx); err != nil {
			b.Fatalf("InsertRows.ExecuteCtx: %v", err)
		}
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_InsertRows_RealDeadline(b *testing.B) {
	s, ds := benchCluster(b)
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("row-%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := s.InsertRows(ds).Bins("v").Row(id, i).ExecuteCtx(ctx); err != nil {
			cancel()
			b.Fatalf("InsertRows.ExecuteCtx: %v", err)
		}
		cancel()
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

func BenchmarkCtx_InsertRows_FullyLoaded(b *testing.B) {
	s, ds := benchCluster(b)
	before := goroutineSample()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("row-%d", i)
		ctx, cancel := fullyLoadedCtx()
		if _, err := s.InsertRows(ds).Bins("v").Row(id, i).ExecuteCtx(ctx); err != nil {
			cancel()
			b.Fatalf("InsertRows.ExecuteCtx: %v", err)
		}
		cancel()
	}
	b.StopTimer()
	after := goroutineSample()
	b.Logf("goroutines: before=%d after=%d delta=%d (b.N=%d)", before, after, after-before, b.N)
}

// --- Direct settlement of "does passing context spawn a goroutine" ---

func TestCtx_NoGoroutineLeak(t *testing.T) {
	c, err := NewClusterDefinition("127.0.0.1", 3000).Connect()
	if err != nil {
		t.Skipf("no live cluster at 127.0.0.1:3000: %v", err)
	}
	defer c.Close()
	s, err := c.CreateSession(nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ds, err := DataSetOf("test", fmt.Sprintf("zzbenchctx_leak_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("DataSetOf: %v", err)
	}
	key := ds.Key("leak-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	runtime.GC()
	before := runtime.NumGoroutine()

	const n = 2000
	for i := 0; i < n; i++ {
		ctx, cancel := fullyLoadedCtx() // timeout + deadline + 4 key/value pairs, every call
		if _, err := GetCtx(s, ctx, key, []string{"v"}); err != nil {
			cancel()
			t.Fatalf("GetCtx: %v", err)
		}
		cancel()
	}

	runtime.GC()
	after := runtime.NumGoroutine()

	t.Logf("goroutines before=%d after=%d (n=%d calls, each with a fully-loaded context: timeout+deadline+4 values)", before, after, n)
	if after > before+2 { // small slack for GC/runtime bookkeeping goroutines
		t.Fatalf("goroutine count grew from %d to %d after %d ctx-bearing calls -- possible leak", before, after, n)
	}
}

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

// Fixed-size concurrent load tests -- not go test -bench's auto-calibrated
// b.N, but an exact, known op count (200,000) driven by real concurrency
// (many goroutines genuinely in flight at once), each operation building its
// own fresh context, the way independent concurrent requests actually look
// in a real service. Tracks PEAK live goroutines during the run, not just a
// before/after snapshot -- that's the number that actually answers "does
// context cost one extra goroutine per in-flight call at scale."
//
//   go test ./sdk/ -run 'TestRealWorld' -v -timeout 300s

package sdk

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// cpuTimeSnapshot reports cumulative user+system CPU time consumed by this
// process so far (via getrusage -- darwin/linux; not portable to Windows,
// fine for this throwaway prototype).
func cpuTimeSnapshot() (userSec, sysSec float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userSec = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sysSec = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return
}

const (
	realWorldTotalOps    = 200_000
	realWorldConcurrency = 500
)

// peakSampler polls runtime.NumGoroutine() (cheap, every tick) and
// runtime.ReadMemStats (heavier, so sampled at a coarser cadence) in the
// background, tracking the maximum goroutine count and maximum live heap
// (HeapAlloc) observed until stopped. One sampler goroutine per run -- a
// known, fixed, disclosed cost, not counted as part of "per call."
type peakSampler struct {
	stop           chan struct{}
	done           chan struct{}
	peakGoroutines int64
	peakHeapAlloc  int64
}

func casMax(addr *int64, v int64) {
	for {
		cur := atomic.LoadInt64(addr)
		if v <= cur || atomic.CompareAndSwapInt64(addr, cur, v) {
			return
		}
	}
}

func startPeakSampler() *peakSampler {
	p := &peakSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		var m runtime.MemStats
		tick := 0
		ticker := time.NewTicker(500 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				casMax(&p.peakGoroutines, int64(runtime.NumGoroutine()))
				// ReadMemStats is far heavier than NumGoroutine -- sample it
				// only every 20th tick (~10ms cadence) so the sampler itself
				// doesn't skew the throughput/latency being measured.
				tick++
				if tick%20 == 0 {
					runtime.ReadMemStats(&m)
					casMax(&p.peakHeapAlloc, int64(m.HeapAlloc))
				}
			}
		}
	}()
	return p
}

func (p *peakSampler) Stop() (peakGoroutines, peakHeapAlloc int64) {
	close(p.stop)
	<-p.done
	return atomic.LoadInt64(&p.peakGoroutines), atomic.LoadInt64(&p.peakHeapAlloc)
}

// runConcurrentLoad fires exactly totalOps calls to op, at most concurrency
// in flight at once, and reports wall-clock duration, peak goroutines, and
// peak/resting heap memory observed during and after the run.
func runConcurrentLoad(t *testing.T, name string, totalOps, concurrency int, op func(i int) error) {
	t.Helper()

	var mBefore, mAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBefore)
	restBefore := runtime.NumGoroutine()
	sampler := startPeakSampler()
	userBefore, sysBefore := cpuTimeSnapshot()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var failures atomic.Int64
	start := time.Now()

	for i := 0; i < totalOps; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := op(i); err != nil {
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	peakGoroutines, peakHeapAlloc := sampler.Stop()
	userAfter, sysAfter := cpuTimeSnapshot()
	runtime.GC()
	runtime.ReadMemStats(&mAfter)
	restAfter := runtime.NumGoroutine()

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	totalAllocDuringRun := mAfter.TotalAlloc - mBefore.TotalAlloc
	bytesPerOp := float64(totalAllocDuringRun) / float64(totalOps)

	userDelta := userAfter - userBefore
	sysDelta := sysAfter - sysBefore
	totalCPUSeconds := userDelta + sysDelta
	avgCoresBusy := totalCPUSeconds / elapsed.Seconds()
	pctOfMachine := avgCoresBusy / float64(runtime.NumCPU()) * 100

	t.Logf("%s: %d ops, concurrency=%d, elapsed=%s, %.0f ops/sec, failures=%d",
		name, totalOps, concurrency, elapsed, opsPerSec, failures.Load())
	t.Logf("  goroutines: resting(before=%d after=%d) peak-during-run=%d (peak-concurrency=%d + baseline)",
		restBefore, restAfter, peakGoroutines, concurrency)
	t.Logf("  heap: resting HeapAlloc(before=%s after=%s delta=%s) peak-during-run=%s, total allocated over run=%s (%.0f B/op)",
		humanBytes(mBefore.HeapAlloc), humanBytes(mAfter.HeapAlloc), humanBytesSigned(int64(mAfter.HeapAlloc)-int64(mBefore.HeapAlloc)),
		humanBytes(uint64(peakHeapAlloc)), humanBytes(totalAllocDuringRun), bytesPerOp)
	t.Logf("  cpu: user=%.2fs sys=%.2fs total=%.2fs, avg-cores-busy=%.2f of %d logical CPUs (%.1f%%), %.1f µs-CPU/op",
		userDelta, sysDelta, totalCPUSeconds, avgCoresBusy, runtime.NumCPU(), pctOfMachine, totalCPUSeconds*1e6/float64(totalOps))

	if got := failures.Load(); got != 0 {
		t.Fatalf("%s: %d/%d operations failed", name, got, totalOps)
	}
}

func humanBytesSigned(b int64) string {
	if b < 0 {
		return "-" + humanBytes(uint64(-b))
	}
	return "+" + humanBytes(uint64(b))
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func realWorldFixture(t *testing.T) (*Session, *DataSet) {
	t.Helper()
	c, err := NewClusterDefinition("127.0.0.1", 3000).Connect()
	if err != nil {
		t.Skipf("no live cluster at 127.0.0.1:3000: %v", err)
	}
	t.Cleanup(c.Close)
	s, err := c.CreateSession(nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ds, err := DataSetOf("test", fmt.Sprintf("zzrealworld_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("DataSetOf: %v", err)
	}
	return s, ds
}

// --- Reads: 200,000 concurrent Gets against one hot key ---

func TestRealWorld_Get_NoCtx(t *testing.T) {
	s, ds := realWorldFixture(t)
	key := ds.Key("hot-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	runConcurrentLoad(t, "Get/NoCtx", realWorldTotalOps, realWorldConcurrency, func(i int) error {
		_, err := Get(s, key, []string{"v"})
		return err
	})
}

func TestRealWorld_Get_CtxPerRequest(t *testing.T) {
	s, ds := realWorldFixture(t)
	key := ds.Key("hot-key")
	if err := s.Put(key, as.BinMap{"v": 1}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	runConcurrentLoad(t, "Get/CtxPerRequest(fully-loaded)", realWorldTotalOps, realWorldConcurrency, func(i int) error {
		// Every single one of the 200,000 calls builds its OWN fresh
		// context, exactly like 200,000 independent incoming requests each
		// carrying their own deadline and request-scoped values would.
		ctx, cancel := fullyLoadedCtx()
		defer cancel()
		_, err := GetCtx(s, ctx, key, []string{"v"})
		return err
	})
}

// --- Writes: 200,000 concurrent unique-key inserts ---

func TestRealWorld_InsertRows_NoCtx(t *testing.T) {
	s, ds := realWorldFixture(t)
	runConcurrentLoad(t, "InsertRows/NoCtx", realWorldTotalOps, realWorldConcurrency, func(i int) error {
		id := fmt.Sprintf("row-%d", i)
		_, err := s.InsertRows(ds).Bins("v").Row(id, i).Execute()
		return err
	})
}

func TestRealWorld_InsertRows_CtxPerRequest(t *testing.T) {
	s, ds := realWorldFixture(t)
	runConcurrentLoad(t, "InsertRows/CtxPerRequest(fully-loaded)", realWorldTotalOps, realWorldConcurrency, func(i int) error {
		id := fmt.Sprintf("row-%d", i)
		ctx, cancel := fullyLoadedCtx()
		defer cancel()
		_, err := s.InsertRows(ds).Bins("v").Row(id, i).ExecuteCtx(ctx)
		return err
	})
}
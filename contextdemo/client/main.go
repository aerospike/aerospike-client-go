// Client binary for the real-comparison demo: no-context baseline vs a
// fully-correct context-aware implementation, using either
// context.AfterFunc or a hand-rolled goroutine+select to react to explicit
// cancellation. Dials the separate stuck-server binary (run its
// server/main.go first) so goroutine/heap/CPU measurements here reflect
// only client-side cost, not server-side goroutines sharing the same
// process.
//
//	go run .   (with the server already running)
package main

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const serverAddr = "127.0.0.1:19191"

type traceIDKey struct{}
type requestIDKey struct{}
type appIDKey struct{}

const callTimeout = 1 * time.Millisecond // SDK's own default timeout

// ctxTimeout is deliberately tighter than callTimeout, so prodCtx's
// deadline always wins the earliest() comparison inside writeWithAfterFunc --
// proving ctx can tighten the default, not just tie it.
const ctxTimeout = 500 * time.Microsecond

// prodCtx builds a per-call context the way a real production request would:
// its own deadline plus request-scoped trace/request/app identifiers.
func prodCtx(i int) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	ctx = context.WithValue(ctx, traceIDKey{}, "trace-"+strconv.Itoa(i))
	ctx = context.WithValue(ctx, requestIDKey{}, "req-"+strconv.Itoa(i))
	ctx = context.WithValue(ctx, appIDKey{}, "billing-api")
	return ctx, cancel
}

// writeWithAfterFunc is the actual production-shape implementation: the
// deadline is mapped straight onto the socket up front (so the common
// WithTimeout case never needs AfterFunc's callback to fire at all -- the
// socket deadline alone bounds it), AND context.AfterFunc is wired to catch
// an explicit early cancel() or a context with no deadline at all (e.g.
// http.Request.Context() on client disconnect, an errgroup sibling failing).
func writeWithAfterFunc(ctx context.Context, conn net.Conn) error {
	deadline := time.Now().Add(callTimeout) // default that always applies
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d // ctx can only tighten it, never remove it
	}
	conn.SetDeadline(deadline)

	stop := context.AfterFunc(ctx, func() {
		conn.SetDeadline(time.Now()) // forces the blocked Read to return immediately
	})
	defer stop()

	var buf [1]byte
	_, err := conn.Read(buf[:])
	return err
}

// writeFullyCorrectWithHandRolled has the same completeness contract as
// writeWithAfterFunc (deadline mapped up front, explicit cancel honored) but
// reacts to cancellation with a hand-rolled goroutine + select instead of
// context.AfterFunc -- the real, apples-to-apples "which mechanism" test.
func writeFullyCorrectWithHandRolled(ctx context.Context, conn net.Conn) error {
	deadline := time.Now().Add(callTimeout) // default that always applies
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d // ctx can only tighten it, never remove it
	}
	conn.SetDeadline(deadline)

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.SetDeadline(time.Now()) // forces the blocked Read to return immediately
		case <-done:
		}
	}()
	defer close(done)

	var buf [1]byte
	_, err := conn.Read(buf[:])
	return err
}

// writeWithNoCtx is the true zero-context baseline: no context.Context is
// ever constructed. Bounding the call is done the pre-context way -- a
// plain time.Duration mapped straight onto the socket deadline, exactly
// like the existing (non-ctx) Aerospike client does today.
func writeWithNoCtx(d time.Duration, conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(d))

	var buf [1]byte
	_, err := conn.Read(buf[:])
	return err
}

func runNoCtx(i int, conn net.Conn) error {
	return writeWithNoCtx(callTimeout, conn)
}

func runFullyCorrect(i int, conn net.Conn) error {
	ctx, cancel := prodCtx(i)
	defer cancel()
	return writeWithAfterFunc(ctx, conn)
}

func runFullyCorrectWithHandRolled(i int, conn net.Conn) error {
	ctx, cancel := prodCtx(i)
	defer cancel()
	return writeFullyCorrectWithHandRolled(ctx, conn)
}

func runFullyCorrectWithBgContext(i int, conn net.Conn) error {
	ctx := context.Background()
	return writeWithAfterFunc(ctx, conn)
}

// runWithCancel simulates a genuine external explicit cancel() -- an
// errgroup sibling failing, an http.Request.Context() closing on client
// disconnect -- as opposed to every other variant, where the deadline fires
// via context.WithTimeout's own internal timer. The context here carries no
// deadline at all, so writeWithAfterFunc's own default (callTimeout) would
// otherwise be what bounds it; cancel() is scheduled at half that, so it
// unambiguously wins the race and is provably what caused the abort.
func runWithCancel(i int, conn net.Conn) error {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(callTimeout/2, cancel)
	defer cancel()
	return writeWithAfterFunc(ctx, conn)
}

// runWithCancelHandRolled is the same explicit-cancel scenario as
// runWithCancel, but through writeFullyCorrectWithHandRolled -- the
// apples-to-apples "which mechanism reacts to a genuine external cancel
// more cheaply" test.
func runWithCancelHandRolled(i int, conn net.Conn) error {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(callTimeout/2, cancel)
	defer cancel()
	return writeFullyCorrectWithHandRolled(ctx, conn)
}

// connPool is a fixed-capacity connection pool: at most `capacity`
// connections exist across the whole pool at any time, shared across all
// workers via borrow/return rather than one dedicated connection per
// worker. Since aborting a call now only resets the deadline (never closes
// the connection), a connection is never actually invalidated -- put()
// always returns it to the pool, no discard-and-redial path needed.
type connPool struct {
	addr  string
	slots chan struct{}
	idle  chan net.Conn
}

func newConnPool(addr string, capacity int) *connPool {
	p := &connPool{
		addr:  addr,
		slots: make(chan struct{}, capacity),
		idle:  make(chan net.Conn, capacity),
	}
	for i := 0; i < capacity; i++ {
		p.slots <- struct{}{}
	}
	return p
}

func (p *connPool) get() net.Conn {
	<-p.slots
	select {
	case c := <-p.idle:
		return c
	default:
		c, err := net.Dial("tcp", p.addr)
		if err != nil {
			panic(err)
		}
		return c
	}
}

func (p *connPool) put(c net.Conn) {
	p.idle <- c
	p.slots <- struct{}{}
}

func cpuTime() (userSec, sysSec float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6, float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}

// run drives totalRequests calls to fn, at a fixed concurrency of reused
// connections. fn owns its own per-call setup (building a ctx or not), so
// each variant's true cost -- including whether it allocates a
// context.Context at all -- is captured faithfully.
func run(name, addr string, totalRequests, concurrency int, fn func(i int, conn net.Conn) error) {
	runtime.GC()
	before := runtime.NumGoroutine()
	userBefore, sysBefore := cpuTime()

	var peakGoroutines, peakHeap int64
	var latSumNanos, latMinNanos, latMaxNanos int64
	latMinNanos = int64(^uint64(0) >> 1)

	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		var m runtime.MemStats
		ticker := time.NewTicker(500 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				n := int64(runtime.NumGoroutine())
				for {
					cur := atomic.LoadInt64(&peakGoroutines)
					if n <= cur || atomic.CompareAndSwapInt64(&peakGoroutines, cur, n) {
						break
					}
				}
				runtime.ReadMemStats(&m)
				h := int64(m.HeapAlloc)
				for {
					cur := atomic.LoadInt64(&peakHeap)
					if h <= cur || atomic.CompareAndSwapInt64(&peakHeap, cur, h) {
						break
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	pool := newConnPool(addr, concurrency)
	sem := make(chan struct{}, concurrency)
	start := time.Now()

	for i := 0; i < totalRequests; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			conn := pool.get()
			callStart := time.Now()
			_ = fn(i, conn)
			d := int64(time.Since(callStart))
			pool.put(conn)

			atomic.AddInt64(&latSumNanos, d)
			for {
				cur := atomic.LoadInt64(&latMaxNanos)
				if d <= cur || atomic.CompareAndSwapInt64(&latMaxNanos, cur, d) {
					break
				}
			}
			for {
				cur := atomic.LoadInt64(&latMinNanos)
				if d >= cur || atomic.CompareAndSwapInt64(&latMinNanos, cur, d) {
					break
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	close(stopSampler)
	<-samplerDone
	userAfter, sysAfter := cpuTime()
	runtime.GC()
	after := runtime.NumGoroutine()

	avgLatency := time.Duration(latSumNanos / int64(totalRequests))

	fmt.Printf("%s requests=%d concurrency=%d elapsed=%s\n", name, totalRequests, concurrency, elapsed.Round(time.Millisecond))
	fmt.Printf("  per-call latency (proof cancellation actually cut the blocked read short): min=%s avg=%s max=%s (configured timeout=%s)\n",
		time.Duration(latMinNanos), avgLatency, time.Duration(latMaxNanos), callTimeout)
	fmt.Printf("  goroutines: before=%d peak=%d after=%d\n", before, atomic.LoadInt64(&peakGoroutines), after)
	fmt.Printf("  heap: peak=%.2fMiB\n", float64(atomic.LoadInt64(&peakHeap))/1024/1024)
	fmt.Printf("  cpu: user=%.2fs sys=%.2fs\n", userAfter-userBefore, sysAfter-sysBefore)
}

func main() {
	const (
		totalRequests = 300_000
		concurrency   = 1000
	)

	//run("writeWithNoCtx", serverAddr, totalRequests, concurrency, runNoCtx)
	//run("writeWithAfterFunc", serverAddr, totalRequests, concurrency, runFullyCorrect)
	//run("writeFullyCorrectWithHandRolled", serverAddr, totalRequests, concurrency, runFullyCorrectWithHandRolled)
	//run("writeAfterFuncWithBgContext", serverAddr, totalRequests, concurrency, runFullyCorrectWithBgContext)
	//run("runWithCancel", serverAddr, totalRequests, concurrency, runWithCancel)
	run("runWithCancelHandRolled", serverAddr, totalRequests, concurrency, runWithCancelHandRolled)
}

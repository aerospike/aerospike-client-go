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

package histogram

import (
	"sync"
	"sync/atomic"
)

// LatencyBuckets is the shared learn-metrics / asadm range histogram:
// bucket 0 is <= 1 unit, then each following bucket doubles by latencyShift.
// Example (columns=7, shift=1): <=1, >1, >2, >4, >8, >16, >32.
//
// This type is not safe for concurrent writers; use SyncLatencyBuckets on the
// command hot path.
type LatencyBuckets struct {
	columns int
	shift   int

	Buckets []uint64 `json:"buckets"`
	Min     uint64   `json:"min"`
	Max     uint64   `json:"max"`
	Sum     float64  `json:"sum"`
	Count   uint64   `json:"count"`
}

// NewLatencyBuckets creates a shift-based latency histogram.
func NewLatencyBuckets(columns, shift int) *LatencyBuckets {
	if columns < 1 {
		columns = 1
	}
	if shift < 1 {
		shift = 1
	}
	return &LatencyBuckets{
		columns: columns,
		shift:   shift,
		Buckets: make([]uint64, columns),
	}
}

// Columns returns the number of buckets.
func (h *LatencyBuckets) Columns() int { return h.columns }

// Shift returns the power-of-two spacing between buckets.
func (h *LatencyBuckets) Shift() int { return h.shift }

// Index returns the bucket for an elapsed value already converted to the
// configured latency unit (milliseconds or microseconds).
func LatencyBucketIndex(elapsed uint64, columns, shift int) int {
	if columns < 1 {
		return 0
	}
	if elapsed <= 1 {
		return 0
	}
	if shift < 1 {
		shift = 1
	}
	limit := uint64(1)
	for i := 1; i < columns; i++ {
		limit <<= uint(shift)
		if elapsed <= limit {
			return i
		}
	}
	return columns - 1
}

func (h *LatencyBuckets) Add(elapsed uint64) {
	if len(h.Buckets) == 0 {
		return
	}
	if h.Count == 0 {
		h.Min = elapsed
		h.Max = elapsed
	} else {
		if elapsed > h.Max {
			h.Max = elapsed
		} else if elapsed < h.Min {
			h.Min = elapsed
		}
	}
	h.Sum += float64(elapsed)
	h.Count++
	h.Buckets[LatencyBucketIndex(elapsed, len(h.Buckets), h.shift)]++
}

func (h *LatencyBuckets) Clone() *LatencyBuckets {
	b := make([]uint64, len(h.Buckets))
	copy(b, h.Buckets)
	return &LatencyBuckets{
		columns: h.columns,
		shift:   h.shift,
		Buckets: b,
		Min:     h.Min,
		Max:     h.Max,
		Sum:     h.Sum,
		Count:   h.Count,
	}
}

func (h *LatencyBuckets) Reset() {
	for i := range h.Buckets {
		h.Buckets[i] = 0
	}
	h.Min = 0
	h.Max = 0
	h.Sum = 0
	h.Count = 0
}

func (h *LatencyBuckets) CloneAndReset() *LatencyBuckets {
	res := h.Clone()
	h.Reset()
	return res
}

func (h *LatencyBuckets) Reshape(columns, shift int) {
	if columns < 1 {
		columns = 1
	}
	if shift < 1 {
		shift = 1
	}
	if h.columns == columns && h.shift == shift && len(h.Buckets) == columns {
		return
	}
	h.columns = columns
	h.shift = shift
	h.Buckets = make([]uint64, columns)
	h.Min = 0
	h.Max = 0
	h.Sum = 0
	h.Count = 0
}

func (h *LatencyBuckets) Merge(other *LatencyBuckets) {
	if other == nil || len(h.Buckets) != len(other.Buckets) {
		return
	}
	if other.Count == 0 {
		return
	}
	// Gate on Count, not Min==0: sub-ms samples record Min=0 in millisecond units.
	if h.Count == 0 || other.Min < h.Min {
		h.Min = other.Min
	}
	if other.Max > h.Max {
		h.Max = other.Max
	}
	h.Sum += other.Sum
	h.Count += other.Count
	for i := range h.Buckets {
		h.Buckets[i] += other.Buckets[i]
	}
}

// latencyShape is an immutable bucket array + layout. Hot-path Add loads the
// pointer once; Reshape swaps in a new shape so concurrent Adds never write a
// freed slice (Java LatencyBuckets AtomicLong[] pattern).
type latencyShape struct {
	columns int
	shift   int
	buckets []atomic.Uint64
}

func newLatencyShape(columns, shift int) *latencyShape {
	if columns < 1 {
		columns = 1
	}
	if shift < 1 {
		shift = 1
	}
	return &latencyShape{
		columns: columns,
		shift:   shift,
		buckets: make([]atomic.Uint64, columns),
	}
}

const latencyMinUnset = ^uint64(0)

// SyncLatencyBuckets is the concurrent shared MUST latency histogram.
// Bucket counts, sum, and count use atomics (Java AtomicLong / LongAdder shape).
// min/max use CAS. Reshape/Reset serialize only the rare layout-change path.
type SyncLatencyBuckets struct {
	shape atomic.Pointer[latencyShape]
	count atomic.Uint64
	sum   atomic.Uint64 // sum of elapsed units (exact; Clone exposes float64)
	min   atomic.Uint64 // latencyMinUnset when empty
	max   atomic.Uint64
	// reshapeMu serializes Reshape/Reset against each other only.
	reshapeMu sync.Mutex
}

// NewSyncLatencyBuckets creates an empty concurrent latency histogram.
func NewSyncLatencyBuckets(columns, shift int) *SyncLatencyBuckets {
	h := &SyncLatencyBuckets{}
	h.shape.Store(newLatencyShape(columns, shift))
	h.min.Store(latencyMinUnset)
	return h
}

// SyncLatencyBucketsFromSnapshot builds a concurrent histogram seeded from a
// point-in-time LatencyBuckets (tend drain / aggregate helpers).
func SyncLatencyBucketsFromSnapshot(lb *LatencyBuckets) SyncLatencyBuckets {
	if lb == nil || len(lb.Buckets) == 0 {
		return *NewSyncLatencyBuckets(1, 1)
	}
	h := NewSyncLatencyBuckets(lb.columns, lb.shift)
	shape := h.shape.Load()
	for i, v := range lb.Buckets {
		if i < len(shape.buckets) {
			shape.buckets[i].Store(v)
		}
	}
	h.count.Store(lb.Count)
	h.sum.Store(uint64(lb.Sum))
	if lb.Count > 0 {
		h.min.Store(lb.Min)
		h.max.Store(lb.Max)
	}
	return *h
}

// Add records one elapsed sample. Lock-free on the hot path.
func (h *SyncLatencyBuckets) Add(elapsed uint64) {
	if h == nil {
		return
	}
	shape := h.shape.Load()
	if shape == nil || len(shape.buckets) == 0 {
		return
	}
	idx := LatencyBucketIndex(elapsed, len(shape.buckets), shape.shift)
	shape.buckets[idx].Add(1)
	h.count.Add(1)
	h.sum.Add(elapsed)

	// max: CAS upward
	for {
		old := h.max.Load()
		if elapsed <= old {
			break
		}
		if h.max.CompareAndSwap(old, elapsed) {
			break
		}
	}
	// min: unset sentinel or CAS downward
	for {
		old := h.min.Load()
		if old != latencyMinUnset && elapsed >= old {
			break
		}
		if h.min.CompareAndSwap(old, elapsed) {
			break
		}
	}
}

// Clone returns a point-in-time non-concurrent snapshot. Concurrent Add may
// interleave; acceptable for metrics export (same tradeoff as Java atomics).
func (h *SyncLatencyBuckets) Clone() *LatencyBuckets {
	if h == nil {
		return NewLatencyBuckets(1, 1)
	}
	shape := h.shape.Load()
	if shape == nil {
		return NewLatencyBuckets(1, 1)
	}
	out := NewLatencyBuckets(shape.columns, shape.shift)
	for i := range shape.buckets {
		out.Buckets[i] = shape.buckets[i].Load()
	}
	out.Count = h.count.Load()
	out.Sum = float64(h.sum.Load())
	out.Max = h.max.Load()
	min := h.min.Load()
	if out.Count == 0 || min == latencyMinUnset {
		out.Min = 0
		out.Max = 0
	} else {
		out.Min = min
	}
	return out
}

// CloneAndReset snapshots then zeros the live counters (tend drain).
func (h *SyncLatencyBuckets) CloneAndReset() *LatencyBuckets {
	snap := h.Clone()
	h.Reset()
	return snap
}

// Reshape replaces the bucket layout when columns/shift change. Resets samples.
func (h *SyncLatencyBuckets) Reshape(columns, shift int) {
	if h == nil {
		return
	}
	if columns < 1 {
		columns = 1
	}
	if shift < 1 {
		shift = 1
	}
	h.reshapeMu.Lock()
	defer h.reshapeMu.Unlock()
	cur := h.shape.Load()
	if cur != nil && cur.columns == columns && cur.shift == shift {
		return
	}
	h.shape.Store(newLatencyShape(columns, shift))
	h.count.Store(0)
	h.sum.Store(0)
	h.max.Store(0)
	h.min.Store(latencyMinUnset)
}

// Reset zeros counters and buckets, keeping the current layout.
func (h *SyncLatencyBuckets) Reset() {
	if h == nil {
		return
	}
	h.reshapeMu.Lock()
	defer h.reshapeMu.Unlock()
	cur := h.shape.Load()
	cols, shift := 1, 1
	if cur != nil {
		cols, shift = cur.columns, cur.shift
	}
	h.shape.Store(newLatencyShape(cols, shift))
	h.count.Store(0)
	h.sum.Store(0)
	h.max.Store(0)
	h.min.Store(latencyMinUnset)
}

// Merge adds a snapshot into this concurrent histogram.
func (h *SyncLatencyBuckets) Merge(other *LatencyBuckets) {
	if h == nil || other == nil || other.Count == 0 {
		return
	}
	shape := h.shape.Load()
	if shape == nil || len(shape.buckets) != len(other.Buckets) {
		return
	}
	for i, v := range other.Buckets {
		if v != 0 {
			shape.buckets[i].Add(v)
		}
	}
	h.count.Add(other.Count)
	h.sum.Add(uint64(other.Sum))
	// max
	for {
		old := h.max.Load()
		if other.Max <= old {
			break
		}
		if h.max.CompareAndSwap(old, other.Max) {
			break
		}
	}
	// min
	for {
		old := h.min.Load()
		if old != latencyMinUnset && other.Min >= old {
			break
		}
		if h.min.CompareAndSwap(old, other.Min) {
			break
		}
	}
}

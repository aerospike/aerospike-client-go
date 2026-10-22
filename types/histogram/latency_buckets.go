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

import "sync"

// LatencyBuckets is the shared learn-metrics / asadm range histogram:
// bucket 0 is <= 1 unit, then each following bucket doubles by latencyShift.
// Example (columns=7, shift=1): <=1, >1, >2, >4, >8, >16, >32.
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

// SyncLatencyBuckets is a mutex-protected LatencyBuckets.
type SyncLatencyBuckets struct {
	l sync.Mutex
	LatencyBuckets
}

func NewSyncLatencyBuckets(columns, shift int) *SyncLatencyBuckets {
	return &SyncLatencyBuckets{LatencyBuckets: *NewLatencyBuckets(columns, shift)}
}

func (h *SyncLatencyBuckets) Add(elapsed uint64) {
	h.l.Lock()
	h.LatencyBuckets.Add(elapsed)
	h.l.Unlock()
}

func (h *SyncLatencyBuckets) Clone() *LatencyBuckets {
	h.l.Lock()
	defer h.l.Unlock()
	return h.LatencyBuckets.Clone()
}

func (h *SyncLatencyBuckets) CloneAndReset() *LatencyBuckets {
	h.l.Lock()
	defer h.l.Unlock()
	return h.LatencyBuckets.CloneAndReset()
}

func (h *SyncLatencyBuckets) Reshape(columns, shift int) {
	h.l.Lock()
	h.LatencyBuckets.Reshape(columns, shift)
	h.l.Unlock()
}

func (h *SyncLatencyBuckets) Reset() {
	h.l.Lock()
	h.LatencyBuckets.Reset()
	h.l.Unlock()
}

func (h *SyncLatencyBuckets) Merge(other *LatencyBuckets) {
	if other == nil {
		return
	}
	h.l.Lock()
	h.LatencyBuckets.Merge(other)
	h.l.Unlock()
}

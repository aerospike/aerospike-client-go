// Copyright 2014-2024 Aerospike, Inc.
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
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
)

type SyncHistogram[T hvals] struct {
	l     sync.RWMutex
	htype Type
	base  T

	Buckets []uint64 `json:"buckets"` // slot -> count
	Min     T        `json:"min"`
	Max     T        `json:"max"`
	Sum     float64  `json:"sum"`
	Count   uint64   `json:"count"`
}

func NewSync[T hvals](htype Type, base T, buckets int) *SyncHistogram[T] {
	return &SyncHistogram[T]{
		htype:   htype,
		base:    base,
		Buckets: make([]uint64, buckets),
	}
}

func (h *SyncHistogram[T]) Reset() {
	h.l.Lock()
	for i := range h.Buckets {
		h.Buckets[i] = 0
	}

	h.Min = 0
	h.Max = 0
	h.Sum = 0
	h.Count = 0
	h.l.Unlock()
}

func (h *SyncHistogram[T]) Reshape(htype Type, base T, buckets int) {
	h.l.Lock()
	if h.htype == htype && h.base == base && len(h.Buckets) == buckets {
		h.l.Unlock()
		return
	}

	h.htype = htype
	h.base = base
	h.Buckets = make([]uint64, buckets)
	h.l.Unlock()
}

func (h *SyncHistogram[T]) String() string {
	h.l.RLock()
	res := new(strings.Builder)
	switch h.htype {
	case Linear:
		for i := 0; i < len(h.Buckets)-1; i++ {
			v := float64(h.base) * float64(i)
			fmt.Fprintf(res, "[%v, %v) => %d\n", v, v+float64(h.base), h.Buckets[i])
		}
		fmt.Fprintf(res, "[%v, inf) => %d\n", float64(h.base)*float64(len(h.Buckets)-1), h.Buckets[len(h.Buckets)-1])
	case Logarithmic:
		fmt.Fprintf(res, "[0, %v) => %d\n", float64(h.base), h.Buckets[0])
		for i := 1; i < len(h.Buckets)-1; i++ {
			v := math.Pow(float64(h.base), float64(i))
			fmt.Fprintf(res, "[%v, %v) => %d\n", v, v*float64(h.base), h.Buckets[i])
		}
		fmt.Fprintf(res, "[%v, inf) => %d\n", math.Pow(float64(h.base), float64(len(h.Buckets))-1), h.Buckets[len(h.Buckets)-1])
	}
	h.l.RUnlock()
	return res.String()
}

func (h *SyncHistogram[T]) Clone() *SyncHistogram[T] {
	h.l.Lock()
	b := make([]uint64, len(h.Buckets))
	copy(b, h.Buckets)
	res := &SyncHistogram[T]{
		htype: h.htype,
		base:  h.base,

		Buckets: b,
		Min:     h.Min,
		Max:     h.Max,
		Sum:     h.Sum,
		Count:   h.Count,
	}
	h.l.Unlock()
	return res
}

func (h *SyncHistogram[T]) CloneAndReset() *SyncHistogram[T] {
	h.l.Lock()
	b := make([]uint64, len(h.Buckets))
	copy(b, h.Buckets)
	res := &SyncHistogram[T]{
		htype: h.htype,
		base:  h.base,

		Buckets: b,
		Min:     h.Min,
		Max:     h.Max,
		Sum:     h.Sum,
		Count:   h.Count,
	}

	// Reset
	for i := range h.Buckets {
		h.Buckets[i] = 0
	}

	h.Min = 0
	h.Max = 0
	h.Sum = 0
	h.Count = 0
	h.l.Unlock()
	return res
}

func (h *SyncHistogram[T]) Merge(other *SyncHistogram[T]) error {
	h.l.Lock()
	other.l.RLock()

	if h.base != other.base || h.htype != other.htype || len(h.Buckets) != len(other.Buckets) {
		other.l.RUnlock()
		h.l.Unlock()
		return errors.New("histograms do not match")
	}

	if other.Min < h.Min || h.Min == 0 {
		h.Min = other.Min
	}

	if other.Max > h.Max {
		h.Max = other.Max
	}

	h.Sum += other.Sum
	h.Count += uint64(other.Count)

	for i := range h.Buckets {
		h.Buckets[i] += other.Buckets[i]
	}
	other.l.RUnlock()
	h.l.Unlock()
	return nil
}

func (h *SyncHistogram[T]) Average() float64 {
	h.l.RLock()
	if h.Count > 0 {
		res := h.Sum / float64(h.Count)
		h.l.RUnlock()
		return res
	}
	h.l.RUnlock()
	return 0
}

func (h *SyncHistogram[T]) Median() T {
	h.l.RLock()
	var s uint64 = 0
	c := h.Count / 2
	for i, bv := range h.Buckets {
		s += bv
		if s >= c {
			// found the bucket
			if h.htype == Linear {
				res := T(i+1) * h.base
				h.l.RUnlock()
				return res
			}
			res := T(math.Pow(float64(h.base), float64(i+1)))
			h.l.RUnlock()
			return res
		}
	}
	res := h.Max
	h.l.RUnlock()
	return res
}

func (h *SyncHistogram[T]) Add(v T) {
	h.l.Lock()
	if h.Count == 0 {
		h.Max = v
		h.Min = v
	} else {
		if v > h.Max {
			h.Max = v
		} else if v < h.Min {
			h.Min = v
		}
	}

	h.Sum += float64(v)
	h.Count++

	var slot int
	if v > 0 {
		switch h.htype {
		case Linear:
			slot = int(math.Floor(float64(v / T(h.base))))
		case Logarithmic:
			slot = int(math.Floor(math.Log(float64(v)) / math.Log(float64(h.base))))
		}
	}

	if slot >= len(h.Buckets) {
		h.Buckets[len(h.Buckets)-1]++
	} else if slot < 0 {
		h.Buckets[0]++
	} else {
		h.Buckets[slot]++
	}
	h.l.Unlock()
}

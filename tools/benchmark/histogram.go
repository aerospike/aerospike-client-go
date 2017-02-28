// Copyright 2013-2016 Aerospike, Inc.
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

package main

import "sync"

// Histogram is not synchronized and should not be used in parallel
type Histogram struct {
	buckets []int64

	min, max int64
}

func NewHistogram(size int) *Histogram {
	return &Histogram{
		buckets: make([]int64, size+1), // size +  1 bucket for the rest
	}
}

func (h *Histogram) Values() []int64 {
	return h.buckets
}

func (h *Histogram) Add(val int64) {
	if val < h.min {
		h.min = val
	}

	if val > h.max {
		h.max = val
	}

	for i := 0; i < len(h.buckets)-1; i++ {
		if val < int64(2^i) {
			h.buckets[i]++
			break
		}
	}

	// >= 2 ^ size
	h.buckets[len(h.buckets)]++
}

func (h *Histogram) Reset() {
	for i := 0; i < len(h.buckets); i++ {
		h.buckets[i] = 0
	}
}

type SyncedHistogram struct {
	Histogram
	mutex sync.RWMutex
}

func (h *SyncedHistogram) Merge(vals []int64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i := range h.buckets {
		h.buckets[i] += vals[i]
	}
}

func (h *SyncedHistogram) Values() []int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	// copy the internal buckets
	res := make([]int64, len(h.buckets))
	copy(res, h.buckets)

	return res
}

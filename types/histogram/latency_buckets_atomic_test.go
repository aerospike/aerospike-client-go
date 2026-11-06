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
	"testing"
)

func TestSyncLatencyBucketsAddClone(t *testing.T) {
	h := NewSyncLatencyBuckets(7, 1)
	h.Add(0)
	h.Add(1)
	h.Add(2)
	h.Add(33)
	snap := h.Clone()
	if snap.Count != 4 {
		t.Fatalf("count=%d", snap.Count)
	}
	if snap.Min != 0 || snap.Max != 33 {
		t.Fatalf("min/max=%d/%d", snap.Min, snap.Max)
	}
	if snap.Sum != 36 {
		t.Fatalf("sum=%v", snap.Sum)
	}
	want := []uint64{2, 1, 0, 0, 0, 0, 1}
	for i := range want {
		if snap.Buckets[i] != want[i] {
			t.Fatalf("buckets=%v want %v", snap.Buckets, want)
		}
	}
}

func TestSyncLatencyBucketsConcurrentAdd(t *testing.T) {
	h := NewSyncLatencyBuckets(7, 1)
	const goroutines = 32
	const per = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				h.Add(uint64(i % 40))
			}
		}()
	}
	wg.Wait()
	snap := h.Clone()
	if snap.Count != goroutines*per {
		t.Fatalf("count=%d want %d", snap.Count, goroutines*per)
	}
	var bucketSum uint64
	for _, b := range snap.Buckets {
		bucketSum += b
	}
	if bucketSum != snap.Count {
		t.Fatalf("bucket sum %d != count %d", bucketSum, snap.Count)
	}
}

func TestSyncLatencyBucketsReshapeResets(t *testing.T) {
	h := NewSyncLatencyBuckets(7, 1)
	h.Add(5)
	h.Reshape(5, 2)
	snap := h.Clone()
	if snap.Count != 0 || len(snap.Buckets) != 5 {
		t.Fatalf("after reshape: count=%d cols=%d", snap.Count, len(snap.Buckets))
	}
	h.Add(1)
	if h.Clone().Count != 1 {
		t.Fatal("add after reshape failed")
	}
}

func TestSyncLatencyBucketsFromSnapshot(t *testing.T) {
	src := NewLatencyBuckets(7, 1)
	src.Add(2)
	src.Add(8)
	h := SyncLatencyBucketsFromSnapshot(src)
	snap := h.Clone()
	if snap.Count != 2 || snap.Sum != 10 {
		t.Fatalf("seeded snap count=%d sum=%v", snap.Count, snap.Sum)
	}
}

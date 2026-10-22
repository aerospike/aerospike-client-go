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

import (
	"bytes"
	"container/heap"
	"math"
	"sort"
	"strings"
	"sync"
)

// topKMerger keeps the k best records of a Top-K query across nodes and
// retry rounds, ranked the way the server ranks them. Each node sends at most
// its own k best, so the client keeps the global k. It is shared by the
// query's commands and safe for concurrent use.
type topKMerger struct {
	mu             sync.Mutex
	orderBy        orderBySpec  // the bin we rank by
	thenOrderBy    orderBySpec  // the bin that breaks ties on orderBy
	hasThenOrderBy bool         // whether thenOrderBy is used
	limit          int          // k — max records to keep
	heap           []*topKEntry // kept records, worst at the root, Pop evicts
	byDigest       map[[20]byte]*topKEntry
}

// topKEntry is one kept record with its order-by values.
type topKEntry struct {
	result      *Result
	firstValue  orderByValue // value of the orderBy bin
	secondValue orderByValue // tie-break value: the thenOrderBy bin, when set
	digest      [20]byte
	index       int // position in the heap
}

// orderByValue is a record's sort key: the value of its order-by bin that
// records are ranked by. isNil means the record has no usable value (the bin
// is missing or holds another type); like the server's NIL, it ranks last in
// either direction.
type orderByValue struct {
	isNil   bool
	integer int64
	float   float64
	str     string
	blob    []byte
}

func newTopKMerger(statement *Statement) *topKMerger {
	return &topKMerger{
		orderBy:        statement.orderBy,
		thenOrderBy:    statement.thenOrderBy,
		hasThenOrderBy: statement.hasThenOrderBy,
		limit:          statement.topK,
		heap:           make([]*topKEntry, 0, statement.topK+1),
		byDigest:       make(map[[20]byte]*topKEntry, statement.topK+1),
	}
}

//-------------------------------------------------------
// Merging
//-------------------------------------------------------

// add offers a record. If the same record is offered twice (for example
// during migrations), the better-ranked copy is kept.
func (merger *topKMerger) add(result *Result) {
	candidate := topKEntry{result: result, firstValue: orderByValueOf(result.Record, merger.orderBy)}
	if merger.hasThenOrderBy {
		candidate.secondValue = orderByValueOf(result.Record, merger.thenOrderBy)
	}
	copy(candidate.digest[:], result.Record.Key.Digest())

	merger.mu.Lock()
	defer merger.mu.Unlock()

	// Already k records and this one is not ahead of the worst (heap[0]): it
	// cannot make the top k. A kept copy of the same record ranks at least as
	// well as the worst, so the better copy stays.
	if len(merger.heap) == merger.limit && !merger.ranksAhead(&candidate, merger.heap[0]) {
		return
	}

	if kept, found := merger.byDigest[candidate.digest]; found {
		if !merger.ranksAhead(&candidate, kept) {
			return
		}
		heap.Remove(merger, kept.index)
		delete(merger.byDigest, kept.digest)
	}

	entry := new(topKEntry)
	*entry = candidate
	heap.Push(merger, entry)
	merger.byDigest[entry.digest] = entry

	if len(merger.heap) > merger.limit {
		evicted := heap.Pop(merger).(*topKEntry)
		delete(merger.byDigest, evicted.digest)
	}
}

// results returns the kept records, best first.
func (merger *topKMerger) results() []*Result {
	merger.mu.Lock()
	entries := make([]*topKEntry, len(merger.heap))
	copy(entries, merger.heap)
	merger.mu.Unlock()

	sort.Slice(entries, func(left, right int) bool {
		return merger.ranksAhead(entries[left], entries[right])
	})

	results := make([]*Result, len(entries))
	for position, entry := range entries {
		results[position] = entry.result
	}
	return results
}

//-------------------------------------------------------
// Order-by value
//-------------------------------------------------------

// orderByValueOf reads the record's value of one order-by bin. A value of
// any type other than the bin's order-by type is nil.
func orderByValueOf(record *Record, spec orderBySpec) orderByValue {
	orderType := spec.orderType

	switch value := record.Bins[spec.binName].(type) {
	case int:
		if orderType == BinDataTypeInteger {
			return orderByValue{integer: int64(value)}
		}
	case int64:
		if orderType == BinDataTypeInteger {
			return orderByValue{integer: value}
		}
	case float64:
		if orderType == BinDataTypeDouble {
			return orderByValue{float: value}
		}
	case string:
		if orderType == BinDataTypeString {
			return orderByValue{str: value}
		}
	case []byte:
		if orderType == BinDataTypeBytes {
			return orderByValue{blob: value}
		}
	}
	return orderByValue{isNil: true}
}

//-------------------------------------------------------
// Ranking
//-------------------------------------------------------

// ranksAhead reports whether entry ranks before other.
//
// The records are compared on the first order-by bin. If they tie, the
// second bin decides. If they still tie, the smaller digest goes first.
//
// For each bin, a record with no usable value ranks last, whether the bin is
// ascending or descending.
func (merger *topKMerger) ranksAhead(entry, other *topKEntry) bool {
	if order := rankOrder(&merger.orderBy, &entry.firstValue, &other.firstValue); order != 0 {
		return order < 0
	}
	if merger.hasThenOrderBy {
		if order := rankOrder(&merger.thenOrderBy, &entry.secondValue, &other.secondValue); order != 0 {
			return order < 0
		}
	}
	return bytes.Compare(entry.digest[:], other.digest[:]) < 0
}

// rankOrder compares value and other on one order-by bin: -1 if value ranks
// ahead, 1 if other does, 0 on a tie. A nil value ranks last in either
// direction.
func rankOrder(spec *orderBySpec, value, other *orderByValue) int {
	if value.isNil != other.isNil {
		if value.isNil {
			return 1
		}
		return -1
	}
	if value.isNil {
		return 0
	}

	comparison := compareValues(spec, value, other)
	if spec.direction == OrderDesc {
		return -comparison
	}
	return comparison
}

// compareValues compares two non-nil values of one order-by bin in ascending
// order, like the server, returning -1, 0 or 1.
func compareValues(spec *orderBySpec, left, right *orderByValue) int {
	switch spec.orderType {
	case BinDataTypeInteger:
		return compareNumbers(left.integer, right.integer)
	case BinDataTypeDouble:
		return compareFloats(left.float, right.float)
	case BinDataTypeString:
		if spec.flags&OrderByFlagCaseInsensitive != 0 {
			return compareIgnoringASCIICase(left.str, right.str)
		}
		return strings.Compare(left.str, right.str)
	default:
		return bytes.Compare(left.blob, right.blob)
	}
}

func compareNumbers[T int64 | float64](left, right T) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}

// compareFloats ranks NaN above every number, and -0.0 equal to 0.0.
func compareFloats(left, right float64) int {
	leftIsNaN, rightIsNaN := math.IsNaN(left), math.IsNaN(right)
	switch {
	case leftIsNaN && rightIsNaN:
		return 0
	case leftIsNaN:
		return 1
	case rightIsNaN:
		return -1
	}
	return compareNumbers(left, right)
}

// compareIgnoringASCIICase compares strings byte by byte with A-Z folded to
// a-z; other bytes compare as they are. On a common prefix the shorter string
// comes first.
func compareIgnoringASCIICase(left, right string) int {
	for position := 0; position < len(left) && position < len(right); position++ {
		leftByte, rightByte := toLowerASCII(left[position]), toLowerASCII(right[position])
		if leftByte != rightByte {
			return compareNumbers(int64(leftByte), int64(rightByte))
		}
	}
	return compareNumbers(int64(len(left)), int64(len(right)))
}

func toLowerASCII(character byte) byte {
	if character >= 'A' && character <= 'Z' {
		return character + ('a' - 'A')
	}
	return character
}

//-------------------------------------------------------
// heap.Interface over the kept records, with the worst-ranked entry at
// the root. Only called with mu held.
//-------------------------------------------------------

func (merger *topKMerger) Len() int { return len(merger.heap) }

// Less puts the worse-ranked entry nearer the root.
func (merger *topKMerger) Less(left, right int) bool {
	return merger.ranksAhead(merger.heap[right], merger.heap[left])
}

func (merger *topKMerger) Swap(left, right int) {
	merger.heap[left], merger.heap[right] = merger.heap[right], merger.heap[left]
	merger.heap[left].index = left
	merger.heap[right].index = right
}

func (merger *topKMerger) Push(item any) {
	entry := item.(*topKEntry)
	entry.index = len(merger.heap)
	merger.heap = append(merger.heap, entry)
}

func (merger *topKMerger) Pop() any {
	last := len(merger.heap) - 1
	entry := merger.heap[last]
	merger.heap[last] = nil
	merger.heap = merger.heap[:last]
	return entry
}

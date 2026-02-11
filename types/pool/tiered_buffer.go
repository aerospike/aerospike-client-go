// Copyright 2014-2023 Aerospike, Inc.
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

package pool

import (
	"sync"
)

// TieredBufferPool is a tiered pool for the buffers.
// It will store buffers in powers of two in sub pools.
// The size of buffers will ALWAYS be powers of two, and the pool
// will throw away buffers passed to it which do not conform to this rule.
type TieredBufferPool struct {
	// Min is Minimum the minimum buffer size.
	Min int

	// Max is the maximum buffer size. The pool will allocate buffers of that size,
	// But will not store them back.
	Max int

	pools []sync.Pool
}

// NewTieredBufferPool creates a new buffer pool.
// New buffers will be created with size and capacity of initBufferSize.
// If  cap(buffer) is larger than maxBufferSize when it is put back in the buffer,
// it will be thrown away. This will prevent unwanted memory bloat and
// set a deterministic maximum-size for the pool which will not be exceeded.
func NewTieredBufferPool(min, max int) *TieredBufferPool {
	if !powerOf2(min) || !powerOf2(max) {
		panic("min and max values should both be powers of 2")
	}

	p := &TieredBufferPool{
		Min: min,
		Max: max,
	}

	buckets := fastLog2(uint64(max))
	if !powerOf2(max) {
		buckets++
	}

	for i := 1; i <= buckets; i++ {
		blockSize := 1 << i
		p.pools = append(p.pools,
			sync.Pool{
				New: func() any {
					// The Pool's New function should generally only return pointer
					// types, since a pointer can be put into the return interface
					// value without an allocation:
					return make([]byte, blockSize, blockSize)
				},
			})
	}

	return p
}

// Returns the pool index based on the size of the buffer.
// Will return -1 if the value falls outside of the pool range.
func (bp *TieredBufferPool) poolIndex(sz int) int {
	factor := fastLog2(uint64(sz))
	szl := factor - 1
	if !powerOf2(sz) {
		szl++
	}
	if szl >= 0 && szl < len(bp.pools) {
		return szl
	}
	return -1
}

// Get returns a buffer from the pool. If sz is bigger than maxBufferSize,
// a fresh buffer will be created and not taken from the pool.
func (bp *TieredBufferPool) Get(sz int) []byte {
	// Short circuit. We know we don't have buffers this size in the pool.
	if sz > bp.Max {
		return make([]byte, sz, sz)
	}

	// do not allocate buffers smaller than a certain size
	if sz < bp.Min {
		sz = bp.Min
	}

	if szl := bp.poolIndex(sz); szl >= 0 {
		res := bp.pools[szl].Get().([]byte)
		origLen := 1 << (szl + 1)
		return res[:origLen] // return the slice to its max capacity
	}

	// this line will never be reached, but Go would complain if omitted
	return make([]byte, sz, sz)
}

// Put will put the buffer back in the pool, unless cap(buf) is smaller than Min
// or larger than Max, or the size of the buffer is not a power of 2
// in which case it will be thrown away.
func (bp *TieredBufferPool) Put(buf []byte) {
	sz := cap(buf)
	// throw away random non-power of 2 buffer sizes
	if len(buf) > bp.Min && len(buf) <= bp.Max && powerOf2(sz) {
		if szl := bp.poolIndex(sz); szl >= 0 {
			bp.pools[szl].Put(buf)
			return
		}
	}
}

///////////////////////////////////////////////////////////////////

// powerOf2 returns true if a number is an EXACT power of 2.
func powerOf2(sz int) bool {
	return sz > 0 && (sz&(sz-1)) == 0
}

var log2tab64 = [64]int8{
	0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42, 3, 61,
	51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4, 62,
	57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21, 56,
	45, 25, 31, 35, 16, 9, 12, 44, 24, 15, 8, 23, 7, 6, 5, 63,
}

// fast log2 implementation
func fastLog2(value uint64) int {
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	value |= value >> 32

	return int(log2tab64[(value*0x03f6eaf2cd271461)>>58])
}

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

package types

import (
	"sync"
)

const maxBufSize = 16 * 1024 * 1024 // 2 * 8 MiB, server block size as of v6.3

type BufferPool struct {
	pools []sync.Pool
}

// NewBufferPool creates a new buffer pool.
// New buffers will be created with size and capacity of initBufferSize.
// If  cap(buffer) is larger than maxBufferSize when it is put back in the buffer,
// it will be thrown away. This will prevent unwanted memory bloat and
// set a deterministic maximum-size for the pool which will not be exceeded.
func NewBufferPool() *BufferPool {
	p := &BufferPool{}

	max := fastlog2(uint64(maxBufSize))
	for i := 1; i <= max; i++ {
		blockSize := 1 << i
		p.pools = append(p.pools,
			sync.Pool{
				New: func() interface{} {
					// The Pool's New function should generally only return pointer
					// types, since a pointer can be put into the return interface
					// value without an allocation:
					return make([]byte, blockSize, blockSize)
				},
			})
	}

	return p
}

// powerOf2 returns true if a number is an EXACT power of 2.
func powerOf2(sz int) bool {
	return sz > 0 && (sz&(sz-1)) == 0
}

// Returns the pool index based on the size of the buffer.
// Will return -1 if the value falls outside of the pool range.
func (bp *BufferPool) poolIndex(sz int) int {
	factor := fastlog2(uint64(sz))
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
func (bp *BufferPool) Get(sz int) []byte {
	// Short circuit
	if sz > maxBufSize {
		return make([]byte, sz, sz)
	}

	if szl := bp.poolIndex(sz); szl >= 0 {
		res := bp.pools[szl].Get().([]byte)
		origLen := 1 << (szl + 1)
		return res[:origLen] // return the slice to its max capacity
	}

	// this line will never be reached, but Go would complain if omitted
	return make([]byte, sz, sz)
}

// Put will put the buffer back in the pool, unless cap(buf) is bigger than
// maxBufSize, in which case it will be thrown away
func (bp *BufferPool) Put(buf []byte) {
	sz := cap(buf)
	// throw away random non-power of 2 buffer sizes
	if powerOf2(sz) {
		if szl := bp.poolIndex(sz); szl >= 0 {
			bp.pools[szl].Put(buf)
			return
		}
	}
}

///////////////////////////////////////////////////////////////////

var log2tab64 = [64]int8{
	0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42, 3, 61,
	51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4, 62,
	57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21, 56,
	45, 25, 31, 35, 16, 9, 12, 44, 24, 15, 8, 23, 7, 6, 5, 63,
}

// fastlog2 implements the fastlog2 function for uint64 values.
func fastlog2(value uint64) int {
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	value |= value >> 32

	return int(log2tab64[(value*0x03f6eaf2cd271461)>>58])
}

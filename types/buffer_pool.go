// Copyright 2013-2014 Aerospike, Inc.
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
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

type BufferPool struct {
	pool     *AtomicQueue
	poolSize int

	maxBufSize  int
	initBufSize int
}

// NewBufferPool creates a new buffer pool.
// New buffers will be created with size and capacity of initBufferSize.
// If  cap(buffer) is larger than maxBufferSize when it is put back in the buffer,
// it will be thrown away. This will prevent unwanted memory bloat and
// set a deterministic maximum-size for the pool which will not be exceeded.
func NewBufferPool(poolSize, initBufferSize, maxBufferSize int) *BufferPool {
	return &BufferPool{
		pool:        NewAtomicQueue(poolSize),
		poolSize:    poolSize,
		maxBufSize:  maxBufferSize,
		initBufSize: initBufferSize,
	}
}

// Get return a buffer from the pool. If pool is empty, a new buffer of
// size initBufSize will be created and returned.
func (bp *BufferPool) Get() []byte {
	res := bp.pool.Poll()
	if res == nil {
		return make([]byte, bp.initBufSize, bp.initBufSize)
	}

	return res.([]byte)
}

// Put will put the buffer back in the pool, unless cap(buf) is bigger than
// initBufSize, in which case it will be thrown away
func (bp *BufferPool) Put(buf []byte) {
	if cap(buf) <= bp.maxBufSize {
		bp.pool.Offer(buf)
	}
}

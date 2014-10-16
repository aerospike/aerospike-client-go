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

// Pool implements a general purpose fixed-size pool.
type Pool struct {
	pool     *AtomicQueue
	poolSize int

	New func() interface{}
}

// NewPool creates a new fixed size pool.
func NewPool(poolSize int) *Pool {
	return &Pool{
		pool:     NewAtomicQueue(poolSize),
		poolSize: poolSize,
	}
}

// Get returns an element from the pool. If pool is empty, and a New function is defined,
// the result of the New function will be returned
func (bp *Pool) Get() interface{} {
	res := bp.pool.Poll()
	if res == nil && bp.New != nil {
		res = bp.New()
	}

	return res
}

// Put will add the elem back to the pool, unless the pool is full.
func (bp *Pool) Put(elem interface{}) {
	bp.pool.Offer(elem)
}

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

package atomic

import "sync/atomic"

// AtomicBool implements a synchronized boolean value
type AtomicBool struct {
	val int32
}

// NewAtomicBool generates a new AtomicBoolean instance.
func NewAtomicBool(value bool) *AtomicBool {
	var i int32 = 0
	if value {
		i = 1
	}
	return &AtomicBool{
		val: i,
	}
}

// Get atomically retrieves the boolean value.
func (ab *AtomicBool) Get() bool {
	if atomic.LoadInt32(&(ab.val)) != 0 {
		return true
	}
	return false
}

// Set atomically sets the boolean value.
func (ab *AtomicBool) Set(newVal bool) {
	var i int32 = 0
	if newVal {
		i = 1
	}
	atomic.StoreInt32(&(ab.val), int32(i))
}

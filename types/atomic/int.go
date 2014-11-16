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

import (
	"sync"
)

// AtomicInt implements an int value with atomic semantics
type AtomicInt struct {
	val   int
	mutex sync.RWMutex
}

// NewAtomicInt generates a new AtomicInt instance.
func NewAtomicInt(value int) *AtomicInt {
	return &AtomicInt{
		val: value,
	}
}

// AddAndGet atomically adds the given value to the current value.
func (ai *AtomicInt) AddAndGet(delta int) int {
	ai.mutex.Lock()
	ai.val += delta
	res := ai.val
	ai.mutex.Unlock()
	return res
}

// CompareAndSet atomically sets the value to the given updated value if the current value == expected value.
// Returns true if the expectation was met
func (ai *AtomicInt) CompareAndSet(expect int, update int) bool {
	res := false
	ai.mutex.Lock()
	if ai.val == expect {
		ai.val = update
		res = true
	}
	ai.mutex.Unlock()
	return res
}

// DecrementAndGet atomically decrements current value by one and returns the result.
func (ai *AtomicInt) DecrementAndGet() int {
	ai.mutex.Lock()
	ai.val--
	res := ai.val
	ai.mutex.Unlock()
	return res
}

// Get atomically retrieves the current value.
func (ai *AtomicInt) Get() int {
	ai.mutex.RLock()
	res := ai.val
	ai.mutex.RUnlock()
	return res
}

// GetAndAdd atomically adds the given delta to the current value and returns the result.
func (ai *AtomicInt) GetAndAdd(delta int) int {
	ai.mutex.Lock()
	old := ai.val
	ai.val += delta
	ai.mutex.Unlock()
	return old
}

// GetAndDecrement atomically decrements the current value by one and returns the result.
func (ai *AtomicInt) GetAndDecrement() int {
	ai.mutex.Lock()
	old := ai.val
	ai.val--
	ai.mutex.Unlock()
	return old
}

// GetAndIncrement atomically increments current value by one and returns the result.
func (ai *AtomicInt) GetAndIncrement() int {
	ai.mutex.Lock()
	old := ai.val
	ai.val++
	ai.mutex.Unlock()
	return old
}

// GetAndSet atomically sets current value to the given value and returns the old value.
func (ai *AtomicInt) GetAndSet(newValue int) int {
	ai.mutex.Lock()
	old := ai.val
	ai.val = newValue
	ai.mutex.Unlock()
	return old
}

// IncrementAndGet atomically increments current value by one and returns the result.
func (ai *AtomicInt) IncrementAndGet() int {
	ai.mutex.Lock()
	ai.val++
	res := ai.val
	ai.mutex.Unlock()
	return res
}

// Set atomically sets current value to the given value.
func (ai *AtomicInt) Set(newValue int) {
	ai.mutex.Lock()
	ai.val = newValue
	ai.mutex.Unlock()
}

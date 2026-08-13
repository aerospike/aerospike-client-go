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

package atomic

import (
	"strconv"
	"sync/atomic"
)

// Int implements an int value with atomic semantics.
//
// It is a thin wrapper over sync/atomic.Int64: every operation is a single
// atomic instruction (or a CAS), not a mutex acquisition.
type Int struct {
	val atomic.Int64
}

// NewInt generates a new Int instance.
func NewInt(value int) *Int {
	ai := &Int{}
	ai.val.Store(int64(value))
	return ai
}

// String implements the Stringer interface
func (ai *Int) String() string {
	res := ai.Get()
	return strconv.Itoa(res)
}

// GomegaString implements the GomegaStringer interface
// to prevent race conditions during tests
func (ai *Int) GomegaString() string {
	return ai.String()
}

// AddAndGet atomically adds the given value to the current value.
func (ai *Int) AddAndGet(delta int) int {
	return int(ai.val.Add(int64(delta)))
}

// CompareAndSet atomically sets the value to the given updated value if the current value == expected value.
// Returns true if the expectation was met
func (ai *Int) CompareAndSet(expect int, update int) bool {
	return ai.val.CompareAndSwap(int64(expect), int64(update))
}

// DecrementAndGet atomically decrements current value by one and returns the result.
func (ai *Int) DecrementAndGet() int {
	return int(ai.val.Add(-1))
}

// Get atomically retrieves the current value.
func (ai *Int) Get() int {
	return int(ai.val.Load())
}

// GetAndAdd atomically adds the given delta to the current value and returns the original value.
func (ai *Int) GetAndAdd(delta int) int {
	return int(ai.val.Add(int64(delta))) - delta
}

// GetAndDecrement atomically decrements the current value by one and returns the original value.
func (ai *Int) GetAndDecrement() int {
	return int(ai.val.Add(-1)) + 1
}

// GetAndIncrement atomically increments current value by one and returns the original value.
func (ai *Int) GetAndIncrement() int {
	return int(ai.val.Add(1)) - 1
}

// GetAndSet atomically sets current value to the given value and returns the old value.
func (ai *Int) GetAndSet(newValue int) int {
	return int(ai.val.Swap(int64(newValue)))
}

// IncrementAndGet atomically increments current value by one and returns the result.
func (ai *Int) IncrementAndGet() int {
	return int(ai.val.Add(1))
}

// Set atomically sets current value to the given value.
func (ai *Int) Set(newValue int) {
	ai.val.Store(int64(newValue))
}

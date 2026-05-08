// Copyright 2014-2022 Aerospike, Inc.
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

// Int implements an int value with atomic semantics, backed by a lock-free
// int64 manipulated via sync/atomic. The public API is preserved for source
// compatibility with the previous mutex-backed implementation.
//
// A plain int64 field is used (rather than atomic.Int64) so that Clone and
// CloneAndSet can return the wrapper struct by value without tripping the
// copylocks vet check. All access goes through sync/atomic, so direct copies
// produce a snapshot that is consistent at the moment of the copy.
type Int struct {
	val int64
}

// NewInt generates a newVal Int instance.
func NewInt(value int) *Int {
	ai := &Int{}
	atomic.StoreInt64(&ai.val, int64(value))
	return ai
}

// String implements the Stringer interface
func (ai *Int) String() string {
	return strconv.Itoa(ai.Get())
}

// GomegaString implements the GomegaStringer interface
// to prevent race conditions during tests
func (ai *Int) GomegaString() string {
	return ai.String()
}

// AddAndGet atomically adds the given value to the current value.
func (ai *Int) AddAndGet(delta int) int {
	return int(atomic.AddInt64(&ai.val, int64(delta)))
}

// Clone atomically clones the atomic Int.
func (ai *Int) Clone() Int {
	return Int{val: atomic.LoadInt64(&ai.val)}
}

// CloneAndSet atomically clones the atomic Int and sets the value to the given updated value.
func (ai *Int) CloneAndSet(value int) Int {
	return Int{val: atomic.SwapInt64(&ai.val, int64(value))}
}

// CompareAndSet atomically sets the value to the given updated value if the current value == expected value.
// Returns true if the expectation was met
func (ai *Int) CompareAndSet(expect int, update int) bool {
	return atomic.CompareAndSwapInt64(&ai.val, int64(expect), int64(update))
}

// DecrementAndGet atomically decrements current value by one and returns the result.
func (ai *Int) DecrementAndGet() int {
	return int(atomic.AddInt64(&ai.val, -1))
}

// Get atomically retrieves the current value.
func (ai *Int) Get() int {
	return int(atomic.LoadInt64(&ai.val))
}

// GetAndAdd atomically adds the given delta to the current value and returns the original value.
func (ai *Int) GetAndAdd(delta int) int {
	return int(atomic.AddInt64(&ai.val, int64(delta))) - delta
}

// GetAndDecrement atomically decrements the current value by one and returns the original value.
func (ai *Int) GetAndDecrement() int {
	return int(atomic.AddInt64(&ai.val, -1)) + 1
}

// GetAndIncrement atomically increments current value by one and returns the original value.
func (ai *Int) GetAndIncrement() int {
	return int(atomic.AddInt64(&ai.val, 1)) - 1
}

// GetAndSet atomically sets current value to the given value and returns the old value.
func (ai *Int) GetAndSet(newValue int) int {
	return int(atomic.SwapInt64(&ai.val, int64(newValue)))
}

// IncrementAndGet atomically increments current value by one and returns the result.
func (ai *Int) IncrementAndGet() int {
	return int(atomic.AddInt64(&ai.val, 1))
}

// Set atomically sets current value to the given value.
func (ai *Int) Set(newValue int) {
	atomic.StoreInt64(&ai.val, int64(newValue))
}

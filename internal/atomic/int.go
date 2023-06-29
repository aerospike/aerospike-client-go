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
	"sync"
)

// Int implements an int value with atomic semantics
type Int struct {
	m   sync.Mutex
	val int
}

// NewInt generates a newVal Int instance.
func NewInt(value int) *Int {
	return &Int{
		val: value,
	}
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
	ai.m.Lock()
	ai.val += delta
	res := ai.val
	ai.m.Unlock()
	return res
}

// CloneAndSet atomically clones the atomic Int and sets the value to the given updated value.
func (ai *Int) Clone() Int {
	ai.m.Lock()
	res := Int{
		val: ai.val,
	}
	ai.m.Unlock()
	return res
}

// CloneAndSet atomically clones the atomic Int and sets the value to the given updated value.
func (ai *Int) CloneAndSet(value int) Int {
	ai.m.Lock()
	res := Int{
		val: ai.val,
	}
	ai.val = value
	ai.m.Unlock()
	return res
}

// CompareAndSet atomically sets the value to the given updated value if the current value == expected value.
// Returns true if the expectation was met
func (ai *Int) CompareAndSet(expect int, update int) bool {
	res := false
	ai.m.Lock()
	if ai.val == expect {
		ai.val = update
		res = true
	}
	ai.m.Unlock()
	return res
}

// DecrementAndGet atomically decrements current value by one and returns the result.
func (ai *Int) DecrementAndGet() int {
	ai.m.Lock()
	ai.val--
	res := ai.val
	ai.m.Unlock()
	return res
}

// Get atomically retrieves the current value.
func (ai *Int) Get() int {
	ai.m.Lock()
	res := ai.val
	ai.m.Unlock()
	return res
}

// GetAndAdd atomically adds the given delta to the current value and returns the result.
func (ai *Int) GetAndAdd(delta int) int {
	ai.m.Lock()
	res := ai.val
	ai.val += delta
	ai.m.Unlock()
	return res
}

// GetAndDecrement atomically decrements the current value by one and returns the result.
func (ai *Int) GetAndDecrement() int {
	ai.m.Lock()
	res := ai.val
	ai.val--
	ai.m.Unlock()
	return res
}

// GetAndIncrement atomically increments current value by one and returns the result.
func (ai *Int) GetAndIncrement() int {
	ai.m.Lock()
	res := ai.val
	ai.val++
	ai.m.Unlock()
	return res
}

// GetAndSet atomically sets current value to the given value and returns the old value.
func (ai *Int) GetAndSet(newValue int) int {
	ai.m.Lock()
	res := ai.val
	ai.val = newValue
	ai.m.Unlock()
	return res
}

// IncrementAndGet atomically increments current value by one and returns the result.
func (ai *Int) IncrementAndGet() int {
	ai.m.Lock()
	ai.val++
	res := ai.val
	ai.m.Unlock()
	return res
}

// Set atomically sets current value to the given value.
func (ai *Int) Set(newValue int) {
	ai.m.Lock()
	ai.val = newValue
	ai.m.Unlock()
}

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

// AtomicBool implements a synchronized boolean value
type AtomicBool struct {
	val   bool
	mutex sync.RWMutex
}

// Generator for AtomicBoolean
func NewAtomicBool(value bool) *AtomicBool {
	return &AtomicBool{
		val: value,
	}
}

// Atomically retrieves the boolean value.
func (this *AtomicBool) Get() bool {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.val
}

// Atomically sets the boolean value.
func (this *AtomicBool) Set(newVal bool) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.val = newVal
}

// Atomically retrieves the current boolean value first, and then toggles it.
func (this *AtomicBool) GetAndToggle() bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	val := this.val
	this.val = !this.val
	return val
}

// Atomically toggles the boolean value first, and then retrieves it.
func (this *AtomicBool) ToggleAndGet() bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.val = !this.val
	return this.val
}

// Atomically sets the boolean value to updated value, if the current value is as expected.
func (this *AtomicBool) CompareAndSet(expect bool, update bool) bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if this.val == expect {
		this.val = update
		return true
	} else {
		return false
	}
}

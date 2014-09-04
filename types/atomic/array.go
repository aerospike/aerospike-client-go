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
	"fmt"
	"sync"
)

// AtomicArray implement a fixed width array with atomic semantics
type AtomicArray struct {
	items  []interface{}
	length int
	mutex  sync.RWMutex
}

// Generator for AtomicArray
func NewAtomicArray(length int) *AtomicArray {
	return &AtomicArray{
		length: length,
		items:  make([]interface{}, length),
	}
}

// Atomically Get an element from the Array.
// If idx is out of range, it will return nil
func (aa *AtomicArray) Get(idx int) interface{} {
	// do not lock if not needed
	if idx < 0 || idx >= aa.length {
		return nil
	}

	aa.mutex.RLock()
	defer aa.mutex.RUnlock()
	return aa.items[idx]
}

// Atomically Set an element in the Array.
// If idx is out of range, it will return an error
func (aa *AtomicArray) Set(idx int, node interface{}) error {
	// do not lock if not needed
	if idx < 0 || idx >= aa.length {
		return fmt.Errorf("index %d is larger than array size (%d)", idx, aa.length)
	}

	aa.mutex.Lock()
	defer aa.mutex.Unlock()
	aa.items[idx] = node
	return nil
}

// Get array size.
func (aa *AtomicArray) Length() int {
	return aa.length
}

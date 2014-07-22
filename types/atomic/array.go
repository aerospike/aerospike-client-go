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
	"errors"
	"fmt"
	"sync"
)

type AtomicArrayItem interface{}

// AtomicArray implement a fixed width array with atomic semantics
type AtomicArray struct {
	items  []AtomicArrayItem
	length int
	mutex  sync.RWMutex
}

// Generator for AtomicArray
func NewAtomicArray(length int) *AtomicArray {
	return &AtomicArray{
		length: length,
		items:  make([]AtomicArrayItem, length),
	}
}

// Atomically Get an element from the Array.
// If idx is out of range, it will return nil
func (this *AtomicArray) Get(idx int) AtomicArrayItem {
	// do not lock if not needed
	if idx < 0 || idx >= this.length {
		return nil
	}

	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.items[idx]
}

// Atomically Set an element in the Array.
// If idx is out of range, it will return an error
func (this *AtomicArray) Set(idx int, node AtomicArrayItem) error {
	// do not lock if not needed
	if idx < 0 || idx >= this.length {
		return errors.New(fmt.Sprintf("index %d is larger than array size (%d)", idx, this.length))
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.items[idx] = node
	return nil
}

// Get array size.
func (this *AtomicArray) Length() int {
	return this.length
}

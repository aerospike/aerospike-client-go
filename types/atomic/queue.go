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

// AtomicQueue is a blocking FIFO queue
// If the queue is empty, nil is returned.
// if the queue is full, offer will return false
type AtomicQueue struct {
	items chan interface{}
}

// NewQueue creates a new queue with initial size
func NewAtomicQueue(size int) *AtomicQueue {
	return &AtomicQueue{
		items: make(chan interface{}, size),
	}
}

// Push adds an item to the queue in specified timeout
func (this *AtomicQueue) Offer(item interface{}) bool {
	// non-blocking send pattern
	select {
	case this.items <- item:
		return true
	default:
	}
	return false
}

// Poll removes and returns a node from the queue in first to last order.
func (this *AtomicQueue) Poll() interface{} {
	// non-blocking read pattern
	select {
	case item := <-this.items:
		return item
	default:
	}
	return nil
}

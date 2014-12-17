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

// AtomicQueue is a blocking FIFO queue.
// If the queue is empty, nil is returned.
// if the queue is full, offer will return false
type AtomicQueue struct {
	items chan interface{}
}

// NewQueue creates a new queue with initial size.
func NewAtomicQueue(size int) *AtomicQueue {
	return &AtomicQueue{
		items: make(chan interface{}, size),
	}
}

// Offer adds an item to the queue unless the queue is full.
// In case the queue is full, the item will not be added to the queue
// and false will be returned
func (aq *AtomicQueue) Offer(item interface{}) bool {
	// non-blocking send pattern
	select {
	case aq.items <- item:
		return true
	default:
	}
	return false
}

// Poll removes and returns an item from the queue.
// If the queue is empty, nil will be returned.
func (aq *AtomicQueue) Poll() interface{} {
	// non-blocking read pattern
	select {
	case item := <-aq.items:
		return item
	default:
	}
	return nil
}

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

package aerospike

import (
	"sync"

	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

// Recordset encapsulates the result of Scan and Query commands.
type Recordset struct {
	// Records is a channel on which the resulting records will be sent back.
	Records chan *Record
	// Errors is a channel on which all errors will be sent back.
	Errors chan error

	wgGoroutines sync.WaitGroup
	goroutines   *AtomicInt

	active    *AtomicBool
	cancelled chan struct{}
}

// NewRecordset generates a new RecordSet instance.
func newRecordset(recSize, goroutines int) *Recordset {
	rs := &Recordset{
		Records:    make(chan *Record, recSize),
		Errors:     make(chan error, goroutines),
		active:     NewAtomicBool(true),
		goroutines: NewAtomicInt(goroutines),
		cancelled:  make(chan struct{}),
	}
	rs.wgGoroutines.Add(goroutines)

	return rs
}

// IsActive returns true if the operation hasn't been finished or cancelled.
func (rcs *Recordset) IsActive() bool {
	return rcs.active.Get()
}

// Close all streams to different nodes.
func (rcs *Recordset) Close() {
	// do it only once
	if rcs.active.CompareAndToggle(true) {
		// this will broadcast to all commands listening to the channel
		close(rcs.cancelled)

		// wait till all goroutines are done
		rcs.wgGoroutines.Wait()

		close(rcs.Records)
		close(rcs.Errors)
	}
}

func (rcs *Recordset) signalEnd() {
	rcs.wgGoroutines.Done()

	cnt := rcs.goroutines.DecrementAndGet()
	if cnt == 0 {
		rcs.Close()
	}
}

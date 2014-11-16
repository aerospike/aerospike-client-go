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
)

// Recordset encapsulates the result of Scan and Query commands.
type Recordset struct {
	// Records is a channel on which the resulting records will be sent back.
	Records chan *Record
	// Errors is a channel on which all errors will be sent back.
	Errors chan error

	active   bool
	chans    []chan *Record
	errs     []chan error
	commands []multiCommand

	mutex sync.RWMutex
}

// NewRecordset generates a new RecordSet instance.
func NewRecordset(size int) *Recordset {
	return &Recordset{
		Records:  make(chan *Record, size),
		Errors:   make(chan error, size),
		active:   true,
		commands: []multiCommand{},
	}
}

// IsActive returns true if the operation hasn't been finished or cancelled.
func (rcs *Recordset) IsActive() bool {
	rcs.mutex.RLock()
	active := rcs.active
	rcs.mutex.RUnlock()

	return active
}

// Close all streams to different nodes.
func (rcs *Recordset) Close() {
	rcs.mutex.Lock()
	rcs.active = false
	rcs.mutex.Unlock()

	for i := range rcs.commands {
		// send signal to close
		rcs.commands[i].Stop()
	}
}

// drains a records channel into the results chan
func (rcs *Recordset) drainRecords(recChan chan *Record) {
	// drain the results chan
	for {
		rec, ok := <-recChan
		// if channel is closed, or is empty, exit the loop
		if !ok || rec == nil {
			break
		}
		rcs.Records <- rec
	}
}

// drains a channel into the errors chan
func (rcs *Recordset) drainErrors(errChan chan error) {
	// drain the results chan
	for {
		err, ok := <-errChan
		// if channel is closed, or is empty, exit the loop
		if !ok || err == nil {
			break
		}
		rcs.Errors <- err
	}
}

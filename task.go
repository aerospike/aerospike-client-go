// Copyright 2013-2017 Aerospike, Inc.
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
	"time"
)

// Task interface defines methods for asynchronous tasks.
type Task interface {
	IsDone() (bool, error)

	onComplete(ifc Task) chan error
	OnComplete() chan error
}

// baseTask is used to poll for server task completion.
type baseTask struct {
	retries int
	cluster *Cluster
	done    bool
}

// newTask initializes task with fields needed to query server nodes.
func newTask(cluster *Cluster, done bool) *baseTask {
	return &baseTask{
		cluster: cluster,
		done:    done,
	}
}

// Wait for asynchronous task to complete using default sleep interval.
func (btsk *baseTask) onComplete(ifc Task) chan error {
	var ch = make(chan error, 1)
	// goroutine will loop every <interval> until IsDone() returns true or error
	const interval = 1 * time.Second
	go func() {
		// always close the channel on return
		defer close(ch)

		for {
			<-time.After(interval)
			done, err := ifc.IsDone()
			btsk.retries++
			if err != nil {
				ch <- err
				return
			}
			if done {
				return
			}
		} // for
	}()

	return ch
}

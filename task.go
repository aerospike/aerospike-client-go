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
	"time"
)

type Task interface {
	IsDone() (bool, error)

	onComplete(ifc Task) chan error
	OnComplete() chan error
}

// Task used to poll for server task completion.
type BaseTask struct {
	cluster        *Cluster
	done           bool
	onCompleteChan chan error
}

// Initialize task with fields needed to query server nodes.
func NewTask(cluster *Cluster, done bool) *BaseTask {
	return &BaseTask{
		cluster: cluster,
		done:    done,
	}
}

// Wait for asynchronous task to complete using default sleep interval.
func (this *BaseTask) onComplete(ifc Task) chan error {
	// create the channel if it doesn't exist yet
	if this.onCompleteChan == nil {
		this.onCompleteChan = make(chan error)
	} else {
		// channel and goroutine already exists; just return the channel
		return this.onCompleteChan
	}

	// goroutine will loop every <interval> until IsDone() returns true or error
	const interval = 100 * time.Millisecond
	go func() {
		// always close the channel on return
		defer close(this.onCompleteChan)

		for {
			select {
			case <-time.After(interval):
				done, err := ifc.IsDone()
				if err != nil {
					this.onCompleteChan <- err
					return
				} else if done {
					this.onCompleteChan <- nil
					return
				}
			} // select
		} // for
	}()

	return this.onCompleteChan
}

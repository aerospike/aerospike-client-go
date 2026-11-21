// Copyright 2014-2026 Aerospike, Inc.
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

package main

import (
	"strconv"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// loadStats are the load counters every snapshot reports. Times are on the
// driver clock.
type loadStats struct {
	lock       sync.Mutex
	succeeded  int64
	failed     int64
	maxLatency time.Duration
	lastError  *string
	resetAt    time.Duration
}

func (stats *loadStats) recordFailure(message string) {
	stats.lock.Lock()
	stats.failed++
	stats.lastError = &message
	stats.lock.Unlock()
}

// recordSuccess counts a command whose first attempt started at firstStart.
// Its latency counts toward the maximum only when it started at or after the
// last reset.
func (stats *loadStats) recordSuccess(firstStart, end time.Duration) {
	stats.lock.Lock()
	stats.succeeded++
	if firstStart >= stats.resetAt && end-firstStart > stats.maxLatency {
		stats.maxLatency = end - firstStart
	}
	stats.lock.Unlock()
}

func (stats *loadStats) reset(at time.Duration) {
	stats.lock.Lock()
	stats.resetAt = at
	stats.maxLatency = 0
	stats.lock.Unlock()
}

func (stats *loadStats) addTo(object line) {
	stats.lock.Lock()
	object["succeeded"] = stats.succeeded
	object["failed"] = stats.failed
	object["maxLatencyMs"] = stats.maxLatency.Milliseconds()
	if stats.lastError == nil {
		object["lastError"] = nil
	} else {
		object["lastError"] = *stats.lastError
	}
	stats.lock.Unlock()
}

// loadKeyIndex is the key a worker uses on an iteration.
func loadKeyIndex(worker, iteration, workers, keys int) int {
	return (worker + iteration*workers) % keys
}

// loadRunner runs the workers until stop closes.
type loadRunner struct {
	client  *as.Client
	options *driverOptions
	output  *driverOutput
	stats   *loadStats
	ready   func()

	stop    chan struct{}
	workers sync.WaitGroup
}

func (runner *loadRunner) start() {
	for worker := 0; worker < runner.options.workers; worker++ {
		runner.workers.Add(1)
		go runner.runWorker(worker)
	}
}

// waitStopped waits for every worker to finish its current attempt, until
// deadline.
func (runner *loadRunner) waitStopped(deadline time.Time) {
	stopped := make(chan struct{})
	go func() {
		runner.workers.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Until(deadline)):
	}
}

func (runner *loadRunner) runWorker(worker int) {
	defer runner.workers.Done()
	readPolicy := runner.options.readPolicy()
	writePolicy := runner.options.writePolicy()

	for iteration := 0; ; iteration++ {
		index := loadKeyIndex(worker, iteration, runner.options.workers, runner.options.keys)
		key, err := as.NewKey(runner.options.namespace, runner.options.set, "load-"+strconv.Itoa(index))
		if err != nil {
			runner.stats.recordFailure(err.Error())
			return
		}

		command := func() as.Error {
			if iteration%2 == 0 {
				return runner.client.Put(writePolicy, key, as.BinMap{"v": iteration})
			}
			_, getErr := runner.client.Get(readPolicy, key)
			if getErr != nil && getErr.Matches(types.KEY_NOT_FOUND_ERROR) {
				return nil
			}
			return getErr
		}

		if !runner.runUntilSuccess(command) {
			return
		}
	}
}

// runUntilSuccess repeats command one tend interval apart until it succeeds.
// It returns false when stop closes first.
func (runner *loadRunner) runUntilSuccess(command func() as.Error) bool {
	firstStart := runner.output.elapsed()
	for {
		select {
		case <-runner.stop:
			return false
		default:
		}

		startedAt := runner.output.now()
		err := command()
		if err == nil {
			runner.stats.recordSuccess(firstStart, runner.output.elapsed())
			runner.ready()
			return true
		}

		message := err.Error()
		runner.stats.recordFailure(message)
		runner.output.writeIfOpen("failure", func(int64) line {
			return line{"startedAt": startedAt, "message": message}
		})

		select {
		case <-runner.stop:
			return false
		case <-time.After(runner.options.tendInterval):
		}
	}
}

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
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	watchRoundEvery     = 10 * time.Millisecond
	watchConnectTimeout = 50 * time.Millisecond
	watchServingRounds  = 3
	watchUsableLimit    = 60 * time.Second
)

// recordStore is one put and one get of bin v, one attempt each.
type recordStore interface {
	put(key *as.Key, value string) as.Error
	get(key *as.Key) (value any, found bool, err as.Error)
}

type clientRecordStore struct {
	client      *as.Client
	readPolicy  *as.BasePolicy
	writePolicy *as.WritePolicy
}

func (store clientRecordStore) put(key *as.Key, value string) as.Error {
	return store.client.Put(store.writePolicy, key, as.BinMap{"v": value})
}

func (store clientRecordStore) get(key *as.Key) (any, bool, as.Error) {
	record, err := store.client.Get(store.readPolicy, key)
	if err != nil {
		if err.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return record.Bins["v"], true, nil
}

// writeReadTally counts one batch of put-then-get checks.
type writeReadTally struct {
	written      int
	read         int
	mismatched   int
	failures     int
	firstFailure *string
}

func (tally *writeReadTally) fail(message string) {
	tally.failures++
	if tally.firstFailure == nil {
		tally.firstFailure = &message
	}
}

// putThenGet writes value under key, reads it back unless the put failed,
// and counts the outcome. label names the key in a failure.
func (tally *writeReadTally) putThenGet(store recordStore, key *as.Key, value, label string) {
	if err := store.put(key, value); err != nil {
		tally.fail(label + " put: " + err.Error())
		return
	}
	tally.written++

	got, found, err := store.get(key)
	switch {
	case err != nil:
		tally.fail(label + " get: " + err.Error())
	case found && got == value:
		tally.read++
	default:
		tally.mismatched++
	}
}

func (tally *writeReadTally) addTo(object line) {
	object["written"] = tally.written
	object["read"] = tally.read
	object["mismatched"] = tally.mismatched
	object["failures"] = tally.failures
	if tally.firstFailure == nil {
		object["firstFailure"] = nil
	} else {
		object["firstFailure"] = *tally.firstFailure
	}
}

// serve writes and reads one key in every partition of the namespace.
func serve(store recordStore, namespace, set, step string, elapsed func() time.Duration) (line, error) {
	keys, err := keysByPartition(namespace, set, step)
	if err != nil {
		return nil, err
	}
	began := elapsed()
	var tally writeReadTally
	for partition, key := range keys {
		tally.putThenGet(store, key, step+"-"+strconv.Itoa(partition), "partition "+strconv.Itoa(partition))
	}
	reply := line{"step": step, "partitions": partitionCount, "elapsedMs": (elapsed() - began).Milliseconds()}
	tally.addTo(reply)
	return reply, nil
}

// largeValue is a string of exactly size characters that starts with name.
func largeValue(name string, size int) string {
	return name + strings.Repeat("x", size-len(name))
}

// parseLarge reads STEP BYTES KEYS. Every key name must fit in BYTES.
func parseLarge(words []string) (step string, size int, keys int, ok bool) {
	if len(words) != 4 {
		return "", 0, 0, false
	}
	size, sizeErr := strconv.Atoi(words[2])
	keys, keysErr := strconv.Atoi(words[3])
	if sizeErr != nil || keysErr != nil || size <= 0 || keys <= 0 {
		return "", 0, 0, false
	}
	step = words[1]
	if len(step+"-"+strconv.Itoa(keys-1)) > size {
		return "", 0, 0, false
	}
	return step, size, keys, true
}

// writeReadLarge writes and reads keys records of size characters.
func writeReadLarge(store recordStore, namespace, set, step string, size, keys int, elapsed func() time.Duration) (line, error) {
	began := elapsed()
	var tally writeReadTally
	timed := &recordStoreTimer{store: store, elapsed: elapsed}
	for index := 0; index < keys; index++ {
		name := step + "-" + strconv.Itoa(index)
		key, err := as.NewKey(namespace, set, name)
		if err != nil {
			return nil, err
		}
		tally.putThenGet(timed, key, largeValue(name, size), name)
	}
	reply := line{"step": step, "bytes": size, "keys": keys, "maxMs": timed.longest.Milliseconds(),
		"elapsedMs": (elapsed() - began).Milliseconds()}
	tally.addTo(reply)
	return reply, nil
}

// recordStoreTimer keeps the longest single put or get.
type recordStoreTimer struct {
	store   recordStore
	elapsed func() time.Duration
	longest time.Duration
}

func (timer *recordStoreTimer) put(key *as.Key, value string) as.Error {
	began := timer.elapsed()
	err := timer.store.put(key, value)
	timer.longest = max(timer.longest, timer.elapsed()-began)
	return err
}

func (timer *recordStoreTimer) get(key *as.Key) (any, bool, as.Error) {
	began := timer.elapsed()
	value, found, err := timer.store.get(key)
	timer.longest = max(timer.longest, timer.elapsed()-began)
	return value, found, err
}

// watchProbe tracks the TCP rounds of one watch.
type watchProbe struct {
	order            []string
	firstConnectedAt map[string]int64
	connectedRounds  int
	servingAt        *int64
	lastClosedAt     *int64
}

func newWatchProbe() *watchProbe {
	return &watchProbe{firstConnectedAt: map[string]int64{}}
}

// recordRound takes one round started at roundStart over the resolved
// addresses and which of them connected. It returns true once the round
// completes the third consecutive round in which every address connected.
func (probe *watchProbe) recordRound(roundStart int64, addresses []string, connected []bool) bool {
	every := len(addresses) > 0
	for index, address := range addresses {
		if !slices.Contains(probe.order, address) {
			probe.order = append(probe.order, address)
		}
		if connected[index] {
			if _, already := probe.firstConnectedAt[address]; !already {
				probe.firstConnectedAt[address] = roundStart
			}
		} else {
			every = false
		}
	}

	if !every {
		closedAt := roundStart
		probe.lastClosedAt = &closedAt
		probe.connectedRounds = 0
		probe.servingAt = nil
		return false
	}
	if probe.connectedRounds == 0 {
		servingAt := roundStart
		probe.servingAt = &servingAt
	}
	probe.connectedRounds++
	return probe.connectedRounds >= watchServingRounds
}

func (probe *watchProbe) addresses() []line {
	addresses := make([]line, 0, len(probe.order))
	for _, address := range probe.order {
		object := line{"address": address, "firstConnectedAt": nil}
		if connectedAt, connected := probe.firstConnectedAt[address]; connected {
			object["firstConnectedAt"] = connectedAt
		}
		addresses = append(addresses, object)
	}
	return addresses
}

// probeRound resolves host and connects to every address at port at once.
func probeRound(host string, port int) ([]string, []bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	cancel()
	if err != nil {
		return nil, nil
	}

	connected := make([]bool, len(addresses))
	var round sync.WaitGroup
	for index, address := range addresses {
		round.Add(1)
		go func(index int, address string) {
			defer round.Done()
			conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort(address, strconv.Itoa(port)), watchConnectTimeout)
			if dialErr == nil {
				connected[index] = true
				conn.Close()
			}
		}(index, address)
	}
	round.Wait()
	return addresses, connected
}

// commandRunner answers the stdin commands.
type commandRunner struct {
	options *driverOptions
	output  *driverOutput
	store   recordStore
	stats   *loadStats
	monitor *routingMonitor

	watchLock sync.Mutex
	watchDone chan struct{}
}

// handle runs one command line. It returns false on quit.
func (runner *commandRunner) handle(commandLine string) bool {
	words := strings.Fields(commandLine)
	if len(words) == 0 {
		runner.replyError("malformed command: " + commandLine)
		return true
	}

	switch words[0] {
	case "quit":
		if len(words) != 1 {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		return false
	case "serve":
		if len(words) != 2 {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		reply, err := serve(runner.store, runner.options.namespace, runner.options.set, words[1], runner.output.elapsed)
		if err != nil {
			runner.replyError(err.Error())
			return true
		}
		runner.output.write("served", func(int64) line { return reply })
	case "large":
		step, size, keys, ok := parseLarge(words)
		if !ok {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		reply, err := writeReadLarge(runner.store, runner.options.namespace, runner.options.set, step, size, keys, runner.output.elapsed)
		if err != nil {
			runner.replyError(err.Error())
			return true
		}
		runner.output.write("large", func(int64) line { return reply })
	case "reset":
		if len(words) != 1 {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		runner.output.atomically(func(t int64, emit func(string, line)) {
			runner.stats.reset(runner.output.elapsed())
			runner.monitor.reset(t)
			emit("reset", line{})
		})
	case "watch":
		if len(words) != 3 {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		host, port, err := parseHostPort(words[1])
		if err != nil {
			runner.replyError("malformed command: " + commandLine)
			return true
		}
		if !runner.startWatch(words[1], host, port, words[2]) {
			runner.replyError("a watch is already running: " + commandLine)
		}
	default:
		runner.replyError("unknown command: " + commandLine)
	}
	return true
}

func (runner *commandRunner) replyError(message string) {
	runner.output.write("error", func(int64) line { return line{"message": message} })
}

// startWatch runs a watch on its own goroutine. It returns false when one is
// already running.
func (runner *commandRunner) startWatch(address, host string, port int, nodeName string) bool {
	runner.watchLock.Lock()
	defer runner.watchLock.Unlock()
	if runner.watchDone != nil {
		return false
	}
	done := make(chan struct{})
	runner.watchDone = done
	go func() {
		defer func() {
			runner.watchLock.Lock()
			runner.watchDone = nil
			runner.watchLock.Unlock()
			close(done)
		}()
		runner.watch(address, host, port, nodeName)
	}()
	return true
}

// waitForWatch lets a running watch reply, until deadline.
func (runner *commandRunner) waitForWatch(deadline time.Time) {
	runner.watchLock.Lock()
	done := runner.watchDone
	runner.watchLock.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
	}
}

// watch measures how soon the client uses node once address serves.
func (runner *commandRunner) watch(address, host string, port int, nodeName string) {
	probe := newWatchProbe()
	for {
		roundStart := runner.output.elapsed()
		addresses, connected := probeRound(host, port)
		if probe.recordRound(roundStart.Milliseconds(), addresses, connected) {
			break
		}
		time.Sleep(time.Until(runner.output.start.Add(roundStart + watchRoundEvery)))
	}

	servingAt := *probe.servingAt
	var usableAt *int64
	for {
		checkStart := runner.output.elapsed()
		if runner.monitor.usable(nodeName) {
			at := checkStart.Milliseconds()
			usableAt = &at
			break
		}
		if checkStart.Milliseconds()-servingAt >= watchUsableLimit.Milliseconds() {
			break
		}
		time.Sleep(time.Until(runner.output.start.Add(checkStart + watchRoundEvery)))
	}

	runner.output.write("watch", func(int64) line {
		reply := line{
			"address":          address,
			"node":             nodeName,
			"servingAt":        servingAt,
			"lastClosedAt":     nil,
			"addresses":        probe.addresses(),
			"usableAt":         nil,
			"usableMs":         nil,
			"ownershipChecked": true,
		}
		if probe.lastClosedAt != nil {
			reply["lastClosedAt"] = *probe.lastClosedAt
		}
		if usableAt != nil {
			reply["usableAt"] = *usableAt
			reply["usableMs"] = *usableAt - servingAt
		}
		return reply
	})
}

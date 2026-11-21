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
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
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
	object["firstFailure"] = nullable(tally.firstFailure)
}

// keysByPartition takes keys prefix-0, prefix-1, ... in order and keeps the
// first that falls in each partition, until every partition has one.
func keysByPartition(namespace, set, prefix string) ([partitionCount]*as.Key, error) {
	var keys [partitionCount]*as.Key
	found := 0
	for index := 0; found < partitionCount; index++ {
		key, err := as.NewKey(namespace, set, prefix+"-"+strconv.Itoa(index))
		if err != nil {
			return keys, err
		}
		partition := key.PartitionId()
		if keys[partition] == nil {
			keys[partition] = key
			found++
		}
	}
	return keys, nil
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

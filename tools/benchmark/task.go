// Copyright 2013-2016 Aerospike, Inc.
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
	"time"

	as "github.com/aerospike/aerospike-client-go"
)

type Task struct {
	client     *as.Client
	throughput int64

	rPct, wPct int64

	hist *Histogram
	bins as.BinMap

	wPolicy *as.WritePolicy
	rPolicy *as.BasePolicy

	sh *SyncedHistogram
}

func NewTask(client *as.Client, bins as.BinMap, histSize int, throughput, rPct, wPct int64) *Task {
	return &Task{
		client:     client,
		throughput: throughput,

		rPct: rPct,
		wPct: wPct,

		hist: NewHistogram(histSize),
		bins: bins,

		wPolicy: as.NewWritePolicy(0, 0),
		rPolicy: as.NewPolicy(),
	}
}

func (t *Task) Run(offset, count int) {
	var keyVal interface{}

	xr := NewXorRand()

	tType := xr.Int64() % 100

	tm := time.Now()
	for i := 1; i <= count; i++ {
		keyVal = offset*count + (i % count)
		if tType < t.rPct {
			t.get(keyVal)
		} else {
			t.put(keyVal)
		}
	}

	if time.Since(tm).Nanoseconds()-int64(tm.Nanosecond()) > 1e6 {
		t.sh.Merge(t.hist.Values())
	}
}

func (t *Task) put(keyVal interface{}) error {
	key, err := as.NewKey(*namespace, *set, keyVal)
	if err != nil {
		return err
	}

	tm := time.Now()
	err = t.client.Put(t.wPolicy, key, t.bins)
	t.hist.Add(time.Since(tm).Nanoseconds() / 10e6)

	return err
}

func (t *Task) get(keyVal interface{}) error {
	key, err := as.NewKey(*namespace, *set, keyVal)
	if err != nil {
		return err
	}

	tm := time.Now()
	_, err = t.client.Get(t.rPolicy, key)
	t.hist.Add(time.Since(tm).Nanoseconds() / 10e6)

	return err
}

/*
 * Copyright 2014-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	shared "github.com/aerospike/aerospike-client-go/v8/examples/shared"
)

const (
	numGoroutines   = 200
	opsPerGoroutine = 100
	keyRange        = 1000
)

var (
	successCount atomic.Int64
	errorCount   atomic.Int64
	commitCount  atomic.Int64
	abortCount   atomic.Int64
)

func main() {
	shared.Client.EnableMetrics(as.DefaultMetricsPolicy())
	log.Println("Metrics enabled")

	log.Printf("Starting %d goroutines, each doing %d transaction operations", numGoroutines, opsPerGoroutine)
	log.Println("Run with: go run -race main.go")

	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// printMetricsSnapshot("Final")
			elapsed := time.Since(startTime)
			log.Printf("Completed in %v", elapsed)
			log.Printf("Success: %d, Errors: %d, Commits: %d, Aborts: %d",
				successCount.Load(), errorCount.Load(), commitCount.Load(), abortCount.Load())
			return
		case <-ticker.C:
			// printMetricsSnapshot("Snapshot")
		}
	}
}

func worker(_ int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < opsPerGoroutine; i++ {
		txn := as.NewTxn()

		wp := as.NewWritePolicy(0, 0)
		wp.Txn = txn

		rp := as.NewPolicy()
		rp.Txn = txn

		keyID := rand.Intn(keyRange)
		key, err := as.NewKey(*shared.Namespace, *shared.Set, keyID)
		if err != nil {
			errorCount.Add(1)
			continue
		}

		bin := as.NewBin("data", rand.Int63())
		err = shared.Client.PutBins(wp, key, bin)
		if err != nil {
			errorCount.Add(1)
			abortTxn(txn)
			continue
		}

		_, err = shared.Client.Get(rp, key)
		if err != nil {
			errorCount.Add(1)
			abortTxn(txn)
			continue
		}

		bin2 := as.NewBin("counter", rand.Intn(1000))
		err = shared.Client.PutBins(wp, key, bin2)
		if err != nil {
			errorCount.Add(1)
			abortTxn(txn)
			continue
		}

		_, err = shared.Client.Commit(txn)
		if err != nil {
			errorCount.Add(1)
			abortTxn(txn)
			continue
		}

		commitCount.Add(1)
		successCount.Add(1)
	}
}

func abortTxn(txn *as.Txn) {
	_, _ = shared.Client.Abort(txn)
	abortCount.Add(1)
}

func printMetricsSnapshot(label string) {
	stats, err := shared.Client.Stats()
	if err != nil {
		log.Printf("Error getting stats: %v", err)
		return
	}

	b, _ := json.MarshalIndent(stats, "", "  ")
	log.Printf("=== %s Metrics ===\n%s", label, string(b))
}

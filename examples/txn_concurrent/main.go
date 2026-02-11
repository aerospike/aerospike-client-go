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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	shared "github.com/aerospike/aerospike-client-go/v8/examples/shared"
)

const (
	// Concurrent example settings
	numGoroutines   = 50
	opsPerGoroutine = 1000
	keyRange        = 100000

	// Batch example settings (10x load)
	batchSize          = 5000
	batchIterations    = 100
	mixedBatchSize     = 1000
	deleteRatio        = 0.2 // 20% of records to delete in mixed batch

	// Query example settings (10x load)
	queryDataSize      = 10000
	queryIterations    = 50
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

	switch *shared.ExampleType {
	case "default", "concurrent":
		runConcurrentExample()
	case "batch":
		runBatchExample()
	case "query":
		runQueryExample()
	case "all":
		runAllExamples()
	default:
		log.Printf("Unknown example type: %s. Use 'concurrent', 'batch', 'query', or 'all'", *shared.ExampleType)
	}
}

// runAllExamples runs all transaction examples sequentially
func runAllExamples() {
	log.Println("========================================")
	log.Println("Running ALL Transaction Examples")
	log.Println("========================================")

	log.Println("\n[1/3] Running Concurrent Transaction Example...")
	log.Println("----------------------------------------")
	runConcurrentExample()

	log.Println("\n[2/3] Running Batch Operations Example...")
	log.Println("----------------------------------------")
	runBatchExample()

	log.Println("\n[3/3] Running Query-then-Transact Example...")
	log.Println("----------------------------------------")
	runQueryExample()

	log.Println("\n========================================")
	log.Println("All Transaction Examples Completed")
	log.Println("========================================")
}

// runConcurrentExample demonstrates concurrent transaction operations
func runConcurrentExample() {
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
			collectStats()
			elapsed := time.Since(startTime)
			log.Printf("Completed in %v", elapsed)
			log.Printf("Success: %d, Errors: %d, Commits: %d, Aborts: %d",
				successCount.Load(), errorCount.Load(), commitCount.Load(), abortCount.Load())
			return
		case <-ticker.C:
			collectStats()
			log.Printf("Progress - Success: %d, Errors: %d, Commits: %d, Aborts: %d",
				successCount.Load(), errorCount.Load(), commitCount.Load(), abortCount.Load())
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

func collectStats() {
	// Collect stats snapshot (without printing)
	shared.Client.Stats()
}

// runBatchExample demonstrates various batch operations
func runBatchExample() {
	log.Println("=== Batch Operations ===")
	log.Printf("Batch size: %d, Iterations: %d", batchSize, batchIterations)

	// Setup: Create initial records
	log.Printf("Setting up %d initial records...", batchSize)
	keys := make([]*as.Key, batchSize)
	startTime := time.Now()

	// Use batch writes for faster setup
	setupBrecs := make([]as.BatchRecordIfc, batchSize)
	for i := range keys {
		key, err := as.NewKey(*shared.Namespace, *shared.Set, i)
		if err != nil {
			log.Fatalf("Failed to create key: %v", err)
		}
		keys[i] = key
		setupBrecs[i] = as.NewBatchWrite(nil, key,
			as.PutOp(as.NewBin("bin1", i)),
			as.PutOp(as.NewBin("bin2", i*10)),
			as.PutOp(as.NewBin("bin3", randString(100))),
		)
	}
	err := shared.Client.BatchOperate(nil, setupBrecs)
	if err != nil {
		log.Fatalf("Failed to setup records: %v", err)
	}
	log.Printf("Setup completed in %v", time.Since(startTime))

	// Example 1: BatchGet within a transaction (multiple iterations)
	log.Println("\n--- Example 1: BatchGet within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchGetExample(keys, iter)
	}

	// Example 2: BatchOperate (write) within a transaction (multiple iterations)
	log.Println("\n--- Example 2: BatchOperate (Write) within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchOperateWriteExample(keys, iter)
	}

	// Example 3: BatchDelete operation
	log.Println("\n--- Example 3: BatchDelete ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchDeleteExample(keys, iter)
	}

	// Example 4: BatchGetComplex within a transaction
	log.Println("\n--- Example 4: BatchGetComplex within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchGetComplexExample(keys, iter)
	}

	// Example 5: Mixed batch operations within a single transaction
	log.Println("\n--- Example 5: Mixed Batch Operations ---")
	for iter := 0; iter < batchIterations; iter++ {
		mixedBatchExample(iter)
	}

	log.Println("\n=== Batch Examples Completed ===")
}

// batchGetExample demonstrates BatchGet operation
func batchGetExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

	// Read records with BatchGet
	records, err := shared.Client.BatchGet(nil, keys, "bin1", "bin2", "bin3")
	if err != nil {
		log.Printf("[Iter %d] BatchGet failed: %v", iteration, err)
		return
	}

	nonNilCount := countNonNil(records)
	log.Printf("[Iter %d] BatchGet returned %d records (%d non-nil) in %v",
		iteration, len(records), nonNilCount, time.Since(startTime))
	collectStats()
}

// batchOperateWriteExample demonstrates BatchOperate with writes
func batchOperateWriteExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

	// Create batch write records with larger data
	brecs := make([]as.BatchRecordIfc, len(keys))
	for i := range brecs {
		brecs[i] = as.NewBatchWrite(nil, keys[i],
			as.PutOp(as.NewBin("bin1", i*100+iteration)),
			as.AddOp(as.NewBin("bin2", 1)),
			as.PutOp(as.NewBin("bin3", randString(100))),
			as.PutOp(as.NewBin("iteration", iteration)),
		)
	}

	// Execute batch write
	err := shared.Client.BatchOperate(nil, brecs)
	if err != nil {
		log.Printf("[Iter %d] BatchOperate failed: %v", iteration, err)
		return
	}

	// Verify changes
	records, err := shared.Client.BatchGet(nil, keys, "bin1", "bin2")
	if err != nil {
		log.Printf("[Iter %d] BatchGet failed: %v", iteration, err)
		return
	}

	nonNilCount := countNonNil(records)
	log.Printf("[Iter %d] BatchOperate updated %d records in %v", iteration, nonNilCount, time.Since(startTime))
	collectStats()
}

// batchDeleteExample demonstrates BatchDelete operation
func batchDeleteExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

	// Delete a portion of keys (20%)
	deleteCount := int(float64(len(keys)) * deleteRatio)
	if deleteCount < 1 {
		deleteCount = 1
	}
	keysToDelete := keys[:deleteCount]

	// First, verify records exist
	records, _ := shared.Client.BatchGet(nil, keysToDelete, "bin1")
	beforeCount := countNonNil(records)
	log.Printf("[Iter %d] Before delete - records exist: %d", iteration, beforeCount)

	dp := as.NewBatchDeletePolicy()
	dp.DurableDelete = true

	// Delete keys
	batchRecords, err := shared.Client.BatchDelete(nil, dp, keysToDelete)
	if err != nil {
		log.Printf("[Iter %d] BatchDelete failed: %v", iteration, err)
		return
	}

	// Count successful deletes
	deletedCount := 0
	for _, br := range batchRecords {
		if br.Err == nil && br.ResultCode == 0 {
			deletedCount++
		}
	}

	// Verify deletion
	records, _ = shared.Client.BatchGet(nil, keysToDelete, "bin1")
	afterCount := countNonNil(records)
	log.Printf("[Iter %d] BatchDelete deleted %d records (before: %d, after: %d) in %v",
		iteration, deletedCount, beforeCount, afterCount, time.Since(startTime))
	collectStats()

	// Re-create deleted records for next iteration
	brecs := make([]as.BatchRecordIfc, len(keysToDelete))
	for i, key := range keysToDelete {
		brecs[i] = as.NewBatchWrite(nil, key,
			as.PutOp(as.NewBin("bin1", i)),
			as.PutOp(as.NewBin("bin2", i*10)),
			as.PutOp(as.NewBin("bin3", randString(100))),
		)
	}
	shared.Client.BatchOperate(nil, brecs)
}

// batchGetComplexExample demonstrates BatchGetComplex with different read configurations
func batchGetComplexExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

	// Create BatchRead records with different configurations
	brecs := make([]*as.BatchRead, len(keys))
	for i := range brecs {
		if i%2 == 0 {
			// Read specific bins
			brecs[i] = as.NewBatchRead(nil, keys[i], []string{"bin1", "bin2"})
		} else {
			// Read all bins
			brecs[i] = as.NewBatchRead(nil, keys[i], nil)
			brecs[i].ReadAllBins = true
		}
	}

	err := shared.Client.BatchGetComplex(nil, brecs)
	if err != nil {
		log.Printf("[Iter %d] BatchGetComplex failed: %v", iteration, err)
		return
	}

	// Count successful reads
	readCount := 0
	for _, br := range brecs {
		if br.Record != nil {
			readCount++
		}
	}
	log.Printf("[Iter %d] BatchGetComplex read %d records in %v", iteration, readCount, time.Since(startTime))
	collectStats()
}

// mixedBatchExample demonstrates multiple batch operations in sequence
func mixedBatchExample(iteration int) {
	startTime := time.Now()

	// Create fresh keys for this example
	baseKey := 100000 + (iteration * mixedBatchSize)
	keys := make([]*as.Key, mixedBatchSize)
	for i := range keys {
		key, _ := as.NewKey(*shared.Namespace, *shared.Set, baseKey+i)
		keys[i] = key
	}

	// Step 1: Batch write to create records
	log.Printf("[Iter %d] Step 1: Creating %d records with BatchOperate...", iteration, len(keys))
	brecs := make([]as.BatchRecordIfc, len(keys))
	for i := range brecs {
		brecs[i] = as.NewBatchWrite(nil, keys[i],
			as.PutOp(as.NewBin("name", "item_"+randString(10))),
			as.PutOp(as.NewBin("count", i*10)),
			as.PutOp(as.NewBin("data", randString(200))),
		)
	}
	err := shared.Client.BatchOperate(nil, brecs)
	if err != nil {
		log.Printf("[Iter %d] BatchOperate (create) failed: %v", iteration, err)
		return
	}

	// Step 2: Batch read to verify
	log.Printf("[Iter %d] Step 2: Reading records with BatchGet...", iteration)
	records, err := shared.Client.BatchGet(nil, keys, "name", "count")
	if err != nil {
		log.Printf("[Iter %d] BatchGet failed: %v", iteration, err)
		return
	}
	log.Printf("[Iter %d] Read %d records", iteration, countNonNil(records))

	// Step 3: Batch update (increment count for all records)
	log.Printf("[Iter %d] Step 3: Updating all records with BatchOperate...", iteration)
	for i := range brecs {
		brecs[i] = as.NewBatchWrite(nil, keys[i],
			as.AddOp(as.NewBin("count", 5)),
			as.PutOp(as.NewBin("updated", true)),
		)
	}
	err = shared.Client.BatchOperate(nil, brecs)
	if err != nil {
		log.Printf("[Iter %d] BatchOperate (update) failed: %v", iteration, err)
		return
	}

	// Step 4: Batch delete some records (last 20%)
	deleteCount := int(float64(len(keys)) * deleteRatio)
	keysToDelete := keys[len(keys)-deleteCount:]
	log.Printf("[Iter %d] Step 4: Deleting %d records with BatchDelete...", iteration, len(keysToDelete))
	dp := as.NewBatchDeletePolicy()
	dp.DurableDelete = true
	_, err = shared.Client.BatchDelete(nil, dp, keysToDelete)
	if err != nil {
		log.Printf("[Iter %d] BatchDelete failed: %v", iteration, err)
		return
	}

	// Step 5: Final read
	log.Printf("[Iter %d] Step 5: Final read...", iteration)
	records, _ = shared.Client.BatchGet(nil, keys, "name", "count")
	remaining := countNonNil(records)
	log.Printf("[Iter %d] Completed: created %d, updated %d, deleted %d, remaining %d in %v",
		iteration, len(keys), len(keys), deleteCount, remaining, time.Since(startTime))
	collectStats()
}

// runQueryExample demonstrates query operations followed by batch updates
func runQueryExample() {
	log.Println("=== Query and Batch Update Operations ===")
	log.Printf("Data size: %d, Iterations: %d", queryDataSize, queryIterations)

	// Setup: Create records with indexed values
	log.Println("\nSetting up records for query example...")
	setupQueryData()

	// Example 1: Query to find keys, then update with individual writes
	log.Println("\n--- Example 1: Query-then-Update Pattern ---")
	for iter := 0; iter < queryIterations; iter++ {
		queryThenUpdateExample(iter)
	}

	// Example 2: Query to find keys, then batch update
	log.Println("\n--- Example 2: Query-then-BatchOperate Pattern ---")
	for iter := 0; iter < queryIterations; iter++ {
		queryThenBatchExample(iter)
	}

	log.Println("\n=== Query Examples Completed ===")
}

func setupQueryData() {
	startTime := time.Now()

	// Use batch writes for faster setup
	brecs := make([]as.BatchRecordIfc, queryDataSize)
	for i := 0; i < queryDataSize; i++ {
		key, _ := as.NewKey(*shared.Namespace, *shared.Set, 200000+i)
		// Distribute across 4 categories: A, B, C, D
		categories := []string{"A", "B", "C", "D"}
		category := categories[i%4]
		brecs[i] = as.NewBatchWrite(nil, key,
			as.PutOp(as.NewBin("id", 200000+i)),
			as.PutOp(as.NewBin("category", category)),
			as.PutOp(as.NewBin("value", i*5)),
			as.PutOp(as.NewBin("data", randString(150))),
		)
	}

	err := shared.Client.BatchOperate(nil, brecs)
	if err != nil {
		log.Printf("Failed to setup query data: %v", err)
		return
	}
	log.Printf("Created %d records with categories A, B, C, D in %v", queryDataSize, time.Since(startTime))
}

// queryThenUpdateExample queries for records and updates them with individual writes
func queryThenUpdateExample(iteration int) {
	startTime := time.Now()

	// Query to find keys
	stmt := as.NewStatement(*shared.Namespace, *shared.Set)

	// Rotate through categories for different iterations
	categories := []string{"A", "B", "C", "D"}
	targetCategory := categories[iteration%4]

	// Use expression filter to find records by category
	stmt.SetFilter(nil) // No secondary index filter
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpEq(
		as.ExpStringBin("category"),
		as.ExpStringVal(targetCategory),
	)

	rs, err := shared.Client.Query(qp, stmt)
	if err != nil {
		log.Printf("[Iter %d] Query failed: %v", iteration, err)
		return
	}

	// Collect keys from query results
	var keysToUpdate []*as.Key
	for res := range rs.Results() {
		if res.Err != nil {
			continue
		}
		keysToUpdate = append(keysToUpdate, res.Record.Key)
	}
	log.Printf("[Iter %d] Query found %d records with category %s", iteration, len(keysToUpdate), targetCategory)

	if len(keysToUpdate) == 0 {
		log.Printf("[Iter %d] No records found to update", iteration)
		return
	}

	// Update records with individual writes
	updateCount := 0
	for _, key := range keysToUpdate {
		err := shared.Client.PutBins(nil, key,
			as.NewBin("updated", true),
			as.NewBin("update_iter", iteration),
			as.NewBin("update_data", randString(50)),
		)
		if err != nil {
			log.Printf("[Iter %d] Update failed: %v", iteration, err)
			continue
		}
		updateCount++
	}

	log.Printf("[Iter %d] Updated %d records in %v", iteration, updateCount, time.Since(startTime))
	collectStats()
}

// queryThenBatchExample queries for records and batch updates them
func queryThenBatchExample(iteration int) {
	startTime := time.Now()

	// Rotate through categories for different iterations
	categories := []string{"A", "B", "C", "D"}
	targetCategory := categories[(iteration+2)%4] // Offset to use different categories than queryThenUpdateExample

	// Query to find records by category
	stmt := as.NewStatement(*shared.Namespace, *shared.Set)
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpEq(
		as.ExpStringBin("category"),
		as.ExpStringVal(targetCategory),
	)

	rs, err := shared.Client.Query(qp, stmt)
	if err != nil {
		log.Printf("[Iter %d] Query failed: %v", iteration, err)
		return
	}

	// Collect keys from query results
	var keys []*as.Key
	for res := range rs.Results() {
		if res.Err != nil {
			continue
		}
		keys = append(keys, res.Record.Key)
	}
	log.Printf("[Iter %d] Query found %d records with category %s", iteration, len(keys), targetCategory)

	if len(keys) == 0 {
		log.Printf("[Iter %d] No records found to update", iteration)
		return
	}

	// Batch update all matching records
	brecs := make([]as.BatchRecordIfc, len(keys))
	for i, key := range keys {
		brecs[i] = as.NewBatchWrite(nil, key,
			as.PutOp(as.NewBin("batch_updated", true)),
			as.PutOp(as.NewBin("batch_iter", iteration)),
			as.AddOp(as.NewBin("value", 100)), // Add 100 to value
			as.PutOp(as.NewBin("batch_data", randString(100))),
		)
	}

	err = shared.Client.BatchOperate(nil, brecs)
	if err != nil {
		log.Printf("[Iter %d] BatchOperate failed: %v", iteration, err)
		return
	}

	// Verify with BatchGet
	records, _ := shared.Client.BatchGet(nil, keys, "id", "category", "value", "batch_updated")
	log.Printf("[Iter %d] Batch updated %d records (verified %d) in %v",
		iteration, len(keys), countNonNil(records), time.Since(startTime))
	collectStats()
}

func countNonNil(records []*as.Record) int {
	count := 0
	for _, r := range records {
		if r != nil {
			count++
		}
	}
	return count
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

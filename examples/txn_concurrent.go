/*
 * Copyright 2014-2026 Aerospike, Inc.
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
)

// Scaled-down settings so the example completes quickly; the original
// load-test values were 50 goroutines x 1000 ops, batch 5000 x 100 and
// query 10000 x 50.
const (
	// Concurrent example settings
	numGoroutines   = 5
	opsPerGoroutine = 20
	keyRange        = 1000

	// Batch example settings
	batchSize       = 100
	batchIterations = 2
	mixedBatchSize  = 100
	deleteRatio     = 0.2 // 20% of records to delete in mixed batch

	// Query example settings
	queryDataSize   = 500
	queryIterations = 2
)

var (
	successCount atomic.Int64
	errorCount   atomic.Int64
	commitCount  atomic.Int64
	abortCount   atomic.Int64
)

// Exercise transactions under concurrency, batch operations inside
// transactions, and query-then-transact patterns.
func runTxnConcurrent() error {
	client.EnableMetrics(as.DefaultMetricsPolicy())
	log.Println("Metrics enabled")

	log.Println("[1/3] Running Concurrent Transaction Example...")
	runConcurrentExample()

	log.Println("[2/3] Running Batch Operations Example...")
	if err := runBatchExample(); err != nil {
		return err
	}

	log.Println("[3/3] Running Query-then-Transact Example...")
	if err := runQueryExample(); err != nil {
		return err
	}

	log.Println("All Transaction Examples Completed")
	return nil
}

// runConcurrentExample demonstrates concurrent transaction operations
func runConcurrentExample() {
	log.Printf("Starting %d goroutines, each doing %d transaction operations", numGoroutines, opsPerGoroutine)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go txnWorker(i, &wg)
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
			log.Printf("Completed in %v", time.Since(startTime))
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

func txnWorker(_ int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < opsPerGoroutine; i++ {
		txn := as.NewTxn()
		wp := as.NewWritePolicy(0, 0)
		wp.Txn = txn
		rp := as.NewPolicy()
		rp.Txn = txn

		keyID := rand.Intn(keyRange)
		key, err := as.NewKey(ns, set, keyID)
		if err != nil {
			errorCount.Add(1)
			continue
		}

		bin := as.NewBin("data", rand.Int63())
		if err := client.PutBins(wp, key, bin); err != nil {
			errorCount.Add(1)
			abortTxnCounted(txn)
			continue
		}

		if _, err := client.Get(rp, key); err != nil {
			errorCount.Add(1)
			abortTxnCounted(txn)
			continue
		}

		bin2 := as.NewBin("counter", rand.Intn(1000))
		if err := client.PutBins(wp, key, bin2); err != nil {
			errorCount.Add(1)
			abortTxnCounted(txn)
			continue
		}

		if _, err := client.Commit(txn); err != nil {
			errorCount.Add(1)
			abortTxnCounted(txn)
			continue
		}
		commitCount.Add(1)
		successCount.Add(1)
	}
}

func abortTxnCounted(txn *as.Txn) {
	_, _ = client.Abort(txn)
	abortCount.Add(1)
}

func collectStats() {
	// Collect stats snapshot (without printing)
	client.Stats()
}

// runBatchExample demonstrates various batch operations
func runBatchExample() error {
	log.Println("=== Batch Operations ===")
	log.Printf("Batch size: %d, Iterations: %d", batchSize, batchIterations)

	// Setup: Create initial records with batch writes for faster setup
	log.Printf("Setting up %d initial records...", batchSize)
	keys := make([]*as.Key, batchSize)
	startTime := time.Now()

	setupBrecs := make([]as.BatchRecordIfc, batchSize)
	for i := range keys {
		key, err := as.NewKey(ns, set, i)
		if err != nil {
			return err
		}
		keys[i] = key
		setupBrecs[i] = as.NewBatchWrite(nil, key,
			as.PutOp(as.NewBin("bin1", i)),
			as.PutOp(as.NewBin("bin2", i*10)),
			as.PutOp(as.NewBin("bin3", randString(100))),
		)
	}
	if err := client.BatchOperate(nil, setupBrecs); err != nil {
		return err
	}
	log.Printf("Setup completed in %v", time.Since(startTime))

	log.Println("--- Example 1: BatchGet within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchGetExample(keys, iter)
	}

	log.Println("--- Example 2: BatchOperate (Write) within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchOperateWriteExample(keys, iter)
	}

	log.Println("--- Example 3: BatchDelete ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchDeleteExample(keys, iter)
	}

	log.Println("--- Example 4: BatchGetComplex within Transaction ---")
	for iter := 0; iter < batchIterations; iter++ {
		batchGetComplexExample(keys, iter)
	}

	log.Println("--- Example 5: Mixed Batch Operations ---")
	for iter := 0; iter < batchIterations; iter++ {
		mixedBatchExample(iter)
	}

	log.Println("=== Batch Examples Completed ===")
	return nil
}

// batchGetExample demonstrates BatchGet operation
func batchGetExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

	records, err := client.BatchGet(nil, keys, "bin1", "bin2", "bin3")
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

	brecs := make([]as.BatchRecordIfc, len(keys))
	for i := range brecs {
		brecs[i] = as.NewBatchWrite(nil, keys[i],
			as.PutOp(as.NewBin("bin1", i*100+iteration)),
			as.AddOp(as.NewBin("bin2", 1)),
			as.PutOp(as.NewBin("bin3", randString(100))),
			as.PutOp(as.NewBin("iteration", iteration)),
		)
	}
	if err := client.BatchOperate(nil, brecs); err != nil {
		log.Printf("[Iter %d] BatchOperate failed: %v", iteration, err)
		return
	}

	// Verify changes
	records, err := client.BatchGet(nil, keys, "bin1", "bin2")
	if err != nil {
		log.Printf("[Iter %d] BatchGet failed: %v", iteration, err)
		return
	}
	log.Printf("[Iter %d] BatchOperate updated %d records in %v",
		iteration, countNonNil(records), time.Since(startTime))
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
	records, _ := client.BatchGet(nil, keysToDelete, "bin1")
	beforeCount := countNonNil(records)
	log.Printf("[Iter %d] Before delete - records exist: %d", iteration, beforeCount)

	dp := as.NewBatchDeletePolicy()
	dp.DurableDelete = true
	batchRecords, err := client.BatchDelete(nil, dp, keysToDelete)
	if err != nil {
		log.Printf("[Iter %d] BatchDelete failed: %v", iteration, err)
		return
	}

	deletedCount := 0
	for _, br := range batchRecords {
		if br.Err == nil && br.ResultCode == 0 {
			deletedCount++
		}
	}

	// Verify deletion
	records, _ = client.BatchGet(nil, keysToDelete, "bin1")
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
	client.BatchOperate(nil, brecs)
}

// batchGetComplexExample demonstrates BatchGetComplex with different read configurations
func batchGetComplexExample(keys []*as.Key, iteration int) {
	startTime := time.Now()

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
	if err := client.BatchGetComplex(nil, brecs); err != nil {
		log.Printf("[Iter %d] BatchGetComplex failed: %v", iteration, err)
		return
	}

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
		key, _ := as.NewKey(ns, set, baseKey+i)
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
	if err := client.BatchOperate(nil, brecs); err != nil {
		log.Printf("[Iter %d] BatchOperate (create) failed: %v", iteration, err)
		return
	}

	// Step 2: Batch read to verify
	log.Printf("[Iter %d] Step 2: Reading records with BatchGet...", iteration)
	records, err := client.BatchGet(nil, keys, "name", "count")
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
	if err := client.BatchOperate(nil, brecs); err != nil {
		log.Printf("[Iter %d] BatchOperate (update) failed: %v", iteration, err)
		return
	}

	// Step 4: Batch delete some records (last 20%)
	deleteCount := int(float64(len(keys)) * deleteRatio)
	keysToDelete := keys[len(keys)-deleteCount:]
	log.Printf("[Iter %d] Step 4: Deleting %d records with BatchDelete...", iteration, len(keysToDelete))
	dp := as.NewBatchDeletePolicy()
	dp.DurableDelete = true
	if _, err := client.BatchDelete(nil, dp, keysToDelete); err != nil {
		log.Printf("[Iter %d] BatchDelete failed: %v", iteration, err)
		return
	}

	// Step 5: Final read
	log.Printf("[Iter %d] Step 5: Final read...", iteration)
	records, _ = client.BatchGet(nil, keys, "name", "count")
	log.Printf("[Iter %d] Completed: created %d, updated %d, deleted %d, remaining %d in %v",
		iteration, len(keys), len(keys), deleteCount, countNonNil(records), time.Since(startTime))
	collectStats()
}

// runQueryExample demonstrates query operations followed by batch updates
func runQueryExample() error {
	log.Println("=== Query and Batch Update Operations ===")
	log.Printf("Data size: %d, Iterations: %d", queryDataSize, queryIterations)

	log.Println("Setting up records for query example...")
	if err := setupQueryData(); err != nil {
		return err
	}

	log.Println("--- Example 1: Query-then-Update Pattern ---")
	for iter := 0; iter < queryIterations; iter++ {
		queryThenUpdateExample(iter)
	}

	log.Println("--- Example 2: Query-then-BatchOperate Pattern ---")
	for iter := 0; iter < queryIterations; iter++ {
		queryThenBatchExample(iter)
	}

	log.Println("=== Query Examples Completed ===")
	return nil
}

func setupQueryData() error {
	startTime := time.Now()

	// Use batch writes for faster setup
	brecs := make([]as.BatchRecordIfc, queryDataSize)
	for i := 0; i < queryDataSize; i++ {
		key, _ := as.NewKey(ns, set, 200000+i)
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
	if err := client.BatchOperate(nil, brecs); err != nil {
		return err
	}
	log.Printf("Created %d records with categories A, B, C, D in %v", queryDataSize, time.Since(startTime))
	return nil
}

// queryThenUpdateExample queries for records and updates them with individual writes
func queryThenUpdateExample(iteration int) {
	startTime := time.Now()

	// Rotate through categories for different iterations
	categories := []string{"A", "B", "C", "D"}
	targetCategory := categories[iteration%4]

	// Use expression filter to find records by category
	stmt := as.NewStatement(ns, set)
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpEq(
		as.ExpStringBin("category"),
		as.ExpStringVal(targetCategory),
	)
	rs, err := client.Query(qp, stmt)
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
		err := client.PutBins(nil, key,
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

	// Offset to use different categories than queryThenUpdateExample
	categories := []string{"A", "B", "C", "D"}
	targetCategory := categories[(iteration+2)%4]

	stmt := as.NewStatement(ns, set)
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpEq(
		as.ExpStringBin("category"),
		as.ExpStringVal(targetCategory),
	)
	rs, err := client.Query(qp, stmt)
	if err != nil {
		log.Printf("[Iter %d] Query failed: %v", iteration, err)
		return
	}

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
	if err := client.BatchOperate(nil, brecs); err != nil {
		log.Printf("[Iter %d] BatchOperate failed: %v", iteration, err)
		return
	}

	// Verify with BatchGet
	records, _ := client.BatchGet(nil, keys, "id", "category", "value", "batch_updated")
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

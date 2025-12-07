# Async Operations Guide

This guide demonstrates how to perform asynchronous data ingestion and retrieval operations using the Aerospike Go client. In Go, asynchronous operations are achieved using goroutines and channels, allowing you to perform multiple database operations concurrently.

## Introduction

The Aerospike Go client supports both single and batch operations. While batch operations already leverage internal concurrency for efficiency, you can further improve performance by running multiple operations concurrently using Go's goroutines. This guide covers:

- **Single Async Operations**: Running individual Put/Get operations
- **Batch Async Operations**: Running multiple batch operations concurrently or leveraging built-in batch concurrency

Unlike some other language clients, the Go client doesn't have explicit async/await patterns. Instead, Go's native concurrency primitives (goroutines and channels) are used to achieve asynchronous behavior.

## Prerequisites

Before using async operations, ensure you have:

1. **Go 1.18+** installed
2. **Aerospike Go client v8** installed:
   ```bash
   go get github.com/aerospike/aerospike-client-go/v8
   ```
3. **Aerospike Server** running and accessible
4. Basic understanding of Go goroutines and channels

## Initializations

### Client Setup

First, establish a connection to your Aerospike cluster:

```go
package main

import (
    "log"
    as "github.com/aerospike/aerospike-client-go/v8"
)

func main() {
    // Connect to Aerospike cluster
    client, err := as.NewClient("127.0.0.1", 3000)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Your async operations here
}
```

### Policy Configuration

Configure policies for your operations:

```go
// Write policy for Put operations
writePolicy := as.NewWritePolicy(0, 0)
// this will enable server to persist key 
writePolicy.SendKey = true

// Read policy for Get operations
readPolicy := as.NewPolicy()

// Batch policy for batch operations
batchPolicy := as.NewBatchPolicy()
batchPolicy.ConcurrentNodes = 5  // Number of concurrent node operations
```

## Single Async Operations

Single async operations let you execute individual Put/Get calls independently. By using goroutines, you can run these operations concurrently on different records. The example below demonstrates how to perform a Get/Put on a single record using a goroutine — and the same pattern can be extended to handle multiple records concurrently.

**Note**: A single Get/Put call does not require a goroutine; it can be invoked directly. The provided examples use goroutines only to show how these operations can participate in concurrent workflows.

### Write Operations (Put)

The following example demonstrates how to write singe records:

```go

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// AsyncPutResult holds the result of an async PUT operation
type AsyncPutResult struct {
	Key   *as.Key
	Error error
}

// asyncPut performs a single PUT operation asynchronously
func asyncPut(client *as.Client, policy *as.WritePolicy, key *as.Key, bins ...*as.Bin) <-chan AsyncPutResult {
	resultChan := make(chan AsyncPutResult, 1) // buffered 1

	go func() {
		err := client.PutBins(policy, key, bins...)
		resultChan <- AsyncPutResult{Key: key, Error: err}
		close(resultChan)
	}()

	return resultChan
}

...

    client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal(err)
	}
    defer client.close()

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true

	// Create a single key and bins
	key, _ := as.NewKey(namespace, set, "key-1")
	bins := []*as.Bin{
		as.NewBin("id", 1),
		as.NewBin("value", "hello"),
	}

	// Start async PUT
	resultChan := asyncPut(client, writePolicy, key, bins...)

	// Wait for result
	result := <-resultChan

	if result.Error != nil {
		log.Printf("Error writing key %s: %v", result.Key.Value(), result.Error)
	} else {
		fmt.Printf("Successfully wrote key %s\n", result.Key.Value())
	}

```

### Read Operations (Get)

The following example demonstrates how to read single records:

```go

import (
	"fmt"
	"log"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// AsyncGetResult holds the result of an async GET operation
type AsyncGetResult struct {
	Key    *as.Key
	Record *as.Record
	Error  error
}

		
// asyncGet performs a single Get operation in a goroutine
func asyncGet(client *as.Client, policy *as.BasePolicy, key *as.Key, binNames ...string) <-chan AsyncGetResult {
	resultChan := make(chan AsyncGetResult, 1)

	go func() {
		record, err := client.Get(policy, key, binNames...)
		resultChan <- AsyncGetResult{Key: key, Record: record, Error: err}
		close(resultChan)
	}()

	return resultChan
}

...

    client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create a read policy (BasePolicy, not Policy)
	readPolicy := as.NewPolicy()

	// Example 1: Single async Get operation
	fmt.Println("Example 1: Single async Get operation")
	key, err := as.NewKey(namespace, set, "key-1")
	if err != nil {
		log.Fatal(err)
	}

	resultChan := asyncGet(client, readPolicy, key)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("Error reading key %s: %v", result.Key.Value(), result.Error)
	} else if result.Record == nil {
		log.Printf("Key %s not found", result.Key.Value())
	} else {
		fmt.Printf("Successfully read key %s: %v\n", result.Key.Value(), result.Record.Bins)
	}

  
```

### Operate Operations

You can also perform async Operate operations (combining read and write):

```go

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// AsyncOperateResult holds the result of an async Operate operation
type AsyncOperateResult struct {
	Key    *as.Key
	Record *as.Record
	Error  error
}

// asyncOperate performs a single Operate operation asynchronously
func asyncOperate(client *as.Client, policy *as.WritePolicy, key *as.Key, ops ...*as.Operation) <-chan AsyncOperateResult {
	resultChan := make(chan AsyncOperateResult, 1)

	go func() {
		record, err := client.Operate(policy, key, ops...)
		resultChan <- AsyncOperateResult{Key: key, Record: record, Error: err}
		close(resultChan)
	}()

	return resultChan
}

...

    client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true

	// Single key
	key, _ := as.NewKey(namespace, set, "key-1")

	// Define operations: Put + Add + Get
	ops := []*as.Operation{
		as.PutOp(as.NewBin("counter", 1)),
		as.AddOp(as.NewBin("counter", 1)),
		as.GetOp(),
	}

	// Start async operate
	resultChan := asyncOperate(client, writePolicy, key, ops...)

	// Wait for result
	res := <-resultChan

	// Check result
	if res.Error != nil {
		log.Printf("Error operating on key %s: %v", res.Key.Value(), res.Error)
	} else if res.Record == nil {
		log.Printf("No record returned for key %s", res.Key.Value())
	} else {
		fmt.Printf("Operate successful for key %s: %v\n", res.Key.Value(), res.Record.Bins)
	}

```

## Batch Async Operations

Batch operations in the Aerospike Go client already use internal concurrency (via goroutines) to process multiple nodes in parallel. However, you can run multiple batch operations concurrently for even better performance.

### Batch Write Operations

The following example demonstrates concurrent batch write operations:

```go

import (
	"fmt"
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// AsyncBatchWriteResult holds the result of a batch write
type AsyncBatchWriteResult struct {
	BatchIndex int
	Keys       []*as.Key
	Error      error
}

// asyncBatchPut performs a batch put asynchronously
func asyncBatchPut(client *as.Client, policy *as.BatchPolicy, writePolicy *as.BatchWritePolicy, batchIdx int, keys []*as.Key, bins []*as.Bin, results chan<- AsyncBatchWriteResult) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				results <- AsyncBatchWriteResult{BatchIndex: batchIdx, Keys: keys, Error: fmt.Errorf("panic: %v", r)}
			}
		}()

		// Convert bins to operations
		ops := make([]*as.Operation, len(bins))
		for i, bin := range bins {
			ops[i] = as.PutOp(bin)
		}

		batchRecords := make([]as.BatchRecordIfc, len(keys))
		for i, key := range keys {
			batchRecords[i] = as.NewBatchWrite(writePolicy, key, ops...)
		}

		err := client.BatchOperate(policy, batchRecords)
		results <- AsyncBatchWriteResult{BatchIndex: batchIdx, Keys: keys, Error: err}
	}()
}

func exampleAsyncBatchPuts(client *as.Client) error {
	// Run multiple batch write operations concurrently
	batchPolicy := as.NewBatchPolicy()
	batchPolicy.ConcurrentNodes = 5

	// Create BatchWritePolicy (not BasePolicy or WritePolicy)
	writePolicy := as.NewBatchWritePolicy()
	writePolicy.SendKey = true

	batchSize := 50
	totalKeys := 500
	numBatches := (totalKeys + batchSize - 1) / batchSize

	// Create all keys
	allKeys := make([]*as.Key, totalKeys)
	for i := 0; i < totalKeys; i++ {
		key, err := as.NewKey(namespace, set, fmt.Sprintf("batch-key-%d", i))
		if err != nil {
			return err
		}
		allKeys[i] = key
	}

	// Shared results channel
	results := make(chan AsyncBatchWriteResult, numBatches)

	// Start all batch writes
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > totalKeys {
			end = totalKeys
		}
		batchKeys := allKeys[start:end]
		bins := []*as.Bin{
			as.NewBin("batchId", batchIdx),
			as.NewBin("timestamp", time.Now().Unix()),
		}

		asyncBatchPut(client, batchPolicy, writePolicy, batchIdx, batchKeys, bins, results)
	}

	// Single goroutine in main reads all results
	successCount := 0
	errorCount := 0
	for i := 0; i < numBatches; i++ {
		res := <-results
		if res.Error != nil {
			log.Printf("Batch %d failed: %v", res.BatchIndex, res.Error)
			errorCount++
		} else {
			log.Printf("Batch %d completed successfully (%d keys)", res.BatchIndex, len(res.Keys))
			successCount++
		}
	}

	close(results)

	log.Printf("Batch writes complete: %d successful batches, %d failed batches", successCount, errorCount)
	return nil
}

...

	client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := exampleAsyncBatchPuts(client); err != nil {
		log.Println("Batch writes failed:", err)
	} else {
		log.Println("All batch writes completed successfully")
	}


```

### Batch Read Operations

The following example demonstrates concurrent batch read operations:

```go

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// AsyncBatchReadResult holds the result of an async batch read operation
type AsyncBatchReadResult struct {
	BatchIndex int
	Keys       []*as.Key
	Records    []*as.Record
	Error      error
}

// asyncBatchGet performs a batch Get operation in a goroutine
func asyncBatchGet(
	client *as.Client,
	policy *as.BatchPolicy,
	batchIdx int,
	keys []*as.Key,
	results chan<- AsyncBatchReadResult,
	binNames ...string,
) {
	go func() {
		// Protect against panic
		defer func() {
			if r := recover(); r != nil {
				results <- AsyncBatchReadResult{
					BatchIndex: batchIdx,
					Keys:       keys,
					Error:      fmt.Errorf("panic: %v", r),
				}
			}
		}()

		// Perform batch Get
		records, err := client.BatchGet(policy, keys, binNames...)

		// Send result
		results <- AsyncBatchReadResult{
			BatchIndex: batchIdx,
			Keys:       keys,
			Records:    records,
			Error:      err,
		}
	}()
}

// Example: Run multiple batch read operations concurrently
func exampleAsyncBatchGets(client *as.Client) error {
	batchPolicy := as.NewBatchPolicy()
	batchPolicy.ConcurrentNodes = 5

	batchSize := 50
	totalKeys := 500
	numBatches := (totalKeys + batchSize - 1) / batchSize

	// Create all keys
	allKeys := make([]*as.Key, totalKeys)
	for i := 0; i < totalKeys; i++ {
		key, err := as.NewKey(namespace, set, fmt.Sprintf("batch-key-%d", i))
		if err != nil {
			return err
		}
		allKeys[i] = key
	}

	// Shared results channel
	results := make(chan AsyncBatchReadResult, numBatches)

	// Start all batch read goroutines
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > totalKeys {
			end = totalKeys
		}

		batchKeys := allKeys[start:end]

		asyncBatchGet(client, batchPolicy, batchIdx, batchKeys, results)
	}

	// Collect results in SINGLE goroutine (main)
	totalRecords := 0
	errorCount := 0

	for i := 0; i < numBatches; i++ {
		res := <-results

		if res.Error != nil {
			log.Printf("Batch %d read failed: %v", res.BatchIndex, res.Error)
			errorCount++
			continue
		}

		// Count found records
		found := 0
		for _, r := range res.Records {
			if r != nil {
				found++
			}
		}

		log.Printf("Batch %d read: %d records found out of %d keys",
			res.BatchIndex, found, len(res.Keys))

		totalRecords += found
	}

	close(results)

	log.Printf("Batch reads complete: %d total records found, %d failed batches",
		totalRecords, errorCount)

	return nil
}

...

	client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Close()

	log.Println("Running async batch GET example...")

	if err := exampleAsyncBatchGets(client); err != nil {
		log.Println("Batch reads failed:", err)
	} else {
		log.Println("All batch reads completed successfully")
	}

```

### Batch Operate Operations

You can also perform batch operate operations concurrently:

```go

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	namespace = "test"
	set       = "demo"
)

// Result struct for batch operate
type AsyncBatchOperateResult struct {
	BatchIndex int
	Error      error
}

// asyncBatchOperate runs a whole batch in a goroutine
func asyncBatchOperate(
	client *as.Client,
	batchPolicy *as.BatchPolicy,
	writePolicy *as.BatchWritePolicy,
	batchIdx int,
	keys []*as.Key,
	results chan<- AsyncBatchOperateResult,
) {
	go func() {
		// ensure panic does not crash program
		defer func() {
			if r := recover(); r != nil {
				results <- AsyncBatchOperateResult{
					BatchIndex: batchIdx,
					Error:      fmt.Errorf("panic: %v", r),
				}
			}
		}()

		// Build operations for each record
		batchRecords := make([]as.BatchRecordIfc, len(keys))

		for i, key := range keys {
			ops := []*as.Operation{
				as.PutOp(as.NewBin("batchId", batchIdx)),
				as.AddOp(as.NewBin("counter", 1)),
				as.GetOp(),
			}
			batchRecords[i] = as.NewBatchWrite(writePolicy, key, ops...)
		}

		// Perform batch operate
		err := client.BatchOperate(batchPolicy, batchRecords)

		// Send result to shared channel
		results <- AsyncBatchOperateResult{
			BatchIndex: batchIdx,
			Error:      err,
		}
	}()
}

func exampleAsyncBatchOperate(client *as.Client) error {

	batchPolicy := as.NewBatchPolicy()
	batchPolicy.ConcurrentNodes = 5

	writePolicy := as.NewBatchWritePolicy()
	writePolicy.SendKey = true

	// Split into batches
	batchSize := 50
	totalKeys := 500
	numBatches := (totalKeys + batchSize - 1) / batchSize

	// Create all keys
	allKeys := make([]*as.Key, totalKeys)
	for i := 0; i < totalKeys; i++ {
		key, err := as.NewKey(namespace, set, fmt.Sprintf("batch-key-%d", i))
		if err != nil {
			return err
		}
		allKeys[i] = key
	}

	// Shared channel for all results
	results := make(chan AsyncBatchOperateResult, numBatches)

	// Launch goroutines for each batch
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > totalKeys {
			end = totalKeys
		}

		batchKeys := allKeys[start:end]

		asyncBatchOperate(client, batchPolicy, writePolicy, batchIdx, batchKeys, results)
	}

	// Single reader loop
	successCount := 0
	errorCount := 0

	for i := 0; i < numBatches; i++ {
		res := <-results

		if res.Error != nil {
			log.Printf("Batch %d failed: %v", res.BatchIndex, res.Error)
			errorCount++
		} else {
			log.Printf("Batch %d OK", res.BatchIndex)
			successCount++
		}
	}

	close(results)

	log.Printf(
		"Batch Operate done: %d success, %d failed",
		successCount, errorCount,
	)

	return nil
}

...

	client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	log.Println("Running async batch OPERATE example...")

	if err := exampleAsyncBatchOperate(client); err != nil {
		log.Println("Batch operate failed:", err)
	} else {
		log.Println("All batch operate completed successfully")
	}

```

### Using Batch Policy Concurrency

The batch operations already leverage internal concurrency. You can control this with the `ConcurrentNodes` setting:

```go
// Configure batch policy for optimal concurrency
batchPolicy := as.NewBatchPolicy()
batchPolicy.ConcurrentNodes = 10  // Process up to 10 nodes concurrently
batchPolicy.AllowPartialResults = true  // Continue even if some keys fail

// The batch operation will automatically use goroutines internally
records, err := client.BatchGet(batchPolicy, keys, "bin1", "bin2")
```

## Clean Up

Proper cleanup is essential to prevent resource leaks and ensure graceful shutdown.

### Closing the Client

Always close the client when done:

```go

    client, err := as.NewClient("127.0.0.1", 3000)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()  // Ensures client is closed even on panic
    
    // Your operations here

```


## Usages

1. **Use Batch Operations When Possible**: Batch operations are more efficient than multiple single operations
5. **Context Usage**: Use context for timeout and cancellation support
6. **Batch Policy Tuning**: Adjust `ConcurrentNodes` based on your cluster size and workload
7. **Connection Pooling**: The client manages connection pooling automatically, but ensure proper client initialization


# Tweaking Performance

This document details available mechanisms to optimize client performance characteristics, and tries to suggest good practices in using the client in a cluster of computers.

There are different options in the library to tweak performance for different workloads. It is important to keep in mind that you should benchmark and profile your application under reasonably production-like workloads to gain best possible performance.

This is a living document and we will keep it updated to help you make the best of our server and client solutions.

## Client Design Goals

We have tweaked and profiled the client library under various workloads to achieve the following goals:

  - **Minimal Memory Allocation**: We are conscious of this and have tried to remove as many allocations as possible. We are pooling buffers and hash objects whenever possible to achieve this goal.

  - **Customization Friendly**: We have added parameters to allow you to customize some variables when those variables could influence performance under different workloads.

  - **Determinism**: We have tried to keep the client inner workings deterministic. We have tried to stay away from data structures, or algorithms which are not deterministic. All pool and queue implementations in the client are bound in maximum memory size and perform in predetermined number of cycles. There are no heuristic algorithms in the client.

Please let us know if you can suggest an improvement anywhere in the library.

## Tweaking Performance / Best Practices

1. **Server Connection Limit**: Each server node has a limited number of file descriptors on the operating system for connections. No matter how big, this resource is still limited and can get exhausted by too many connections. Clients pool their connections to database nodes for optimal performance. If node connections are exhausted by existing clients, new clients won't be able to connect to the database. (e.g. When you start up a new application in the cluster)

  To guard against this, you should observe the following in your application design:

  1.1. **Use only one `Client` object in your application**: `Client` objects pool connections inside and synchronize their inner functionality. They are goroutine friendly. Use only one `Client` object in your application and pass it around.

  1.2. **Limit `Client` connection pool**: The default number of maximum connection pool size in a client object is 256. Even under extreme load in fast metal, clients rarely use more than even a quarter of this many connection. When there's no available connections in the pool, new connection to server will be made. If the pool is full, connections will be closed after their use to guard against too many connections.

  If this pool is too small, the client will waste time in connecting to the server for each new request; If too big, it will waste server connections.

  At its maximum number of 256 for each client, and `proto-fd-max` set to 10000 in your server node configuration, you can safely have around 50 clients **per server node**. In practice, this will approach 150 high performing clients. You can change this pool size in `ClientPolicy`, and then initialize your `Client` object using `NewClientWithPolicy(policy **ClientPolicy, hostname string, port int)` initializer.

  You can also guard against the number of new connections to each node using `ClientPolicy.LimitConnectionsToQueueSize = true`, so that if a connection is not available in the pool, the client will wait or timeout instead of creating a new client.

  **Example - Configuring connection pool settings:**
  ```go
  clientPolicy := NewClientPolicy()

  // Set maximum connections per node (default: 100)
  clientPolicy.ConnectionQueueSize = 100

  // Enforce connection limits (default: true)
  // When true, client will wait or timeout instead of creating new connections
  // when the pool is exhausted
  clientPolicy.LimitConnectionsToQueueSize = true

  // Maintain minimum connections (default: 0)
  // Only set if you can configure server proto-fd-idle-ms
  clientPolicy.MinConnectionsPerNode = 10

  // Set idle timeout to be less than server proto-fd-idle-ms
  // If server proto-fd-idle-ms = 60000ms, set this to ~55 seconds
  clientPolicy.IdleTimeout = 55 * time.Second

  client, err := NewClientWithPolicy(clientPolicy, "localhost", 3000)
  ```

2. **Initial Connection Buffer Size**: Client library retains its buffers to reduce memory allocation. The memory buffers are grown automatically, but the initial size can be set to avoid reallocations in case the initial size is always too small. If you ever determine that the initial pool size is sub-optimal for you application, you can set the size by `DefaultBufferSize`.

3. **Using `Bin` objects in `Put` operations instead of BinMaps**: `Put` method requires you to pass a map for bin values. While convenient, it will allocate an array of bins on each call, iterate on the map, and make `Bin` objects to use.

  If performance is absolutely important, use `PutBins` method and pass bins yourself.

  **Example - Less efficient (using BinMap):**
  ```go
  bins := BinMap{
      "bin1": 42,
      "bin2": "value",
  }
  client.Put(nil, key, bins)
  ```

  **Example - More efficient (using PutBins):**
  ```go
  client.PutBins(nil, key,
      NewBin("bin1", 42),
      NewBin("bin2", "value"),
  )
  ```

  The `PutBins` method avoids the overhead of:
  - Allocating a map for bin values
  - Iterating over the map
  - Creating `Bin` objects from map entries
  - Additional memory allocations

  Use `PutBins` when performance is critical, especially in high-throughput scenarios.

## Additional Best Practices

### Enable Client Logging

Each `Client` instance runs a background cluster tend goroutine that periodically polls all nodes for cluster status. This background goroutine generates log messages that reflect node additions/removal and any errors when retrieving node status, peers, partition maps, and racks. It's critical that user applications enable logging to receive these important messages.

The client uses the logger package for all logging. Configure logging appropriately for your environment:

```go
import "github.com/aerospike/aerospike-client-go/v8/logger"

// Set log level
logger.Logger.SetLevel(logger.INFO)

// Or use a custom logger
logger.Logger.SetLogger(customLogger)
```

See the [Logging documentation](https://aerospike.com/docs/develop/client/go/logging) for more details.

### Warm Up Connection Pool

The connection pool after connecting to the database is initially empty, and connections are established on a per-need basis, which can be slow and time out some initial commands. It is recommended to call the `client.WarmUp()` method right after connecting to the database to fill up the connection pool to the required service level.

The client provides three levels of WarmUp methods:

- `Client.WarmUp(count)` - Warms up connections for all nodes in the cluster
- `Cluster.WarmUp(count)` - Warms up connections for all nodes in the cluster (same as Client.WarmUp)
- `Node.WarmUp(count)` - Warms up connections for a specific node


```go
client, err := NewClient("localhost", 3000)
if err != nil {
    log.Fatal(err)
}

// Warm up the connection pool with 10 connections per node
warmed, err := client.WarmUp(10)
if err != nil {
    log.Printf("Warning: Only %d connections warmed up: %v", warmed, err)
}
```

For more granular control, you can warm up specific nodes:

```go
// Warm up a specific node
nodes := client.GetNodes()
if len(nodes) > 0 {
    warmed, err := nodes[0].WarmUp(10)
    if err != nil {
        log.Printf("Warning: Only %d connections warmed up for node: %v", warmed, err)
    }
}

// Or warm up via cluster
cluster := client.Cluster() // Access cluster if needed
warmed, err := cluster.WarmUp(10)
if err != nil {
    log.Printf("Warning: Only %d connections warmed up: %v", warmed, err)
}
```

### User-Defined Key

By default, the user-defined key is not stored on the server. It is converted to a hash digest which is used to identify a record. If the user-defined key must persist on the server, use one of the following methods:

1. **Set `BasePolicy.SendKey` to true**: The key is sent to the server for storage on writes, and retrieved on multi-record scans and queries.

```go
writePolicy := NewWritePolicy(0, 0)
writePolicy.SendKey = true
client.Put(writePolicy, key, bins)
```

**Note**: Avoid sending the key on reads unless necessary, as it adds overhead. The server will generate the hash digest from the key and validate that digest with the digest sent by the client.


### Replace Mode

In cases where all record bins are created or updated by a command, enable Replace mode on the command to increase performance. The server then does not have to read the old record before updating. Do not use Replace mode when updating a subset of bins.

```go
writePolicy := NewWritePolicy(0, 0)
writePolicy.RecordExistsAction = REPLACE
client.Put(writePolicy, key, bins)
```

### Policy Management

Each database command takes in a policy as the first argument. If the policy is identical for a group of commands, reuse them instead of instantiating policies for each command.

#### Set Policy Defaults

Get and overide defaults policy for `client`

```go

client, err := NewClient("localhost", 3000)
policy := client.GetDefaultPolicy()
policy.UseCompression = false
client.SetDefaultPolicy(policy)

// Use nil to use defaults
client.Put(nil, key, bins)
```


### Circuit Breaker

Employ a circuit breaker that activates when a maximum error count is reached for a node and rejects requests to that node until the specified error window expires. The following `ClientPolicy` fields can create a circuit breaker.

#### MaxErrorRate

Maximum number of errors allowed per node per `ErrorRateWindow`. Errors include connection errors, timeouts, and device overload. If maximum errors are reached, further requests to that node are retried to another node depending on replica policy. If maxRetries are exhausted, a backoff exception `ErrMaxErrorRate` is returned.

#### ErrorRateWindow

The number of cluster tend iterations that defines the window for `MaxErrorRate`. One tend iteration is defined as the tend interval (default 1 second) plus the time to tend all nodes. At the end of the window, the error count is reset to zero and backoff state is removed on all nodes.

```go
policy := NewClientPolicy()
policy.MaxErrorRate = 100        // Maximum errors per window
policy.ErrorRateWindow = 10      // Window size in tend iterations
policy.TendInterval = 1 * time.Second

client, err := NewClientWithPolicy(policy, "localhost", 3000)
```

The user application could optionally use a fallback cluster to handle traffic when the circuit breaker is employed.


### Resource Clean Up

#### Close Recordset

`Recordset` query iterators should always be closed after the iterator is no longer used. Failure to close the iterator when an exception occurs while processing query results may cause the query buffer to fill up and prevent server nodes from completing the query.

```go

recordset, err := client.ScanAll(nil, namespace, setName)
if err != nil {
    return err
}
defer recordset.Close()

for res := range recordset.Results() {
    if res.Err != nil {
        // handle error
        continue
    }
    // process record
    fmt.Println(res.Record)
}
```

#### Goroutine and Channel Cleanup

- When using goroutines for async operations, ensure all goroutines complete before closing the client
- Use `sync.WaitGroup` to wait for all goroutines to finish
- Close channels after all senders are done
- **Example**:
```go

import (
	"fmt"
	"log"
	as "github.com/aerospike/aerospike-client-go/v8"
)

...

client, err := as.NewClient("localhost", 3000)
if err != nil {
    log.Fatal(err)
}

var wg sync.WaitGroup
errorChan := make(chan error, 100)

// Start async operations
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(idx int) {
        defer wg.Done() // Ensure goroutine cleanup

        key, _ := as.NewKey(namespace, set, fmt.Sprintf("key-%d", idx))
        err := client.PutBins(nil, key, as.NewBin("id", idx))
        if err != nil {
            errorChan <- err
        }
    }(i)
}

// Wait for all goroutines to complete
wg.Wait()
close(errorChan) // Close channel after all senders are done

// Process errors
for err := range errorChan {
    if err != nil {
        log.Printf("Operation error: %v", err)
    }
}

```

### Use Operate for Multiple Operations

Use `Client.Operate()` to batch multiple operations (add/get) on the same record in a single call. This reduces network round trips and improves performance.

```go
// Instead of multiple calls:
client.Put(nil, key, BinMap{"counter": 1})
record, _ := client.Get(nil, key)
counter := record.Bins["counter"].(int)
client.Put(nil, key, BinMap{"counter": counter + 1})

// Use Operate for atomic operations:
record, err := client.Operate(nil, key,
    AddOp(NewBin("counter", 1)),
    GetOp(),
)
```

### Error Handling

Always check for errors returned by client operations. The client returns `Error` type which provides detailed information about failures:

```go
record, err := client.Get(nil, key)
if err != nil {
    // Check for specific error types
    if err.Matches(ErrKeyNotFound) {
        // Handle key not found
    } else if err.Matches(ErrTimeout) {
        // Handle timeout
    } else {
        // Handle other errors
        log.Printf("Error: %v", err)
    }
    return
}
```

### Additional Performance Considerations

#### Tend Interval

The `ClientPolicy.TendInterval` controls how often the client checks cluster status. The default is 1 second. Adjust based on your cluster stability requirements:

```go
policy := NewClientPolicy()
policy.TendInterval = 1 * time.Second  // Default
```

## Summary

Following these best practices will help you:

- **Improve Performance**: Through proper connection pooling, policy reuse, and efficient operations
- **Increase Reliability**: Through circuit breakers, proper error handling, and connection management
- **Reduce Resource Usage**: Through shared client instances and proper cleanup
- **Maintain Observability**: Through proper logging configuration

Remember to benchmark and profile your application under production-like workloads to determine the optimal settings for your specific use case.

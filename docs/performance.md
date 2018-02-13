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

2. **Initial Connection Buffer Size**: Client library retains its buffers to reduce memory allocation. The memory buffers are grown automatically, but the initial size can be set to avoid reallocations in case the initial size is always too small. If you ever determine that the initial pool size is sub-optimal for you application, you can set the size by `DefaultBufferSize`.

3. **Using `Bin` objects in `Put` operations instead of BinMaps**: `Put` method requires you to pass a map for bin values. While convenient, it will allocate an array of bins on each call, iterate on the map, and make `Bin` objects to use.

  If performance is absolutely important, use `PutBins` method and pass bins yourself.

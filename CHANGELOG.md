# Change History

## May 5 2025: v7.10.1

- **Fixes**
  - [CLIENT-3438] Adding `err` to return in `Node.requestRawInfo`.

## April 29 2025: v7.10.0

- **Fixes**
  - [CLIENT-3379] Setting TotalTimeout to 0 causes Scan / Query to fail (timeout)

## February 26 2025: v7.9.0

- **New Features**
  - [CLIENT-3334] Support old PHP7 client encoded boolean and null values.

- **Fixes**
  - [CLIENT-3348] Parsing error during node rack update.

## December 19 2024: v7.8.0

  Minor fix release.

- **Fixes**
  - [CLIENT-3217] Do not send `nil` or `NullValue` as key to the server.

## November 29 2024: v7.7.3

  Minor fix release.

- **Fixes**
  - [CLIENT-3196] Parse nil keys properly in scan/query operations.

## November 1 2024: v7.7.2

  Minor fix release.

- **Fixes**
  - [CLIENT-3156] Fix an issue where rack policy always returns the master node. Resolves #455

## September 18 2024: v7.7.1

  Hot Fix release. This version fixes a major bug introduced in v7.7.0. You should use this release instead.

- **Fixes**
  - [CLIENT-3122] Fix `nil` dereference in the tend logic.

## September 13 2024: v7.7.0

  Minor improvement release.

- **Improvements**
  - [CLIENT-3112] Correctly handle new error messages/error codes returned by AS 7.2.
  - [CLIENT-3102] Add "XDR key busy" error code 32.
  - [CLIENT-3119] Use Generics For a General Code Clean Up
    Uses several new generic containers to simplify concurrent access in the client.
    Uses a Guard as a monitor for the tend connection. This encapsulates synchronized tend connection management using said Guard.
  - Add documentation about client.WarmUp to the client initialization API.

- **Fixes**
  - [CLIENT-3082] BatchGet with empty Keys raises gRPC EOF error.

## August 12 2024: v7.6.1

  Minor improvement release.

- **Improvements**
  - [CLIENT-3071] Increase info command max response buffer size to 64 MiB.

## July 19 2024: v7.6.0

  Minor fix release.

- **Improvements**
  - [CLIENT-3045] Move proxy client build behind a build flag.
    This removes the GRPC compilation and potential namespace conflict from the default build and moves it behind the compiler build flag "as_proxy".

- **Fixes**
  - [CLIENT-3022] `Close()` throws a `nil` pointer error on `ProxyClient` without Authentication.
  - [CLIENT-3044] Circular reference in between `Client`<->`Cluster` causes memory leak when the client is not closed manually.
  - [CLIENT-3046] Wrong return type in Single Key Batch Operations with Multiple Ops per Bin.
  - [CLIENT-3047] Fix pointer value assignment in baseMultiCommand.parseKey (#443).
  - [CLIENT-3048] Use precomputed ops variable in batchIndexCommandGet.executeSingle (#442).
  - [CLIENT-3049] Use a specialized pool for grpc conns to prevent premature reaping.

## July 1 2024: v7.5.0

  This a minor feature and fix release.

- **New Features**
  - [CLIENT-2968] Support new v7.1 proxy server features:
    - `Info` command.
    - `QueryPolicy.QueryDuration`
  - [CLIENT-3012] Support new server 7.1 info command error response strings.

- **Improvements**
  - [CLIENT-2997] Scans should work in a mixed cluster of v5.7 and v6.4 server nodes.
  - [CLIENT-3020] Change `ReadModeSC` doc from server to client perspective.

- **Fixes**
  - [CLIENT-3019] Prevent Goroutine leak in `AuthInterceptor` for the Proxy Client.

## May 20 2024: v7.4.0

  This a minor fix release. We strongly suggest you upgrade to this version over the v7.3.0 if you use the `Client.BatchGetOperate` API.

- **Improvements**
  - Add code coverage tests to the Github Actions workflow.
  - Call the `CancelFunc` for the `context.WithTimeout` per linter suggestions in grpc calls.
  - Minor clean up and remove dead code.

- **Fixes**
  - [CLIENT-2943] `Client.BatchGetOperate` does not consider ops in single key transforms.
  - [CLIENT-2704] Client dev tests failing with new server map key restrictions.
  - Fix `as_performance` and `app_engine` build tags.

## May 3 2024: v7.3.0

> [!WARNING]
> Do not use this version if you are using the `Client.BatchGetOperate` API.

This is a major feature release of the Go client and touches some of the fundamental aspects of the inner workings of it.
We suggest complete testing of your application before using it in production.

- **New Features**
  - [CLIENT-2238] Convert batch calls with just one key per node in sub-batches to Get requests.
    If the number keys for a sub-batch to a node is equal or less then the value set in BatchPolicy.DirectGetThreshold, the client use direct get instead of batch commands to reduce the load on the server.

  - [CLIENT-2274] Use constant sized connection buffers and resize the connection buffers over time.

    The client would use a single buffer on the connection and would grow it
    per demand in case it needed a bigger buffer, but would not shrink it.
    This helped with avoiding using buffer pools and the associated
    synchronization, but resulted in excessive memory use in case there were a
    few large records in the results, even if they were infrequent.
    This changeset does two things:
            1. Will use a memory pool for large records only. Large records
               are defined as records bigger than `aerospike.PoolCutOffBufferSize`.
               This is a tiered pool with different buffer sizes. The pool
               uses `sync.Pool` under the cover, releasing unused buffers back to the
               runtime.
            2. By using bigger `aerospike.DefaultBufferSize` values, the user can
               imitate the old behavior, so no memory pool is used most of the time.
            3. Setting `aerospike.MinBufferSize` will prevent the pool using buffer sizes too small,
               having to grow them frequently.
            4. Buffers are resized every 5 seconds to the median size of buffers used over the previous period,
               within the above limits.

    This change should result in much lower memory use by the client.

  - [CLIENT-2702] Support Client Transaction Metrics. The native client can now track transaction latencies using histograms. Enable using the `Client.EnableMetrics` API.

- **Improvements**
  - [CLIENT-2862] Use default batch policies when the record level batch policy is nil.
  - [CLIENT-2889] Increase grpc `MaxRecvMsgSize` to handle big records for the proxy client.
  - [CLIENT-2891] Export various batch operation struct fields. Resolves #247.
  - Remove dependency on `xrand` sub-package since the native API is fast enough.
  - Linter Clean up.
  - `WritePolicy.SendKey` documentation, thanks to [Rishabh Sairawat](https://github.com/rishabhsairawat)
  - Replaced the deprecated `ioutil.ReadFile` with `os.ReadFile`. PR #430, thanks to [Swarit Pandey](https://github.com/swarit-pandey)

- **Fixes**
  - [CLIENT-2905] Fix inconsistency of handling in-doubt flag in errors.
  - [CLIENT-2890] Support `[]MapPair` return in reflection.
    This fix supports unmarshalling ordered maps into `map[K]V` and `[]MapPair` in the structs.

## April 10 2024: v7.2.1

This release updates the dependencies to mitigate security issues.

- **Fixes**
  - [CLIENT-2869] Update modules. Fix Allocation of Resources Without Limits or Throttling for `golang.org/x/net/http2`.

## March 28 2024: v7.2.0

This is a major update. Please test your code thoroughly before using in production.

- **New Features**
  - [CLIENT-2766] Support `RawBlobValue` in the Go client.
  - [CLIENT-2767] Support Persistent List Indexes.
  - [CLIENT-2823] Support `QueryDuration`.
  - [CLIENT-2831] Support `ReadPolicy.ReadTouchTTLPercent`.
  - [CLIENT-2240] Add more client statistics.
    - Adds the following statistics:
      - `circuit-breaker-hits`: Number of times circuit breaker was hit.
      - `connections-error-other`: Connection errors other than timeouts.
      - `connections-error-timeout`: Connection Timeout errors.
      - `connections-idle-dropped`: The connection was idle and dropped.
      - `connections-pool-overflow`: The command offered the connection to the pool, but .the pool was full and the connection was closed
      - `exceeded-max-retries`: Number of transactions where exceeded maximum number of retries specified in the policy
      - `exceeded-total-timeout`: Number of transactions that exceeded the specified total timeout
      - `total-nodes`: Total number of nodes in the cluster
  - Export private fields in `PartitionStatus` and add `Recordset.BVal`.

- **Improvements**
  - [CLIENT-2784] Do not use batch repeat flag on batch writes when `policy.SendKey` is set to `true`.
  - [CLIENT-2442] Document that Only `string`, `integer`, `bytes` are allowed as map key types; `Policy.SendKey` clarification.
  - Reduce the required Go version to 1.20 to support EL9; Update the dependencies.
  - Update `ExpCond()` doc to say that all action expressions must return the same type.

- **Fixes**
  - [CLIENT-2811] `RespondPerEachOp` doesn't work for list operation. To allow backwards compatibility, this change will change the default value of `RespondPerEachOp` to `true`.
  - [CLIENT-2818] Fix return type for `ExpListRemoveByValueRange`.
  - Update the proto grpc files to resolve namespace issues.
  - Improve the tests for `BatchOperations` to run on Github Actions.
  - Fix tests that relied on the server nsup-period setting to be larger than zero.
  - Fix Truncate test on slow servers.

## January 25 2024: v7.1.0

- **New Features**
  - Add `TaskId()` to `ExecuteTask`.
  - [CLIENT-2721] Make `PartitionFilter.Retry` public.

- **Improvements**

  - Clean up documentation and remove dependency of examples to the v6 version of the client.

- **Fixes**

  - [CLIENT-2725] `QueryExecute` (background query) doesn't work without operations.
  - [CLIENT-2726] Proxy doesn't handle invalid filter expression error in query.
  - [CLIENT-2727] Go proxy: Query Pagination never complete.
  - [CLIENT-2728] Fix an issue where Bin names were ignored if a FilterExpression was passed to the Query.
  - [CLIENT-2732] Go proxy: Not able to multiple query calls with the same statement.
  - [CLIENT-2759] Go proxy: Background query with Expression doesn't filter records.

## December 11 2023: v7.0.0

> [!CAUTION]
> This is a breaking release. It is required to allow upgrading your programs to the Aerospike Server v7. This program upgrade process required as a prerequisite to upgrading your cluster, otherwise seemless cluster upgrade will not be possible. The changes and their rationale are documented in the following section.

- **Breaking Changes**

  - [CLIENT-2713] Handle Normalized Integers in Maps and Lists.
    Aerospike Server v7 normalizes all positive integers in Maps and Lists into unsigned values for various efficiency reasons, and returns them as uint64. This effectively means that the type of positive `int64` values will be lost. Go client supported `uint64` types in lists and maps, and this change breaks that functionality by normalizing the values and removing the sign bits in case they are not needed. To support all versions of the server before and after the v7 consistently, the Go client will now behave like other Aerospike smart clients and automatically convert all unsigned int64 values inside maps and lists into signed values. This means a `math.MaxUint64` value in a List or Map will return as two's compliment: -1.
    Example:

    ```go
      client.Put(wpolicy, key, BinMap{"map": map[any]any{"max": uint64(math.MaxUint64), "typed": uint64(0)}})
    ```

    will return as:

  ```go
      rec, err := client.Get(rpolicy, key)
      // rec.Bins will be:
      // BinMap{"map": map[any]any{"max": int64(-1), "typed": int64(0)}}
  ```

    This will break all code that used to cast `rec.Bins["map"].(map[any]any)["max].(uin64)`. As a result, all such code should cast to int64 and then convert back to `uint64` via a sign switch.
    If you didn't use `uint64` values in Maps ans Lists, you should not be affected by this change.
    All the test cases that depended on the old behavior have been adapted to the new behavior.
  - [CLIENT-2719] Typed `GeoJSON` and `HLL` deserialization.
    The Go client would read GeoJSON and HLL values back as `string` and `[]byte` respectively. So if you read a record with bins of these types and wrote it directly back to the database, the type of these fields would be lost.
    The new version addresses this issue, but could be a breaking change if you have code that casts the values to the old `string` and `[]byte`. You now need to cast these values to `GeoJSONValue` and `HLLValue` types respectively.
  - [CLIENT-2484] Add `returnType` to supported `ExpMapRemoveBy*` and `ExpListRemoveBy*` methods.

  - [CLIENT-2319] Revise BatchReadAPIs to accept BatchReadPolicy argument. `NewBatchReadOps` no longer takes `binNames` and changes ops parameter to variadic for consistency.

    Changes the following Public API:

      ```go
        func NewBatchRead(key *Key, binNames []string) *BatchRead {
        func NewBatchReadOps(key *Key, binNames []string, ops []*Operation) *BatchRead {
        func NewBatchReadHeader(key *Key) *BatchRead {
      ```

      to

      ```go
        func NewBatchRead(policy *BatchReadPolicy, key *Key, binNames []string) *BatchRead {
        func NewBatchReadOps(policy *BatchReadPolicy, key *Key, ops ...*Operation) *BatchRead {
        func NewBatchReadHeader(policy *BatchReadPolicy, key *Key) *BatchRead {
      ```

  - Replace `WritePolicy` with `InfoPolicy` in `client.Truncate`.
  - Remove the deprecated `ClientPolicy.RackId`. Use `Policy.RackIds` instead.

- **New Features**

  - [CLIENT-2712] [CLIENT-2710] Support read replica policy in scan/query.
      This includes `PREFER_RACK` which allows scan/query to be directed at local rack nodes when possible.
  - [CLIENT-2434] Use 'sindex-exists' command in `DropIndexTask`.
  - [CLIENT-2573] Support `ExpRecordSize()`.
  - [CLIENT-2588] SINDEX Support for 'Blob' Type Elements.
  - [CLIENT-2721] Make PartitionFilter.Retry public.
  - Add TaskId() to ExecuteTask.

- **Improvements**
  - [CLIENT-2694] Use RawURLEncoding instead of RawStdEncoding in proxy authenticator.
  - [CLIENT-2616] Update dependencies to the latest, require Go 1.21
  - Remove HyperLogLog tests from the Github Actions suite
  - Remove Go v1.18-v1.20 from the Github Actions Matrix
  - Rename grpc proto definition files due to compiler limitations. Resolves #414

- **Fixes**
  - [CLIENT-2318] Fixes an issue where Expression in `BatchPolicy` takes precedence rather than `BatchDeletePolicy` in `BatchDelete`.


## November 1 2023: v6.14.1

  Hotfix.

- **Fixes**

  - [CLIENT-2624] `BatchGetOperate` triggering SIGSEGV nil pointer in the Go client.
    Caching of the operation is faulty and causes race conditions when used concurrently.
    This commit removes the caching which included a useless allocation and rarely, if ever, had any practical influence on performance.


## August 2023: v6.14.0

- **New Features**

  - Adds support for the AerospikeProxy and DBAAS service.


## July 28 2023: v6.13.0

- **New Features**

  - Add `ClientPolicy.SeedOnlyCluster` in clientPolicy for using only seedNodes for connection. (Github #407) thanks to [Sudhanshu Ranjan](https://github.com/sud82)
  - [CLIENT-2307] Add ExpInfinityValue() and ExpWildCardValue()
  - [CLIENT-2316], [CLIENT-2317] BatchResult Err and ResultCode are not always set on errors, Incorrect in-doubt flag handling during batch operation.

- **Improvements**

  - [CLIENT-2317] Better handle setting the Error.InDoubt flag in commands
  - [CLIENT-2379] Fix returning overall batch error
  - Move particle type outside of internal and back to `types/` dir.
  - Updated dependencies to address security advisories.
  - Improve tests in slow environments.
  - Remove atomic integer operations, move to Mutex.
  - [CLIENT-2315] Use BatchReadPolicy in BatchRead instead of BatchPolicy.
  - [CLIENT-2312] Remove support for old-style queries.
  - [CLIENT-2265] Increase the required Go version to 1.17 to be able to compile a dependency which itself was updated due to security issues.


- **Fixes**

  - Removes race condition from the client, resolves #399.
  - [CLIENT-2339] Developer tests detecting race condition after updating query protocol.
  - Assign DefaultInfoPolicy on Client initialization.
  - Fixed panics in Read Command Reflection API. (Github #402) thanks to [Yegor Myskin](https://github.com/un000)
  - [CLIENT-2283] Set correct return types in list/map read expressions.


## March 20 2023: v6.12.0

- **Improvements**

  - [CLIENT-2235] Minor documentation cleanup. Formatting, clarification and TODOs for next major version.
  - [CLIENT-2234] Export `BatchRead.Policy`
  - Update Ginkgo and Gomega to the latest version to avoid security issues.

- **Fixes**

  - [CLIENT-2230] The first row of batch records represented a server error instead of the over error in call.
  - [CLIENT-2233] Wrong FilterExpression size calculation in BatchOperate commands

## March 9 2023: v6.11.0

- **Improvements**

  - [CLIENT-2116] Check for completion/cancellation of the Scan/Query before firing off commands to nodes.

- **Fixes**

  - [CLINET-2227] PARAMETER_ERROR using expressions and batch write. Wire protocol was violated when the estimateExpressionSize was called.

## February 8 2023: v6.10.0

- **New Features**

  - [CLIENT-2170] Export `MapReturnTypes`, `MapOrderTypes` and `MapWriteModes`.
  - [CLIENT-2129], [CLIENT-2035] Adds `Expression.Base64()` and `ExpFromBase64()` to allow for serialization of expressions.

## January 23 2023: v6.9.1

  Hotfix release.

- **Fixes**
  - Mark `PartitionStatus.unavailable` and `PartitionStatus.replicaIndex` as transient in `PartitionFilter.EncodeCursor`.

## January 23 2023: v6.9.0

  NOTICE: This release is redacted. Please use v6.9.1.

- **New Features**
  - [CLIENT-2138] Allow PartitionFilter to persist its cursor, and retrieve the encoded cursor back to allow pagination. While the Go client supported pagination, it did not export the necessary data structures to allow that cursor to be persisted by the user. These data structures remain private, but two methods (`PartitionFilter.EncodeCursor` and `PartitionFilter.DecodeCursor`) are exported on the PartitionFilter to allow this mechanism to work.

## January 10 2023: v6.8.0

- **New Features**

  - [CLIENT-1988] dded base64 encoding functions `CDTContextToBase64` and `Base64ToCDTContext` for `CDTContext`.

- **Improvements**

  - Added an example for regexp expressions in Queries.

- **Fixes**

  - [CLIENT-1204] Retry logic will keep retrying on `KEY_NOT_FOUND` errors until `policy.MaxRetries` is hit or record is returned to the client.

## December 5 2022: v6.7.0

This is a minor improvement and fix release.

- **Improvements**

  - Improves testing for float64 precision formatting between amd64 and aarch64.

- **Fixes**

  - [CLIENT-2019] Write Batch operation with an invalid namespace record causes all batch transactions to fail.
  - [CLIENT-2020] Support `QueryPartitions` with non-nil filter (secondary index query)

## November 8 2022: v6.6.0

This is a minor improvement and fix release.

- **Improvements**

  - [CLIENT-1959] Retry a foreground scan/query partition to a different replica when a partition unavailable error occurs.
  - [CLIENT-1957] Handle Query, QueryExecute and QueryAggregate on non-existing sets without timing out.
  - [CLIENT-1956] Return an error when a read operation is used in a background query.

- **Fixes**

  - [CLIENT-1958] Prevent division by zero if the node array is empty in `Cluster.GetRandomNode()`.
  - [CLIENT-1955] Allow `BatchPolicy.SendKey` to be applied to all batch keys in all batch commands. Batch commands used in `BatchOperate` take an independent policy object. We used to ignore the `SendKey` attribute in those policies.

## October 26 2022: v6.5.0

This is a Minor feature and fix release. We recommend you update your client if you are using batch operate API.

- **New Features**

  - [CLIENT-1852] Add `ListSetWithPolicyOp` to list operations.

- **Fixes**

  - [CLIENT-1852] Fix `ListRemoveByIndexRangeOp` index param type.
  - [CLIENT-1855] Fix an issue where `ListGetByValueRangeOp`, `ListRemoveByValueRangeOp` with `nil` end value return empty slice.
  - [CLIENT-1867] Fix an issue where `BatchOperate` returns unexpected `ResultCode` for successful records when a record within a batch returns an error.

## September 16 2022: v6.4.0

This is a Major fix release. We recommend you update to this version ASAP.

- **Fixes**

  - [CLIENT-1827] IdleTimeout new default 0 may be missing tend thread reaping.
  - [CLIENT-1822] Scan/Query/Other streaming commands, including some Batch could put a faulty connection back to the pool after a cluster event where in certain conditions its buffer contents would end up in another scan and mix the results.
  - Update go.mod, redact buggy versions and update required Go version to v1.16

- **Improvements**

  - Update the examples for the new retriable scan/queries
  - Avoid indirection for `[]byte` conversion during reflection. Resolves #382.
  - Change v5 to v6 in some documentation.

## August 29 2022: v6.3.0

[**IMPORTANT NOTE**] A bug might occur when a client performing a scan hits a “Partition Unavailable” during an unstable cluster (in both high availability (AP) and strong consistency (CP) modes). Previous versions of the client aborted the scan and put the connection back into the pool, which might cause unprocessed results to be sent to a different transaction (of the same client), possibly resulting in incorrect application behavior. This has been fixed by Go Client v5.10.0 and v6.4.0.

- **New Features**

  - [CLIENT-1802] Support creating an secondary index on elements within a CDT using context. Supported by server v6.1+.
  - [CLIENT-1773] Change client configuration defaults:
    - Set `ClientPolicy.ConnectionQueueSize` from 256 to 100.
    - Set `ClientPolicy.IdleTimeout` from 55 to 0 secs.
    - Set `ClientPolicy.MaxErrorRate` from 0 to 100.
    - Set `Policy.TotalTimeout from` 0 to 1000ms for all commands except scan/query.

- **Fixes**

  - Fixed a few Linter warnings.

- **Improvements**

  - Documented a few constraints in HLL API docs.
  - Documented that PartitionFilter is both a filter and a cursor in the API docs.

## July 27 2022: v6.2.1

[**IMPORTANT NOTE**] A bug might occur when a client performing a scan hits a “Partition Unavailable” during an unstable cluster (in both high availability (AP) and strong consistency (CP) modes). Previous versions of the client aborted the scan and put the connection back into the pool, which might cause unprocessed results to be sent to a different transaction (of the same client), possibly resulting in incorrect application behavior. This has been fixed by Go Client v5.10.0 and v6.4.0.

This is mostly a re-release of v6.2.0, with one added minor change. It seems that we bungled the github tag somehow for that release. Upgrade from v6.2.0 to this version for the changes in that version to be applied to your code.

- **Fixes**

  - Add a nil check for error in batch retry to be on the safe side.

## June 30 2022: v6.2.0

[**IMPORTANT NOTE**] A bug might occur when a client performing a scan hits a “Partition Unavailable” during an unstable cluster (in both high availability (AP) and strong consistency (CP) modes). Previous versions of the client aborted the scan and put the connection back into the pool, which might cause unprocessed results to be sent to a different transaction (of the same client), possibly resulting in incorrect application behavior. This has been fixed by Go Client v5.10.0 and v6.4.0.

NOTE: It seems that the tag reference for this release was incorrect on Github, or we somehow confused the `Go mod`. Do not use this version. Use v6.2.1 instead.

This is a major fix release. We recommend upgrading to this release if you are using `BatchOperate` or `Scan/Queries`.

- **Fixes**

  - [CLIENT-1783] Client crashes when tracker is `nil` during Scan/Queries.
  - [CLIENT-1782] `BatchOperate` doesn't return error `BATCH_MAX_REQUESTS_EXCEEDED`.
  - [CLIENT-1781] Fix `BatchOperate`: use offset index to pick correct record.

## June 23 2022: v6.1.0

NOTE: This release contains a bug in `BatchOperate` command. If you are using that command, we strongly recommend you upgrade to v6.2.1.

This is a minor feature and bug fix release version.

- **New Features**

  - [CLIENT-1747] Add `EXISTS` return type for CDT read operations.

- **Fixes**

  - [CLIENT-1754] Go SDK doesn't support MapIncrementOp for batch write.
  - [CLIENT-1770] Assume background query is complete when the server 6.0+ returns 'not found' in 'query-show' info command. For the old servers, the client will check if it has already observed the job before. If it has, it will also assume the job has finished.

## April 7 2022: v6.0.0

NOTE: This release contains a bug in `BatchOperate` command. If you are using that command, we strongly recommend you upgrade to v6.2.1.

This is a major feature release. It adds several new features supported by the server v6, and drops supports for Predicate Expressions.

- **New Features**

  - [CLIENT-1699] Support New v6 Queries.
  - [CLIENT-1700] Support New Server v6 Roles and Privileges.
  - [CLIENT-1701] Support New Batch Writes.
  - [CLIENT-1702] Remove PredExp feature from the client.

## April 7 2022: v5.8.0

This is a major fix release. We recommend upgrading to this release if you are using authentication.

- **Improvements**

  - Adds notices regarding Auth issue to CHANGELOG and clarifies how to change code for an old breaking change in v3.
  - Forward compatibility with Server v6 regarding queries and scans not sending a fresh message header per partition.

- **Fixes**

  - [CLIENT-1695] Fixes a potential `nil` deference in `sessionInfo.isValid()` method.
  - Fixes an issue where with default policies and after certain errors the replica node was not selected on retry.

## December 6 2021: v5.7.0

- **Improvements**

  - Improve `Policy.deadline()` logic to use `MaxRetries` and `SocketTimeout` to calculate `TotalTimeout` when it is not set.
  - [CLIENT-1635] Allow Preventing Retries on Exhausted Connection Pools.
  - Do not test `PredExp` for server v5.8+.
  - Explicitly remove departed nodes from the partition map on cluster change.

## September 17 2021: v5.6.0

- **Fixes**

  - [CLIENT-1605] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. This fix invalidates the Session Token on unsuccessful login, Copy token from the connection buffer, and will consider tend interval in session expiration calculations.

## September 6 2021: v5.5.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

- **New Features**

  - [CLIENT-1586] Support Batch Read Operations.

- **Improvements**

  - Add authentication to info example.

- **Fixes**

  - Fix the worng udf name in predexp test.

## August 16 2021: v5.4.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

- **New Features**

  - [CLIENT-1576] Support a list of preferred racks rather than a single rack when replica is `PREFER_RACK`.
  - [CLIENT-1577] Support PKI authentication where the TLS certificate's common name (CN) specifies the Aerospike user name.
  - [CLIENT-1578] Support `scan-show` and `query-show` info commands.

- **Improvements**

  - Run fewer iterations for CDT RSHIFTEX and LSHIFTEX.
  - Add PKI authentication to the benchmark utility.

## August 2 2021: v5.3.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

- **Improvements**

  - Improve seeding of cluster nodes in tend. All seeds are discovered and added on the first step.
  - Add `-hosts` flag to test command arguments.

- **Fixes**

  - Fix where Bin names were not sent to the server in GetXXX commands and all bins were retrieved.

## June 28 2021: v5.2.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

Major fix release. We recommend updating to this version immediately.

- **New Features**

  - Add support for boolean bins in expressions (`ExpBoolBin`).
  - Add `Node.PeersGeneration`, `Node.PartitionGeneration`, `Node.RebalanceGeneration`.
  - Support TLS connections in benchmark tool. Resolves #313.
  - Expose Partition Health API to the user (`Cluster.Healthy()`). Resolves #334.

- **Improvements**

  - Do not keep connection on all client-side errors
  - Refactor batch commands to better reflect that keys are not created on batch requests.
  - Mention List/Map WriteFlags in List/Map Policy constructors.
  - Fix `ClientPolicy.ErrorRateWindow` documentation.
  - Fix benchmark document. Thanks to [Koji Miyata](https://github.com/miyatakoji)
  - Fix unidiomatic variable naming. Thanks to [Yevgeny Rizhkov](https://github.com/reugn)

- **Fixes**

  - Fix an issue where batch commands for a single node were not retried. Resolves #355.

## June 10 2021: v5.1.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

Major fix release. We recommend updating to this version immediately.

- **Improvements**

  - Set the error node on transaction failures.
  - Add compression and minConnsPerNode to benchmark tool options.

- **Fixes**

  - Add missing Compress commands.
  - Check if error is not `nil` before chaining. Resolves issue #353.
  - Handle `nil` case in `Node.String()`.
  - Correctly handle errors in `Connection.Read` and `Connection.Write`. Avoids shadowing of the error. Resolves issue #352.

## May 30 2021: v5.0.2

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

Minor fix release.

- **Fixes**

  - Improve handling and chaining of errors in `BatchCommand` retries.
  - Don't wrap in `chainErrors` if outer is `nil` and the inner error is of type `Error`.
  - Support reading back keys with original `List` values.

## May 27 2021: v5.0.1

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

Minor fix release.

- **Fixes**

  - Handle lack of key digests in `BatchExists` command.
  - Allow and handle nil arguments for `chainError`.
  - Avoid race condition in chaining prefined errors.

## May 10 2021: v5.0.0

[**IMPORTANT NOTE**] An authentication bug was introduced in Go client 5.0.0. As a result, the client may fail to refresh its session token after it expires, requiring the client to be restarted. If you are using password-based authentication, we highly recommend that you upgrade your client to version 5.6.0+, which you can do safely.

This is a major feature release. It is also a major breaking release. We have adopted Go's module system as recommended by the Go authors, so the new release moves the active branch to `v5`.
As such, the import path changes to `github.com/aerospike/aerospike-client-go/v5`. The `master` branch remains in place to allow maintenance for the older v4 classic version until most users
get the chance to upgrade.

This release also changes the way errors work in the client, and invalidates the old way. This allows the client to support Go's somewhat new `errors.Is` and `errors.As` API, and properly
chain errors together.

Also note that the Go Client now requires server version 4.9+ and will not work properly with older versions.

- **New Features**

  - Adopts module system and changes the import paths to `github.com/aerospike/aerospike-client-go/v5`
  - [CLIENT-1476] Support new expressions introduced in server version 5.6, including `ExpReadOp` and `ExpWriteOp`.
  - [CLIENT-1463] Support boolean particle type and make it opt-in for reflection API via `UseNativeBoolTypeInReflection`.
  - [CLIENT-1522] Support user quotas and statistics.
  - [CLIENT-1492] Remove ability to use old authentication protocol. This works behind the scenes and doesn't have any impact on user code.
  - [CLIENT-1081] Adds `Error` interface, changes all API signature to return `Error`.
  - Exports `AdminCommand` and its `QueryXXX` API.

- **Breaking Changes**

  - Limits keys to `int`, `string` and `[]byte` types. The old `ListValue` arrays as types are not supported anymore.
  - Remove TLS code support for golang v1.8 and before.
  - Moves `AerospikeError` from `/types` to the root of the package, and removes all other error type like `NodeError`
  - [CLIENT-1526] Removes `Policy.Priority`, `ScanPolicy.ScanPercent` and `ScanPolicy.FailOnClusterChange`
  - Removes `Recordset.Read()` and avoids multiplexing of `Records` channel in `Recordset.Results()`, and unexports the `Error` channel.
  - Remove legacy client code for old servers. Go client now requires server version 4.9+
  - Remove `Statement.PredExp`, and only use the `Policy.PredExp` to avoid confusion. `PredExp` has been deprecated and replaced by `Expression`.
  - Renames type `FilterExpression` to `Expression`.
  - `Client.GetBatchXXX()` will return `ErrFilteredOut` if an expression is passed to the API and some records were filtered out, regardless of `BatchPolicy.AllowPartialResults`.
  - `Client.CreateRole()` now requires quota information in the param list.
  - Removed `Connection.Authenticate()` API.
  - Renamed `GetOpForBin()` to `GetBinOp()`
  - Removed `ScanPolicy.ConcurrentNodes`. Now only uses `.MaxConcurrentNodes` to avoid confusion.
  - Moves the `RequestInfo()` under `Connection`.

- **Improvements**

  - Implement `GomegaStringer` interface to prevent very long error messages in tests.
  - Adds `ResultCode.String()`.

## April 9 2021: v4.5.0

Minor feature and fix release.

- **New Features**

  - Allows reading of boolean types from the server, supported in Aerospike server v5.6. The current client will not support writing boolean type to the server. That features will be supported in the upcoming Go client v5.

- **Improvements**

  - [CLIENT-1495] Tentatively check if a connection is allowed to avoid launching too many goroutines.

- **Fixes**

  - Implements correct and re-triable Scans for the Reflection API.
  - Fixes an obscure var shadowing bug in TLS connection handshake error logging.

## March 12 2021: v4.4.0

Minor fix and improvements release.

- **Fixes**

  - Fixes an issue where the client's reflection API expected certain `int` value types returned from the database. That assumption was wrong for `CDT`s and free form `List`s and `Map`s. The client will now convert types to each other per Go's conversion rules where possible.

- **Improvements**

  - Use a global TLS setting everywhere in tests.

## March 1 2021: v4.3.0

Minor feature and fix and major clean up release. While there aren't many user facing changes, the quality of the code has been markedly improved.
This release puts us on a good footing for the next few bigger releases.

- **New Features**:

  - [CLIENT-1457] Support scan pagination through `ScanPartitions()` with `PartitionFilter`

- **Fixes**

  - Fixes an issue where if errors and filtered records happened at the same time in Batch requests, no error would be returned to the user.

- **Improvements**

  - Makes the code samples more readable in the docs.
  - Fixes a lot of code samples in documentation, along with typos, etc.
  - Fixes copy/paste naming errors in the documentation. Thanks to [Yevgeny Rizhkov](https://github.com/reugn)
  - Removes a few unreachable lines from the code. Thanks to [Yevgeny Rizhkov](https://github.com/reugn)
  - Handles a few TLS connection related issues in tests.

## February 12 2021: v4.2.0

Major feature and improvements release.

- **New Features**:

  - [CLIENT-1192] Adds Support for partition scans. Queries which lack a `Statement.Filter` will be automatically converted to partition scans. If the cluster supports partition scans, all Scans and Queries will use the new protocol to allow retrying in case of some errors.
  - [CLIENT-1237] Adds Support for `MultiPolicy.MaxRecords` in scans and queries without `Statement.Filter`.
  - Adds `NewHosts` convenience function. (Github #320) thanks to [Yegor Myskin](https://github.com/un000)

- **Improvements**

  - Adds a few missing error checks.
  - Moves examples files to dedicated folders to avoid `multiple main function` errors for new users.
  - Some documentation clean up. (Github #314) thanks to [Shin Uozumi](https://github.com/sinozu)
  - Fix typo in example `NewKey()`. (Github #331) thanks to [Patrick Kuca](https://github.com/pkuca)
  - Adds an example to list operations (using operate and list policy).
  - Runs the XDR tests only when XDR is configured on the server.
  - Add TLS config to test params.
  - Mark `NewPredExpXXX` return value as the PredExp interface instead of concrete type. It will now group them under the `PredExp` interface in the docs.

- **Changes**
  - Only use `Policy.Priority` and `MultiPolicy.FailOnClusterChange` on server versions < 4.9. `Priority` is now deprecated and replaced with `MultiPolicy.RecordPerSecond`.
  - `Statement.TaskID` is deprecated and will be removed in the next major version.
  - `ScanPolicy.ConcurrentNodes` is deprecated and will be removed in the next major version.

… versions < 4.9

## January 25 2021: v4.1.0

Major feature release.

- **New Features**:

  - [CLIENT-1417] Adds Circuit-Breaker. Rejects command when assigned node's error rate exceeds `ClientPolicy.MaxErrorRate` over `ClientPolicy.ErrorRateWindow`.
  - [CLIENT-1410] Adds `Client.SetXDRFilter()`.
  - [CLIENT-1433] Adds `ExpMemorySize()` to expression filters.

- **Fixes**

  - Fixes an issue where remainder miscalculation would cause the connection pool to be smaller than it should have been. (Github #332) thanks to [ShawnZhang](https://github.com/xqzhang2015)

- **Improvements**

  - [CLIENT-1434] Reset peers, partition and rebalance generations on node tend errors.
  - Use named fields in `LimitedReader` initialization.
  - Skip `device_total_bytes` tests in Expressions for memory-only namespaces
  - Change unexported field check in marshaller in anticipation of go 1.16 changes

- **Changes**

  - Pack byte array header with string header codes when using msgpack to be consistent with server.
  - Adds `ResultCode.LOST_CONFLICT`
  - Change log level from Debug to Error for partition validation failures

- **Fixes**

  - Fix remainder calculation in `ConnectionHeap`.

## November 27 2020: v4.0.0

Major feature release. Deprecates `PredExp` filters and replaces them with the far more capable Expressions.

- **New Features**:

  - [CLIENT-1361] Replace predicate filters with new Aerospike Expressions.

- **Fixes**

  - Allows unmarshalling of bool fields to sub objects in reflection API. (Github #325)
  - Fixes an issue where BatchIndexGet commands were not retried in some circumstances.

- **Incompatible changes**:

  - Changes the `BitResizeFlagsXXX` enum types to `BitResizeFlags` type. This should not affect any code if the enums were used.
  - Changes the `ListSortFlagsXXX` enum types to`ListSortFlags` are now typed. This should not affect any code if the enums were used.

## November 9 2020: v3.1.1

Hotfix release. We recommend upgrading to this version, or cherry-picking the changeset to your vendored version if possible.

- **Fixes**

  - Handle cleanup cases in `Offer` and `DropIdleTail` for `singleConnectionHeap`. (Github #318)
  - Unlock the mutex in `singleConnectionHeap.Poll` if called after cleanup. (Github #323) thanks to [linchuan4028](https://github.com/linchuan4028)

- **Changes**

  - Removes support for versions prior to Go v1.12 due to incompatibility in the testing library we use. Go v1.9+ should still work, though they will not be tested in our tests.

## September 10 2020: v3.1.0

Minor fix release.

- **Fixes**

  - Fixes an issue where initial tend was not adhering to the `ClientPolicy.Timeout`. (CLIENT-1344)

## August 19 2020: v3.0.5

Minor fix release.

- **Fixes**

  - Corrects the maximum bin name size in error message.
  - Fixes geo coordinates in predexp tests due to more strict server validation.
  - Fixes misspelled words and doc links. PR #311, thanks to [Abhay](https://github.com/pnutmath)

## July 7 2020: v3.0.4

Minor fix release.

- **Fixes**

  - Fixes `Client.SetWhitelist` API.
  - Fixes an issue where `Whilelist` was not set during `QueryRole`.

## July 3 2020: v3.0.3

Minor fix release.

- **Fixes**

  - Resolves an issue where batch retry could return some nil records in some situations.

## June 24 2020: v3.0.2

Minor improvement release.

- **Fixes**

  - Fixes an issue where if a slice was pre-assigned on a struct, the data would not be allocated to it in reflection API. PR #302, thanks to [gdm85](https://github.com/gdm85)
  - Fixes an issue where `Node.GetConnection()` could in rare circumstances return no connection without an `error`. This would potentially cause a panic in VERY slow production servers.

- **Improvements**

  - Converts a few panics to errors in wire protocol encoding/decoding. Resolves issue #304.

## June 17 2020: v3.0.1

Minor bug fix release.

- **Fixes**

  - Fixes caching of embedded structs with options in alias. Resolves issue #301.

## June 8 2020: v3.0.0

Major feature release. There are a few minor breaking API changes. See `ClientPolicy`.

Note: There has been significant changes to clustering code. We recommend extensive testing before using in production.

- **New Features**

  - Adds support for Relaxed Strong Consistency mode. `ClientPolicy.LinearizeRead = true` has been removed and should be replaced with `policy.ReadModeSC = as.ReadModeSCLinearize`.
  - Adds support for whitelists in Roles.

## May 28 2020: v2.12.0

Minor feature release.

- **New Features**

  - Adds `MapCreateOp` and `ListCreateOp` in `Context` for CDTs.

## May 27 2020: v2.11.0

Major feature release.

- **New Features**

  - Adds HyperLogLog support.

- **Improvements**

  - Exports `estimateSize` on `Value` Datastructure. PR #299, thanks to [Sainadh Devireddy](https://github.com/sainadh-d)
  - Adds more detail regarding `ClientPolicy.IdleTimeout` in the documentation, and changes the default value to 55 seconds.

## May 12 2020: v2.10.0

Minor feature release.

- **New Features**

  - Adds `ClientPolicy.MinConnectionsPerNode`.

- **Improvements**

  - Returns distinct error when empty slice is passed to BatchGetObjects. PR #297, thanks to [Mohamed Osama](https://github.com/oss92)

## March 14 2020: v2.9.0

Minor feature release.

- **New Features**

  - Supports use of anonymous structs in reflection API. PR #287, thanks to [小马哥](https://github.com/andot)

## March 4 2020: v2.8.2

Hotfix.

- **Fixes**

  - Fixes a race condition introduced in the last release.

## March 4 2020: v2.8.1

Minor fix and improvements.

- **Improvements**

  - Uses a `sync.Pool` to preserve the connection buffers on close to remove pressure from the allocator and the GC during connection churns.

- **Fixes**

  - Cleanup the data structure cross refs on Cluster.Close to help GC free the objects.

## February 28 2020: v2.8.0

Minor feature release.

- **New Features**

  - Allows `,omitempty` tag to be used in struct tags. It behaves the same as the stdlib json. Note that there should be no whitespace between the comma and the rest of the tag.

## January 30 2020: v2.7.2

Minor fix release.

- **Fixes**

  - Resolves an issue where an invalid/malformed compressed header could cause a panic by reporting wrong compressed data size. Resolves #280.

## January 20 2020: v2.7.1

Minor fix release.

- **Fixes**

  - Fixes an issue where an error was not checked after a read in `multi_command.go` and would cause a panic. Resolves #280.

## December 24 2019: v2.7.0

Minor feature and fix release.

- **New Features**

  - Adds support for client/server wire transport compression.
  - Adds support for descending CDT list order.

- **Fixes**

  - Fixes an issue where unpacking `Value` objects would cause an infinite loop. PR #273, thanks to [small-egg](https://github.com/small-egg)

## November 25 2019: v2.6.0

Minor feature release.

- **New Features**

  - Supports correct Query/Scans via `Scan/QueryPolicy.FailOnClusterChange`

- **Fixes**

  - Fixes an issue where the client using multiple seeds via DNS or Load Balancer would fail to connect if more than one of them were unreachable.

## November 8 2019: v2.5.0

Major feature release.

- **New Features**

  - Adds support for predicate expressions in all transactions. See `Policy.Predexp`.

## October 29 2019: v2.4.0

Major feature release.

- **New Features**

  - Adds support for bitwise operations.
  - Adds support for nested CDTs.

## October 17 2019: v2.3.0

Major feature release.

- **New Features**

  - Adds support for mixed security modes in cluster to enable rolling upgrade with security changes.
  - Adds support for delete record operation `DeleteOp()` in `Client.Operate()`.
  - Adds support for write operations in background scan/query.
  - Adds support for `Scan/QueryPolicy.RecordsPerSecond` field to limit throughput.

## August 13 2019: v2.2.1

Minor improvement release.

- **Improvements**

  - Supports the `Write` role in server v4.6.0.2+

## May 21 2019: v2.2.0

Minor Fixes and improvements release.

- **Fixes**

  - Fixes an issue where an empty connection pool would cause a lock contention that would in turn lead to high CPU load.
  - Fixes an issue where in some circumstances connection pool would be depleted of connections.
  - Fixes an issue where the replica node would not be selected in case of master-node failure in `Policy.ReplicaPolicy.SEQUENCE` for reads.

- **Improvements**

  - Transactions will not count a lack of connection in the node's connection pool as an iteration anymore.

## April 25 2019: v2.1.1

Minor Fixes and improvements release.

- **Fixes**

  - Fixes an issue where meta tags were ignored in reflection API for `ScanAllObjects`/`QueryObjects`/`BatchGetObjects`. Resolves #260.

- **Improvements**

  - Tend won't send `rack:` command to the nodes if `ClientPolicy.RackAware` is not set. PR #259, thanks to [Dmitry Maksimov](https://github.com/kolo)
  - Adds a new GeoJson example.

## April 11 2019: v2.1.0

Minor Feature and Improvements release.

- **New Features**

  - Adds `WarmUp` method for `Client`, `Cluster` and `Node`. This method will fill the connection queue to ensure maximum and smooth performance on start up.

- **Improvements**

  - Simplify connection Timeout calculation and floor the min timeout to 1ms.
  - Simplify resetting server timeout for each iteration.
  - Adds a few pre-defined errors to avoid allocating them during runtime.

- **Changes**

  - Adds a TLS connection example.
  - Adds `Cap` method to `connectionHeap`.

## March 19 2019: v2.0.0

Major release. There are some breaking changes, both syntactically and semantically.
Most changes are minor, and can be fixed with relative ease.
The only major issue is that the behavior of the client when a key does not exist has changed.
It used to return no error, but `nil` `Record.Bins`. Now it returns `ErrKeyNotFound` error.
This is a significant changes, and you should search your code for all instances of `Bins == nil` and adapt the code accordingly.

- **Major**:

  - Optimizes connection creation out of the transaction pipeline and makes it async.
  - Put a threshold on the number of connections allowed to open simultaneously. Controlled via `ClientPolicy.OpeningConnectionThreshold`.
  - Do not clear partition map entry when a node reports that it no longer owns that partition entry.
  - Uses rolling timeout instead of strict timeout for connections.
  - Remove `ToValueArray` and `ToValueSlice` methods to discourage such suboptimal use. Changes `QueryAggregate` signature to remove the need for those methods.
  - Remove unnecessary conversion from BinMap to Bins in reflection API to speedup the command an avoid unnecessary memory allocations.
  - Use shorter intervals with exponential back-off for tasks.

- **Breaking**:

  - `Get`/`Put`/`Touch`/`Operate` and `ExecuteUDF` commands will return an `ErrKeyNotFound` error when the key does not exist in the database. The old behavior used to be not to return an error, but have an empty `Record.Bins`.
  - Renames `Statement.Addfilter` to `Statement.SetFilter`, change the name and type of `Statement.Filters` to `Statement.Filter`.
  - Remove `ClientPolicy.RequestProleReplicas`. THe client will always request them.
  - Removes `ScanPolicy.ServerSocketTimeout` and `QueryPolicy.ServerSocketTimeout` in favor of the already existing `Policy.SocketTimeout`.
  - Renames `Policy.Timeout` to `Policy.TotalTimeout` to make the naming consistent with other clients.
  - Moves `atomic` package to internal.
  - Moves `ParticleType` package to internal.
  - Moves `RequestNodeInfo` and `RequestNodeStats` to methods on Node object, and adds `InfoPolicy` to the relevant API signatures.
  - Removes `WaitUntilMigrationIsFinished` from `Scan`/`Query` policies.
  - Changes `NewConnection` method signature, makes `LoginCommand` private.
  - Makes `OperationType` private.
  - Remove long deprecated method for pool management.
  - Removes unused `ReadN` method in `Connection`.
  - Embeds Policies as values and not pointers inside `MultiPolicy`, `ScanPolicy`, `QueryPolicy`

- **Minor**:
  - Fixes a race condition in the `AdminCommand`.
  - Synchronize the `XORShift` to avoid race conditions.
  - Completely removes deprecated LDT code.

## March 11 2019: v1.39.0

Major improvements Release.

- **Improvements**

  - Significantly improves `Batch`/`Scan`/`Query`/`UDF`/`QueryAggregate` performance, up to 10x depending on the number of records.

- **Changes**

  - Removes `BatchPolicy.UseBatchDirect` from the code since it is not supported on the server anymore.

## February 21 2019: v1.38.0

- **New Features**

  - Support new server `truncate-namespace` command via `Client.Truncate` when `set` is not specified.

- **Improvements**

  - The client will not clear a partition map entry when a node reports that it no longer owns that partition entry until another node claims ownership.
  - Adapt UDF test for new server changes. The server will not return an error after `RemoveUDF` if the UDF did not exist.
  - Improves a few tests and relaxes tolerances in tests to accommodate slower cloud test environments.

- **Fixes**

  - Fixes a race condition in XOR shift RNG.
  - Fixes a race condition in the AdminCommand.

## December 3 2018: v1.37.0

- **New Features**

  - Support lut-now parameter for Client.Truncate() in servers that support and require it.
  - Added support for CDT Map Relative Ops: `MapGetByKeyRelativeIndexRangeOp`, `MapGetByKeyRelativeIndexRangeCountOp`, `MapGetByValueRelativeRankRangeOp`, `MapGetByValueRelativeRankRangeCountOp`, `MapRemoveByKeyRelativeIndexRangeOp`, `MapRemoveByKeyRelativeIndexRangeCountOp`.
  - Added support for CDT List Relative Ops: `ListGetByValueRelativeRankRangeOp`, `ListGetByValueRelativeRankRangeCountOp`, `ListRemoveByValueRelativeRankRangeOp`, `ListRemoveByValueRelativeRankRangeCountOp`.
  - Added `INFINITY` and `WILDCARD` values for use in CDT map/list comparators.

- **Improvements**

  - Increase default `Policy.SocketTimeout` to 30s. If `SocketTimeout` is longer than `Timeout`, `Timeout` will be used instead silently. This change is done for the client to perform more intuitively in cloud environments.
  - Never return a random node if a node was not found in the partition map.
  - Return more descriptive error messages on various partition map and other node related errors.

- **Changes**

  - Remove the ability to force old batch direct protocol on the client because the server will be removing support for the old batch direct protocol.
  - Update admin message version to 2.
  - Remove unused error codes.
  - Remove Go 1.7 and 1.8 from travis tests due to incompatibility with the test framework.

## November 1 2018: v1.36.0

Feature Release.

- **New Features**

  - Support rackaware feature. You need to set the `ClientPolicy.RackAware = true`, and set the `ClientPolicy.RackId`. All read operations will try to choose a node on the same rack if `Policy.ReplicaPolicy = PREFER_RACK`. This feature is especially useful when the app/cluster are on the cloud and network throughput over different zones are price differently.

- **Improvements**

  - Update Operate command documentation.
  - Improve an expectation in a CDT Map test.
  - Move UDF object test to the proper file.
  - Support float64 struct fields when the value of the field has been changed inside lua and set to int - will only affect clusters which support float.
  - Fixes an issue where key value was sent and cause server PARAMETER_ERROR via the operate command if policy.SendKey was set but no write operations were passed.
  - Updated README example with clarification.

- **Fixes**

  - Fixes an issue where multiple operation results for a bin would be appended to the first result if it was a list.

## October 2 2018: v1.35.2

Improvement release.

- **Improvements**

  - Do not allocate a partition map on each tend unless needed.
  - Adds `ConnectionsClosed` stat and sets the connection and dataBuffer to nil in a few places to help the GC.
  - Use a heap data structure for connection pooling instead of a queue.
    This allows better management of connections after a surge, since it keeps the unused connection in the bottom of the heap to close.
    It also helps with performance a bit due to better caching of the data structure in CPU.

## September 18 2018: v1.35.1

Hot fix release. We recommend updating to this version if you are using authentication.

- **Fixes**

  - Fixes a regression to avoid hashing passwords per each login, using the cached password.

- **Changes**

  - Minor code clean up and dead code removal.

## August 29 2018: v1.35.0

- **New Features**

  - Support for external authentication (LDAP).
  - Support Map and List WriteFlags: `NoFail` and `Partial`.
  - Support load balancers as seed node.

- **Changes**

  - Change default Scan/Query `ServerSocketTimeout` to 30s.

- **Improvements**

  - Adds `QueryPolicy.ServerSocketTimeout` and `QueryPolicy.FailOnClusterChange` for when the queries are automatically converted to scans.
  - Minor documentation improvements.
  - Synchronize logging at all situations.
  - Add -debug switch to allow logging at debug level in tests.
  - Allow the user to define the namespace for tests to run on.

- **Fixes**

  - Fix a few go vet errors for Go 1.11.
  - Fixes minor unsigned length conversions for admin command.

## August 29 2018: v1.34.2

Fix release.

- **Fixes**

  - Use pointer receiver for `AerospikeError.SetInDoubt` and `AerospikeError.MarkInDoubt`.
  - Remove unused variable in truncate test.

- **Changes**

  - Add Go 1.11 to Travis' test versions.
  - Use the last error code in MaxRetries timeout errors for Go 1.11.

## August 9 2018: v1.34.1

Hot fix release. We recommend updating to this version asap, especially if you are using the Strong Consistency feature.

- **Fixes**

  - Fixes an issue where a race condition was preventing the partition table to form correctly. (CLIENT-1028)

## July 17 2018: v1.34.0

- **Changes**

  - Removed the LDT code completely.
  - Adds build tag `app_engine` for compatibility with Google's App Engine. Query Aggregate features are not available in this mode due to lua limitations.

- **Improvements**

  - Document how to use AerospikeError type in the code.
  - Allow Task.OnComplete() to be listened to by multiple goroutines. Thanks to [HArmen](https://github.com/alicebob)

- **Fixes**

  - Fixes an issue where `ClientPolicy.FailIfNotConnected` flag was not respected.
  - Fixes a merging issue for PartitionMap, and add a naive validation for partition maps. (CLIENT-1027)

## June 11 2018: v1.33.0

- **New Features**

  - Adds `BatchPolicy.AllowPartialResults` flag to allow the batch command return partial records returned from the cluster.
  - Adds `INVERTED` flag to the `MapReturnType`. Take a look at INVERTED test in `cdt_map_test.go` to see how to use it.
  - Adds a lot of new Ordered Map and List operations and brings the client up to date with the latest server API.

- **Changes**

  - Use the default values for `BasePolicy` in `BatchPolicy` to keep the behavior consistent with the older releases.

- **Improvements**

  - Adds a recover to the tend goroutine to guarantee the client will recover from internal crashes.
  - Removes unneeded type casts.
  - Uses the new stat name for migrations check.

- **Fixes**

  - Fixes TTL in `GetObject` and `BatchGetObject` reflection API.
  - Handle extension marker in List headers.

## March 15 2018: v1.32.0

Major feature release.

- **New Features**

  - Support for _Strong Consistency_ mode in Aerospike Server v4. You will need to set the `policy.LinearizeRead` to `true`. Adds `AerospikeError.InDoubt()` method.
  - Set the resulting `Record.Key` value in Batch Operations to the Original User-Provided Key to preserve the original namespace/set/userValue and avoid memory allocation.

- **Changes**

  - Does not retry on writes by default, and put a 100ms timeout on all transactions by default.
  - Changed some warn logs to debug level.
  - Add missing stats counters to improve statistics reports.
  - Uses sync.Once instead of sync.Mutex for `Connection.close` method.
  - Added `DefaultBufferSize` for initial buffer size for connections.

- **Fixes**

  - Fix the tests for object marshalling to account for monotonic time in Go v1.8+.
  - Stops the ongoing tends on initial connection errors.

## November 29 2017: v1.31.0

Feature release.

- **New Features**

  - Support for newer Batch Protocol. Add `BatchGetComplex` for complex batch queries. Old batch API upgraded to automatically support the new protocol under the hood, unless `BatchPolicy.UseBatchDirect` flag is set to `true`.

- **Changes**

  - Renames ResultCode `NO_XDS` to `ALWAYS_FORBIDDEN`.
  - Makes `SERVER_NOT_AVAILABLE` a client generated error.

## October 12 2017: v1.30.0

Fix and improvements release.

- **Changes**

  - Deprecated LDTs and removed them from the official build.
  - Change supported go versions to 1.7+ due to gopher-lua requiring `Context`.

- **Improvements**

  - Get socket timeout once per command execution, do not redefine err var in command execution loop. PR #211, thanks to [Maxim Krasilnikov](https://github.com/chapsuk)
  - Allow running a UDF on a specific node only.
  - Close cluster only once. PR #208, thanks to [Jun Kimura](https://github.com/bluele)
  - Use actual cluster name in tests instead of assuming `null`.
  - Check for the type of error as well in Duplicate Index Creation.
  - Update description for error code 21. PR #207, thanks to [Maxim Krasilnikov](https://github.com/chapsuk)

## September 5 2017: v1.29.0

Feature and improvements release.

- **New Features**

  - Added `ListIncrementOp` to the CDT list operations.
  - Added `SEQUENCE` to replica policies.

- **Improvements**

  - Tweaked node removal algorithm to cover more corner cases.
  - Make `predExp` interface public. Closes issue #205.
  - Added more stats to the `Client.Stats()`.

## August 17 2017: v1.28.0

Feature, Performance improvements and bug fix release.

- **New Features**

  - Added `Client.Stats()` method to get client's internal statistics.
  - Added `Policy.SocketTimeout` to differentiate between network timeouts and the total transaction timeouts.
  - Support `policy.IncludeBinData` for queries. Only for servers that support this feature.
  - Minor documentation updates.
  - Return key not found exception (instead of returning nil record) for Operate() command where operations include a write.

- **Improvements**

  - Close the tend connection when closing node connections.
  - Added Connection finalizer to make sure all connections are closed eventually.
  - Automatically retry failed info requests on async tasks before returning an error.
  - Updated build instructions for the benchmark tool.
  - Make digest_modulo test deterministic.
  - Relax predexp_modulo test a bit to avoid occasional failures.

- **Fixes**

  - Indirect CAS ops to prevent the compiler from optimizing them out.
  - Return errors instead of nil.

## April 25 2017: v1.27.0

Feature, Performance improvements and bug fix release.

- **New Features**

  - Added `BatchGetObjects` method.
  - Added Exponential Backoff by introducing `BasePolicy.SleepMultiplier`. Only Values > 1.0 are effective. PR #192, thanks to [Venil Noronha](https://github.com/venilnoronha)

- **Improvements**

  - Packer tries to see if it can use generic data types before using reflection.
  - Operations, including CDTs do not allocate a buffer anymore, unless reused.

- **Incompatible changes**:

  - `BinName` and `BinValue` are not exported in `Operation` anymore. These fields shouldn't have been used anyway since `Operation`s used to cache their internal command.

- **Fixes**

  - Documentation Fixes. Thanks to [Nassor Paulino da Silva](https://github.com/nassor) and [HArmen](https://github.com/alicebob)

## April 5 2017: v1.26.0

Feature, Performance improvements and bug fix release.

- **New Features**

  - Predicate API is supported (for server v3.12+)
  - Added `Truncate` method to quickly remove all data from namespaces or sets (for server v3.12+).
  - Support `ScanPolicy.ServerSocketTimeout` (for server v3.12+).
  - Support `ClientPolicy.IgnoreOtherSubnetAliases` to ignore hosts from other subnets. PR #182, thanks to [wedi-dev](https://github.com/wedi-dev)

- **Improvements**

  - Added a lot of predefined generic slice and map types in `NewValue` method to avoid hitting reflection as much as possible.
  - Fix `go vet` complaints.

- **Fixes**

  - Allow streaming commands (scan/query/aggregation) to retry unless the error occurs during parsing of the results. Fixes issue #187
  - Use `net.JoinHostPort` to concatinate host and port values instead of doing it directly. Fixes some issues in IPv6 connection strings.
  - Improved initial Tend run.
  - Fixes `cluster-name` checking bug.

## March 8 2017: v1.25.1

Hot fix release. Updating the client is recommended.

- **Fixes**

  - Fixed an issue where errors in Scan/Query unmarshalling would be duplicated and could cause a deadlock.

## February 28 2017: v1.25.0

Performance improvements and fix release.

- **Improvements**

  - Check tend duration and compare it to tend interval, and warn the user if tend takes longer than tend interval.
  - Seed the cluster concurrently, and return as soon as any of the seeds is validated.
  - Tend the cluster concurrently. Allows use of very big clusters with no delay.
  - Partitions the connection queue to avoid contention.
  - Cluster partition map is merged from all node fragments and updated only once per tend to reduce contention to absolute minimum.

- **Fixes**

  - Fixed an issue where a valid but unreachable seed could timeout and stall connecting and tending the cluster..
  - Fix result code comments.

## January 11 2017: v1.24.0

Minor feature and fix release.

- **New Features**

  - TLS/SSL connections are now officially supported.
  - Added Role/Privilege API.

- **Improvements**

  - Return a client-side error when no ops are passed to the operate command.
  - Export error attribute in `NodeError`
  - Do not attempt to refresh peers if it is not supported by the nodes.

- **Fixes**

  - Use namespace default-ttl for tests instead of assuming 30d
  - Always drain scan connections after parsing the records.
  - Fix panic in GetObject() if all bins in result is nil. PR #172, thanks to [Hamper](https://github.com/hamper)
  - Fix WritePolicy usage with UDF. PR #174, thanks to [Bertrand Paquet](https://github.com/bpaquet)
  - Close connection right when it has an io error and don't wait for the caller.

## December 20 2016 : v1.23.0

Minor feature and fix release.

- **New Features**

  - Exposes the internal `client.Cluster` object to the users.
  - Added New API for high-performance complex data type packing, and removed the old API.

- **Improvements**

  - Only update the partition map if the partition generatio has changed.
  - Use tend connection for user management commands.
  - Marks LargeList as deprecated. Use CDT methods instead.
  - Always validate the message header to avoid reading the remainder of other command buffers.
  - Removes GeoJson from key helper.
  - Improves tend algorthm to allow complete disconnection from the cluster if none of the clusters are accessible.
  - `PutObject` method will now accept objects as well. PR #156, thanks to [Sarath S Pillai](https://github.com/sarathsp06)

- **Fixes**

  - Do not attemp to add a node which were unaccessible to avoid panic.
  - Fix invalid connectionCount. PR #168, thanks to [Jun Kimura](https://github.com/bluele)
  - Fixes minor bug that didn't return the error on reading from the connection during scans.

## November 29 2016 : v1.22.0

Hot fix release. Please upgrade if you have been using other aerospike clients with your database parallel to Go.

- **Fixes**

  - Fixes an issue where short strings in Lists and Maps wouldn't unpack correctly. Resolves #161.

## November 16 2016 : v1.21.0

Minor fix release.

- **New Features**

  - Added new constants for expiration in `WritePolicy`: `TTLServerDefault`, `TTLDontExpire`, `TTLDontUpdate`

- **Improvements**

  - Corrects typos in the code. PR #142, thanks to [Muyiwa Olurin ](https://github.com/muyiwaolurin)
  - Use the tend connection for `RequestInfo` commands.

- **Fixes**

  - Fixes an issue where TTL values were calcualted wrongly when they were set not to expire.
  - Fixes an issue where `PutObjects` would marshal `[]byte` to `List` in database. PR #152, thanks to [blide](https://github.com/blide)
  - Fixes an issue where `Recordset` could leak goroutines. PR #153, thanks to [Deepak Prabhakara](https://github.com/deepakprabhakara)

## October 25 2016 : v1.20.0

Major improvements release. There has been major changes in the library. Please test rigorously before upgrading to the new version.

- **New Features**

  - Let user define the desired tag for bin names in structs using `SetAerospikeTag` function.
  - Added `as_performance` build tag to avoid including the slow convenience API which uses reflections in the client code.
    To use this feature, you should include -tags="as_performance" when building your project.

    _NOTICE_: Keep in mind that your code may not compile using this flag. That is by design.

- **Improvements**

  - Added special packer for map[string]interface{} in `NewValue` method.
  - Avoid allocating memory for Map and List values.
  - Allocate commands on the stack to avoid heap allcations.
  - Avoid allocating memory for `packer`.
  - Avoid Allocating memory in computeHash for keys.
  - Avoid allocating memory in Ripe160MD digest.
  - Removed BufferPool and moved buffers to `Connection` objects to remove lock contention.
  - Added `ListIter` and `MapIter` interfaces to support passing Maps and Lists to the client without using reflection.

## October 14 2016 : v1.19.0

Major feature and improvement release.

- **New Features**

  - Support TLS secured connections. (Feature will be supported in coming server releases.)

  - Support IPv6 protocol. Supported by Aerospike Server 3.10+.

  - Support `cluster-name` verification. Supported by Aerospike Server 3.10+.

  - Support new peers info protocol. Supported by Aerospike Server 3.10+.

- **Improvements**

  - Will retry the operation even when reading from the buffer. Set `Policy.MaxRetries = 0` to avoid this behavior. PR #143, thanks to [Hector Jusforgues](https://github.com/hectorj)

  - Much improved cluster management algorithm. Will now handle the case where multiple nodes go down simultaneously, still protecting against split brain rogue nodes.

- **Fixes**

  - Try all alias IPs in node validator. Resolves #144.

  - Updated job status check for execute tasks.

## August 19 2016 : v1.18.0

Minor improvements release.

- **New Features**

  - Support 'Durable Deletes' for the next version of Aerospike Server Enterprise.

- **Improvements**

  - Don't run tests for features that are not supported by the server.

  - Added new server error codes.

## July 27 2016 : v1.17.1

Minor improvements release.

- **Improvements**

  - Add `TaskId()` method for `Recordset`.

  - Cleanup indexes after test cases.

  - Keep connections on recoverable server errors.

  - Return the error on unexpected keys in `BatchCommandGet/Header`.

  - Use the same client object in tests and support using replicas on travis.

## July 19 2016 : v1.17.0

Major feature and improvement release.

- **New Features**

  - Client now supports distributing reads from Replicas using `ClientPolicy.RequestProleReplicas` and `Policy.ReplicaPolicy`

- **Improvements**

  - `Cluster.GetConnection` will now retry to acquire a connection until timeout.

  - `Client.DropIndex` method now blocks until all nodes report the index is dropped.

  - Async tasks like `CreateIndex` will retry a few times before deciding a non-existing job means it has finished.

  - Don't use math.MaxInt64, it breaks 32-bit builds. PR #139, thanks to [Cameron Sparr](https://github.com/sparrc)

- **Fixes**

  - Maps with 0 elements will automatically shortcut to unordered empty maps.

  - Return the error in BatchCommandGet on parse error.

## June 28 2016 : v1.16.3

Major bugfix release. Update recommended.

- **Improvements**

  - Skip LDT tests if LDT is not enabled.

  - Returns last error after all retry attempts to run a command are exhausted.

  - Reserves a connection for tend operation to avoid dropping a node when high load prevents acquiring a proper connection.

  - Added Finalizers to `Client` and `Recordset`. Both will be automatically closed by the GC.

- **Fixes**

  - Fixes an issue where `services-alternate` wasn't used in `Node.addFriends()` when instructed so in the policy.

  - Fixes an issue where object metadata wasn't cached if `QueryObjects` was called before `PutObject`.

  - Fixes an issue where idle connections were not dropped.

  - Fixes an issue where requested buffer sizes were not guarded against negative numbers.

## June 7 2016 : v1.16.2

Minor bugfix release.

- **Fixes**

  - Fixes an issue where empty unordered maps were confused with CDT maps.

## June 6 2016 : v1.16.1

Minor bugfix release.

- **Fixes**

  - Fixes an issue where complex maps and lists weren't unmarshalled correctly in `GetObject` method.

## June 2 2016 : v1.16

Major feature and improvements release.

> NOTICE: Due to the relatively extensive code overhaul, upgrade with caution.

- **New Features**

  - Added CDT Ordered Map API. (Requires server v3.8.3+)

- **Improvements**

  - Removed mutexes from `Cluster` and `Node` code.

  - Improved code quality using various linters.

## May 27 2016 : v1.15

Minor fixes and improvements release.

- **Fixes**

  - Fixed an issue where unmarshalling embedded structs and pointers didn't work properly if they were tagged.

## May 16 2016 : v1.14

Minor fixes and improvements release.

- **Fixes**

  - Fixed an issue in which go-routines were leaked in `Results()` method of `Recordset` on cancellation. Based on PR #128, thanks to [Noel Cower](https://github.com/nilium)

  - Fixed issues regarding leaked goroutines in `Cluster.WaitTillStablized()`, `Cluster.MigrationInProgress()`, and `Cluster.WaitUntillMigrationIsFinished()` methods. PR #126, thanks to [Anton](https://github.com/yiiton)

- **Improvements**

  - Improved cluster `tend()` logic.

  - Added `Recordset.Read()` method.

  - Minor fixes in docs and code formatting. Thanks to [Andrew Murray](https://github.com/radarhere) and [Erik Dubbelboer](https://github.com/erikdubbelboer)

## April 1 2016 : v1.13

Minor features and improvements release.

- **New Features**

  - Added `NewGeoWithinRegionForCollectionFilter`, `NewGeoRegionsContainingPointForCollectionFilter`, `NewGeoWithinRadiusForCollectionFilter` for queries on collection bins.

- **Fixes**

  - Fixed an issue in which bounded byte arrays were silently being dropped as map keys.

- **Improvements**

  - Removed and fixed unused assignments and variables.

  - Fixed typos in the comments.

  - Minor changes and formatting. PR #124, thanks to [Harmen](https://github.com/alicebob)

## March 8 2016 : v1.12

Minor features and improvements release.

- **New Features**

  - Support Metadata in struct tags to fetch TTL and Generation via `GetObject`.
    Notice: Metadata attributes in an struct are considered transient, and won't be persisted.

  Example:

  ```go
  type SomeStruct struct {
    TTL  uint32         `asm:"ttl"` // record time-to-live in seconds
    Gen  uint32         `asm:"gen"` // record generation
    A    int
    Self *SomeStruct
  }

  key, _ := as.NewKey("ns", "set", value)
  err := client.PutObject(nil, key, obj)
  // handle error here

  rObj := &OtherStruct{}
  err = client.GetObject(nil, key, rObj)
  ```

  - GeoJSON support in Lists and Maps

- **Improvements**

  - Use `ClientPolicy.timeout` for connection timeout when refreshing nodes

  - Added new server error codes

  - Protect RNG pool against low-precision clocks during init

  - Better error message distingushing between timeout because of reaching deadline and exceeding maximum retries

- **Fixes**

  - Fixed object mapping cache for anonymous structs. PR #115, thanks to [Moshe Revah](https://github.com/zippoxer)

  - Fixed an issue where `Execute()` method wasn't observing the `SendKey` flag in Policy.

## February 9 2016 : v1.11

Minor features and improvements release.

- **New Features**

  - Can now use `services-alternate` for cluster tend.

  - New CDT List API: `ListGetRangeFromOp`, `ListRemoveRangeFromOp`, `ListPopRangeFromOp`

- **Improvements**

  - Improves marshalling of data types into and out of the Lua library and avoids marshalling values before they are needed.

  - Returns error for having more than one Filter on client-side to avoid confusion.

  - Increases default `ClientPolicy.Timeout` and return a meaningful error message when the client is not fully connected to the cluster after `waitTillStabilized` call

## January 13 2016 : v1.10

Major release. Adds Aggregation.

- **New Features**

  - Added `client.QueryAggregate` method.

    - For examples regarding how to use this feature, look at the examples directory.

    - You can find more documentation regarding the [Aggregation Feature on Aerospike Website](http://www.aerospike.com/docs/guide/aggregation.html)

- **Improvements**

  - Improve Query/Scan performance by reading from the socket in bigger chunks

## December 14 2015 : v1.9

Major release. Adds new features.

- **New Features**

  - Added CDT List operations.

  - Added `NewGeoWithinRadiusFilter` filter for queries.

- **Changes**

  - Renamed `NewGeoPointsWithinRegionFilter` to `NewGeoWithinRegionFilter`

## December 1 2015 : v1.8

Major release. Adds new features and fixes important bugs.

- **New Features**

  - Added `ScanAllObjects`, `ScanNodeObjects`, `QueryObjects` and `QueryNodeObjects` to the client, to facilitate automatic unmarshalling of data similar to `GetObject`.

    - NOTICE: This feature and its API are experimental, and may change in the future. Please test your code throughly, and provide feedback via Github.

  - Added `ScanPolicy.IncludeLDT` option (Usable with yet to be released server v 3.7.0)

  - Added `LargeList.Exist` method.

- **Improvements**

  - Makes Generation and Expiration values consistent for WritePolicy and Record.

    - NOTICE! BREAKING CHANGE: Types of `Record.Generation` and `Record.Expiration`, and also `WritePolicy.Generation` and `WritePolicy.Expiration` have changed, and may require casting in older code.

  - Refactor tools/asinfo to be more idiomatic Go. PR #86, thanks to [Tyler Gibbons](https://github.com/Kavec)

  - Many documentation fixes thanks to [Charl Matthee](https://github.com/charl) and [Tyler Gibbons](https://github.com/Kavec)

- **Fixes**

  - Changed the `KeepConnection` logic from black-list to white-list, to drop all

  - Fix RemoveNodesCopy logic error.

  - Add missing send on recordset Error channel. PR #99, thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

  - Fix skipping of errors/records in (\*recordset).Results() select after cancellation. PR #99, thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

## October 16 2015 : v1.7

Major release. Adds new features and fixes important bugs.

- **New Features**

  - Added support for Geo spatial queries.

  - Added support for creating indexes on List and Map bins, and querying them.

  - Added support for native floating point values.

  - Added `ClientPolicy.IpMap` to use IP translation for alias recognition. PR #81, Thanks to [Christopher Guiney](https://github.com/chrisguiney)

- **Improvements**

  - Cosmetic change to improve code consistency for `PackLong` in `packer.go`. PR #78, Thanks to [Erik Dubbelboer](https://github.com/ErikDubbelboer)

- **Fixes**

  - Fixes an issue when the info->services string was malformed and caused the client to panic.

  - Fixes an issue with unmarshalling maps of type map[ANY]struct{} into embedded structs.

  - Fixes issue with unmarshalling maps of type map[ANY]struct{} into embedded structs.

  - Fixes an issue with bound checking. PR #85, Thanks to [Tait Clarridge](https://github.com/oldmantaiter)

  - Fixes aa few typos in the docs. PR #76, Thanks to [Charl Matthee](https://github.com/charl)

## August 2015 : v1.6.5

Minor maintenance release.

- **Improvements**

  - Export `MaxBufferSize` to allow tweaking of maximum buffer size allowed to read a record. If a record is bigger than this size (e.g: A lot of LDT elements in scan), this setting wil allow to tweak the buffer size.

## July 16 2015 : v1.6.4

Hot fix release.

- **Fixes**

  - Fix panic when a scan/query fails and the connection is not dropped.

## July 9 2015 : v1.6.3

Minor fix release.

- **Improvements**

  - Improved documentation. PR #64 and #68. Thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

- **Fixes**

  - Fix a bunch of golint notices. PR #69, Thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

  - Connection.Read() total bytes count on error. PR #71, Thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

  - Fixed a race condition on objectMappings map. PR #72, Thanks to [Geert-Johan Riemer](https://github.com/GeertJohan)

  - Fixed a few uint -> int convertions.

## June 11 2015 : v1.6.2

Minor fix release.

- **Improvements**

  - Improved documentation. Replaced all old API references regarding Recordset/Query/Scan to newer, more elegant API.

- **Fixes**

  - Fixed an issue where erroring out on Scan would result a panic.

  - Fixed an issue where `Statement.TaskId` would be negative. converted `Statement.TaskId` to `uint64`

## June 9 2015 : v1.6.1

Minor fix release.

- **Fixes**

  - Fixed an issue where marshaller wouldn't marshal some embedded structs.

  - Fixed an issue where querying/scanning empty sets wouldn't drain the socket before return.

## May 30 2015 : v1.6.0

There's an important performance regression bug fix in this release. We recommend everyone to upgrade.

- **New Features**

  - Added New LargeList API.

    - NOTICE! BREAKING CHANGE: New LargeList API on the Go Client uses the New API defined on newer server versions. As Such, it has changed some signatures in LargeList.

- **Fixes**

  - Fixed an issue where connections where not put back to the pool on some non-critical errors.

  - Fixed an issue where Object Unmarshaller wouldn't extend a slice.

  - Decode RegisterUDF() error message from base64

  - Fixed invalid connection handling on node connections (thanks to @rndive)

## May 15 2015 : v1.5.2

Hotfix release.

- **Fixes**

  - Fixed a branch-merge mistake regarding error handling during connection authentication.

## May 15 2015 : v1.5.1

Major maintenance release.

NOTICE: All LDTs on server other than LLIST have been deprecated, and will be removed in the future. As Such, all API regarding those features are considered deprecated and will be removed in tandem.

- **Improvements**

  - Introduces `ClientPolicy.IdleTimeout` to close stale connections to the server. Thanks to Mário Freitas (@imkira). PR #57

  - Use type alias instead of struct for NullValue.

  - Removed workaround regarding filtering bin names on the client for `BatchGet`. Issue #60

- **Fixes**

  - Fixed a few race conditions.

  - Fixed #58 regarding race condition accessing `Cluster.password`.

  - Fixed minor bugs regarding handling of nulls in structs for `GetObj()` and `PutObj()`.

  - Fixed a bug regarding setting TaskIds on the client.

- ** Other Changes **

  - Removed deprecated `ReplaceRoles()` method.

  - Removed deprecated `SetCapacity()` and `GetCapacity()` methods for LDTs.

## April 13 2015 : v1.5.0

This release includes potential BREAKING CHANGES.

- **New Features**

  - Introduces `ClientPolicy.LimitConnectionsToQueueSize`. If set to true, the client won't attemp to create new connections to the node if the total number of pooled connections to the node is equal or more than the pool size. The client will retry to poll a connection from the queue until a timeout occurs. If no timeout is set, it will only retry for ten times.

- **Improvements**

  - BREAKING CHANGE: |
    Uses type aliases instead of structs in several XXXValue methods. This removes a memory allocation per `Value` usage.
    Since every `Put` operation uses at list one value object, this has the potential to improve application performance.
    Since the signature of several `NewXXXValue` methods have changed, this might break some existing code if you have used the value objects directly.

  - Improved `Logger` so that it will accept a generalized `Logger` interface. Any Logger with a `Printf(format string, values ...interface{})` method can be used. Examples include Logrus.

  - Improved `Client.BatchGet()` performance.

- **Fixes**

  - Bin names were ignored in BatchCommands.

  - `BatchCommandGet.parseRecord()` returned wrong values when `BinNames` was empty but not nil.

## March 31 2015 : v1.4.2

Maintenance release.

- **Improvements**

  - Replace channel-based queue system with a lock-based algorithm.
  - Marshaller now supports arrays of arbitrary types.
  - `Client.GetObject()` now returns an error when the object is not found.
  - Partition calculation uses a trick that is twice as fast.

- **Improvements**

  - Unpacking BLOBs resulted in returning references to pooled buffers. Now copies are returned.

## March 12 2015 : v1.4.1

This is a minor release to help improve the compatibility of the client on Mac OS, and to make cross compilation easier.

- **Improvements**

  - Node validator won't call net.HostLookup if an IP is passed as a seed to it.

## Feb 17 2015 : v1.4.0

This is a major release, and makes using the client much easier to develop applications.

- **New Features**

  - Added Marshalling Support for Put and Get operations. Refer to [Marshalling Test](client_object_test.go) to see how to take advantage.
    Same functionality for other APIs will follow soon.
    Example:

  ```go
  type SomeStruct struct {
    A    int            `as:"a"`  // alias the field to a
    Self *SomeStruct    `as:"-"`  // will not persist the field
  }

  type OtherStruct struct {
    i interface{}
    OtherObject *OtherStruct
  }

  obj := &OtherStruct {
    i: 15,
    OtherObject: OtherStruct {A: 18},
  }

  key, _ := as.NewKey("ns", "set", value)
  err := client.PutObject(nil, key, obj)
  // handle error here

  rObj := &OtherStruct{}
  err = client.GetObject(nil, key, rObj)
  ```

  - Added `Recordset.Results()`. Consumers of a recordset do not have to implement a select anymore. Instead of:

  ```go
  recordset, err := client.ScanAll(...)
  L:
  for {
    select {
    case r := <-recordset.Record:
      if r == nil {
        break L
      }
      // process record here
    case e := <-recordset.Errors:
      // handle error here
    }
  }
  ```

  one should only range on `recordset.Results()`:

  ```go
  recordset, err := client.ScanAll(...)
  for res := range recordset.Results() {
    if res.Err != nil {
      // handle error here
    } else {
      // process record here
      fmt.Println(res.Record.Bins)
    }
  }
  ```

  Use of the old pattern is discouraged and deprecated, and direct access to recordset.Records and recordset.Errors will be removed in a future release.

- **Improvements**

  - Custom Types are now allowed as bin values.

## Jan 26 2015 : v1.3.1

- **Improvements**

  - Removed dependency on `unsafe` package.

## Jan 20 2015 : v1.3.0

- **Breaking Changes**

  - Removed `Record.Duplicates` and `GenerationPolicy/DUPLICATE`

- **New Features**

  - Added Security Features: Please consult [Security Docs](https://www.aerospike.com/docs/guide/security.html) on Aerospike website.

    - `ClientPolicy.User`, `ClientPolicy.Password`
    - `Client.CreateUser()`, `Client.DropUser()`, `Client.ChangePassword()`
    - `Client.GrantRoles()`, `Client.RevokeRoles()`, `Client.ReplaceRoles()`
    - `Client.QueryUser()`, `Client.QueryUsers`

  - Added `Client.QueryNode()`

  - Added `ClientPolicy.TendInterval`

- **Improvements**

  - Cleaned up Scan/Query/Recordset concurrent code

- **Fixes**

  - Fixed a bug in `tools/cli/cli.go`.

  - Fixed a bug when `GetHeaderOp()` would always translate into `GetOp()`

## Dec 29 2014: v1.2.0

- **New Features**

  - Added `NewKeyWithDigest()` method. You can now create keys with custom digests, or only using digests without
    knowing the original value. (Useful when you are getting back results with Query and Scan)

## Dec 22 2014

- **New Features**

  - Added `ConsistencyLevel` to `BasePolicy`.

  - Added `CommitLevel` to `WritePolicy`.

  - Added `LargeList.Range` and `LargeList.RangeThenFilter` methods.

  - Added `LargeMap.Exists` method.

- **Improvements**

  - We use a pooled XORShift RNG to produce random numbers in the client. It is FAST.

## Dec 19 2014

- **Fixes**

  - `Record.Expiration` wasn't converted to TTL values on `Client.BatchGet`, `Client.Scan` and `Client.Query`.

## Dec 10 2014

- **Fixes**:

  - Fixed issue when the size of key field would not be estimated correctly when WritePolicy.SendKey was set.

## Nov 27 2014

Major Performance Enhancements. Minor new features and fixes.

- **Improvements**

  - Go client is much faster and more memory efficient now.
    In some workloads, it competes and wins against C and Java clients.

  - Complex objects are now de/serialized much faster.

- **New Features**

  - Added Default Policies for Client object.
    Instead of creating a new policy when the passed policy is nil, default policies will be used.

## Nov 24 2014

- **Fixes**:

  - Fixed issue when WritePolicy.SendKey = true was not respected in Touch() and Operate()

## Nov 22 2014

Hotfix in unpacker. Update strongly recommended for everyone using Complex objects, LDTs and UDFs.

- **Fixes**:

  - When Blob, ByteArray or String size has a bit sign set, unpacker reads it wrong.
    Note: This bug only affects unpacking of these objects. Packing was unaffected, and data in the database is valid.

## Nov 2 2014

Minor, but very impoortant fix.

- **Fixes**:

  - Node selection in partition map was flawed on first refresh.

- **Incompatible changes**:

  - `Expiration` and `Generation` in `WritePolicy` are now `int32`
  - `TaskId` in `Statement` is now always set in the client, and is `int64`

- **New Features**:

  - float32, float64 and bool are now supported in map and array types

## Oct 15 2014 (Beta 2)

- **Hot fix**:

  - Fixed pack/unpack for uint64

## Aug 20 2014 (Beta 1)

Major changes and improvements.

- **New Features**:

  - Added client.Query()
  - Added client.ScanNode()/All()
  - Added client.Operate()
  - Added client.CreateIndex()
  - Added client.DropIndex()
  - Added client.RegisterUDF()
  - Added client.RegisterUDFFromFile()
  - Added client.Execute()
  - Added client.ExecuteUDF()
  - Added client.BatchGet()
  - Added client.BatchGetHeader()
  - Added client.BatchExists()
  - Added LDT implementation
  - Added `Node` and `Key` references to the Record

- **Changes**:

  - Many minor and major bug fixes
  - Potentially breaking change: Reduced Undocumented API surface
  - Fixed a few places where error results were not checked
  - Breaking Change: Convert Key.namespace & Key.setName from pointer to string; affects Key API
  - Renamed all `this` receivers to appropriate names
  - Major performance improvements (~2X improvements in speed and memory consumption):

    - better memory management for commands; won't allocate if capacity is big enough
    - better hash management in key; avoids two redundant memory allocs
    - use a buffer pool to reduce GC load
    - fine-grained, customizable and deterministic buffer pool implementation for command

  - Optimizations for Key & Digest

    - changed digest implementation, removed an allocation
    - Added RIPEMD160 hash files from crypto to lib
    - pool hash objects

  - Various Benchmark tool improvements
    - now profileable using localhost:6060
    - minor bug fixes

## Jul 26 2014 (Alpha)

- Initial Release.

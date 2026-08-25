# Client Class

The `Client` class provides operations which can be performed on an Aerospike
database cluster. In order to get an instance of the Client class, you need
to call `NewClient()`:

```go
  import as "github.com/aerospike/aerospike-client-go/v8"

  client, err := as.NewClient("127.0.0.1", 3000)
```

To customize a Client with a ClientPolicy:

```go
  clientPolicy := as.NewClientPolicy()
  clientPolicy.ConnectionQueueSize = 64
  clientPolicy.LimitConnectionsToQueueSize = true
  clientPolicy.Timeout = 50 * time.Millisecond

  client, err := as.NewClientWithPolicy(clientPolicy, "127.0.0.1", 3000)
```

*Notice*: Examples in this section assume `import as "github.com/aerospike/aerospike-client-go/v8"`. They omit some error checks for brevity. Always check errors in production.

Public client methods return the client's `Error` interface, not the built-in `error` type. `Error` is compatible with `errors.Is` and `errors.As`. See [pkg.go.dev](https://pkg.go.dev/github.com/aerospike/aerospike-client-go/v8) for the full API.

With a new client, you can use any of the methods specified below. You need only *ONE* client object. This object is goroutine-friendly, and pools its resources internally.

- [Methods](#methods)
  - [Add()](#add)
  - [Append()](#append)
  - [Close()](#close)
  - [Delete()](#delete)
  - [Exists()](#exists)
  - [BatchExists()](#batchexists)
  - [Get()](#get)
  - [GetHeader()](#getheader)
  - [BatchGet()](#batchget)
  - [BatchGetHeader()](#batchgetheader)
  - [IsConnected()](#isConnected)
  - [Operate()](#operate)
  - [Prepend()](#prepend)
  - [Put()](#put)
  - [PutBins()](#putbins)
  - [Touch()](#touch)
  - [ScanAll()](#scanall)
  - [ScanNode()](#scannode)
  - [CreateIndex()](#createindex)
  - [DropIndex()](#dropindex)
  - [RegisterUDF()](#registerudf)
  - [RegisterUDFFromFile()](#registerudffromfile)
  - [Execute()](#execute)
  - [ExecuteUDF()](#executeudf)
  - [Query()](#query)
  - [BatchOperate()](#batchoperate)
  - [Filter expressions](#filter-expressions)
  - [Multi-record transactions](#multi-record-transactions)
  - [Object API](#object-api)
  - [CDT operations](#cdt-operations)


<a name="methods"></a>
## Methods

<!--
################################################################################
add()
################################################################################
-->
<a name="add"></a>

### Add(policy *WritePolicy, key *Key, bins BinMap) Error

Using the provided key, adds values to the mentioned bins.
Bin value types should be of type `integer` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  bins := as.BinMap {
    "e": 2,
    "pi": 3,
  }

  err = client.Add(nil, key, bins)
```

<!--
################################################################################
append()
################################################################################
-->
<a name="append"></a>

### Append(policy *WritePolicy, key *Key, bins BinMap) Error

Using the provided key, appends provided values to the mentioned bins.
Bin value types should be of type `string` or `[]byte` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  bins := as.BinMap {
    "story": ", and lived happily ever after...",
  }

  err = client.Append(nil, key, bins)
```

<!--
################################################################################
close()
################################################################################
-->
<a name="close"></a>

### Close()

Closes the client connection to the cluster.

Example:
```go
  client.Close()
```

<!--
################################################################################
remove()
################################################################################
-->
<a name="delete"></a>

### Delete(policy *WritePolicy, key *Key) (bool, Error)

Removes a record with the specified key from the database cluster.

Parameters:

- `policy`      – (optional) The [delete Policy object](policies.md#RemovePolicy) to use for this operation.
- `key`         – A [Key object](datamodel.md#key) used for locating the record to be removed.

returned values:

- `existed`         – Boolean value that indicates if the Key existed.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  if existed, err := client.Delete(nil, key); existed {
    // do something
  }
```

<!--
################################################################################
exists()
################################################################################
-->
<a name="exists"></a>

### Exists(policy *BasePolicy, key *Key) (bool, Error)

Using the key provided, checks for the existence of a record in the database cluster .

Parameters:

- `policy`      – (optional) The [BasePolicy object](policies.md#BasePolicy) to use for this operation.
                  Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.

Example:

```go
  key, err := as.NewKey("test", "demo", 123)
  if err != nil {
    panic(err)
  }

  exists, err := client.Exists(nil, key)
  if err != nil {
    panic(err)
  }
  if exists {
    // do something
  }
```

<!--
################################################################################
batchexists()
################################################################################
-->
<a name="batchexists"></a>

### BatchExists(policy *BatchPolicy, keys []*Key) ([]bool, Error)

Using the keys provided, checks for the existence of records in the database cluster in one request.

Parameters:

- `policy`      – (optional) The [BatchPolicy object](policies.md#BatchPolicy) to use for this operation.
                  Pass `nil` for default values.
- `keys`         – A [Key array](datamodel.md#key), used to locate the records in the cluster.

Example:

```go
  key1, err := as.NewKey("test", "demo", 123)
  key2, err := as.NewKey("test", "demo", 42)

  existenceArray, err := client.BatchExists(nil, []*as.Key{key1, key2})
  if err != nil {
    panic(err)
  }
  _ = existenceArray
```

<!--
################################################################################
get()
################################################################################
-->
<a name="get"></a>

### Get(policy *BasePolicy, key *Key, bins ...string) (*Record, Error)

Using the key provided, reads a record from the database cluster .

Parameters:

- `policy`      – (optional) The [BasePolicy object](policies.md#BasePolicy) to use for this operation.
                  Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – (optional) Bins to retrieve. Will retrieve all bins if not provided.

Example:

```go
  key, err := as.NewKey("test", "demo", 123)

  rec, err := client.Get(nil, key) // reads all the bins
```

<!--
################################################################################
getheader()
################################################################################
-->
<a name="getheader"></a>

### GetHeader(policy *BasePolicy, key *Key) (*Record, Error)

Using the key provided, reads *ONLY* record metadata from the database cluster. Record metadata includes record generation and Expiration (TTL from the moment of retrieval, in seconds)

```record.Bins``` will always be empty in resulting ```record```.

Parameters:

- `policy`      – (optional) The [BasePolicy object](policies.md#BasePolicy) to use for this operation.
                  Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.

Example:

```go
  key, err := as.NewKey("test", "demo", 123)

  rec, err := client.GetHeader(nil, key) // No bins will be retrieved
```

<!--
################################################################################
batchget()
################################################################################
-->
<a name="batchget"></a>

### BatchGet(policy *BatchPolicy, keys []*Key, bins ...string) ([]*Record, Error)

Using the keys provided, reads all relevant records from the database cluster in a single request.

Parameters:

- `policy`      – (optional) The [BatchPolicy object](policies.md#BatchPolicy) to use for this operation.
                  Pass `nil` for default values.
- `keys`         – A [Key array](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – (optional) Bins to retrieve. Will retrieve all bins if not provided.

Example:

```go
  key1, err := as.NewKey("test", "demo", 123)
  key2, err := as.NewKey("test", "demo", 42)

  recs, err := client.BatchGet(nil, []*as.Key{key1, key2}) // reads all the bins
```

<!--
################################################################################
batchgetheader()
################################################################################
-->
<a name="batchgetheader"></a>

### BatchGetHeader(policy *BatchPolicy, keys []*Key) ([]*Record, Error)

Using the keys provided, reads all relevant record metadata from the database cluster in a single request.

```record.Bins``` will always be empty in resulting ```record```.

Parameters:

- `policy`      – (optional) The [BatchPolicy object](policies.md#BatchPolicy) to use for this operation.
                  Pass `nil` for default values.
- `keys`         – A [Key array](datamodel.md#key), used to locate the record in the cluster.

Example:

```go
  key1, err := as.NewKey("test", "demo", 123)
  key2, err := as.NewKey("test", "demo", 42)

  recs, err := client.BatchGetHeader(nil, []*as.Key{key1, key2})
```
<!--
################################################################################
idConnected()
################################################################################
-->
<a name="isConnected"></a>

### IsConnected() bool

Checks if the client is connected to the cluster.

<!--
################################################################################
operate()
################################################################################
-->
<a name="operate"></a>

### Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, Error)

Performs multiple read and write operations on a single key in one request. For example, you can increment a counter and read the result in the same call.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `operations`  – One or more operations such as `PutOp`, `AddOp`, `GetOp`, or CDT operations.

Example:

```go
  key, err := as.NewKey("test", "demo", 123)
  if err != nil {
    panic(err)
  }

  record, err := client.Operate(nil, key,
    as.AddOp(as.NewBin("counter", 1)),
    as.GetOp(),
  )
```

List and Map operations are documented in [CDT Operations](cdt_operations.md).

<!--
################################################################################
prepend()
################################################################################
-->
<a name="prepend"></a>

### Prepend(policy *WritePolicy, key *Key, bins BinMap) Error

Using the provided key, prepends provided values to the mentioned bins.
Bin value types should be of type `string` or `[]byte` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  bins := as.BinMap {
    "story": "Long ago, in a galaxy far far away, ",
  }

  err = client.Prepend(nil, key, bins)
```

<!--
################################################################################
put()
################################################################################
-->
<a name="put"></a>

### Put(policy *WritePolicy, key *Key, bins BinMap) Error

Writes a record to the database cluster. If the record exists, it modifies the record with bins provided.
To remove a bin, set its value to `nil`.

#### Node: Under the hood, Put converts BinMap to []Bins and uses ```PutBins```. Use PutBins to avoid unnecessary memory allocation and iteration.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap map](datamodel.md#binmap) used for specifying the fields to store.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  bins := as.BinMap {
    "a": "Lack of skill dictates economy of style.",
    "b": 123,
    "c": []int{1, 2, 3},
    "d": map[string]any{"a": 42, "b": "An elephant is mouse with an operating system."},
  }

  err = client.Put(nil, key, bins)
```

<!--
################################################################################
putbins()
################################################################################
-->
<a name="putbins"></a>

### PutBins(policy *WritePolicy, key *Key, bins ...*Bin) Error

Writes a record to the database cluster. If the record exists, it modifies the record with bins provided.
To remove a bin, set its value to `nil`.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [Bin array](datamodel.md#bin) used for specifying the fields to store.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  bin1 := as.NewBin("a", "Lack of skill dictates economy of style.")
  bin2 := as.NewBin("b", 123)
  bin3 := as.NewBin("c", []int{1, 2, 3})
  bin4 := as.NewBin("d", map[string]any{"a": 42, "b": "An elephant is mouse with an operating system."})

  err = client.PutBins(nil, key, bin1, bin2, bin3, bin4)
```

<!--
################################################################################
touch()
################################################################################
-->
<a name="touch"></a>

### Touch(policy *WritePolicy, key *Key) Error

Create record if it does not already exist.
If the record exists, the record's time to expiration will be reset to the policy's expiration.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.

Example:
```go
  key, err := as.NewKey("test", "demo", 123)

  err = client.Touch(as.NewWritePolicy(0, 5), key)
```

<!--
################################################################################
scanall()
################################################################################
-->
<a name="scanall"></a>

### ScanAll(policy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, Error)

Performs a full Scan on all nodes in the cluster, and returns the results in a [Recordset object](datamodel.md#recordset)


Parameters:

- `policy`      – (optional) A [Scan Policy object](policies.md#ScanPolicy) to use for this operation.
                Pass `nil` for default values.
- `namespace`         – Namespace to perform the scan on.
- `setName`         – Name of the Set to perform the scan on.
- `binNames`         – Name of bins to retrieve. If not passed, all bins will be retrieved.

Refer to [Recordset object](datamodel.md#recordset) documentation for details on how to retrieve the data.

Example:
```go
  // scan the whole cluster
  recordset, err := client.ScanAll(nil, "test", "demo")

  for res := range recordset.Results() {
    if res.Err != nil {
      // handle error; or close the recordset and break
    }
    
  // process record
  fmt.Println(res.Record)
  }
```

<!--
################################################################################
scannode()
################################################################################
-->
<a name="scannode"></a>

### ScanNode(policy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error)

Performs a full Scan *on a specific node* in the cluster, and returns the results in a [Recordset object](datamodel.md#recordset)

It works the same as ScanAll() method.

<!--
################################################################################
createindex()
################################################################################
-->
<a name="createindex"></a>

### CreateIndex(policy *WritePolicy, namespace string, setName string, indexName string, binName string, indexType IndexType) (*IndexTask, Error)

Creates a secondary index. IndexTask will return a IndexTask object which can be used to determine if the operation is completed.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `namespace`         – Namespace
- `setName`         – Name of the Set
- `indexName`         – Name of index
- `binName`         – Bin name to create the index on
- `indexType`         – `STRING`, `NUMERIC`, `GEO2DSPHERE`, or `BLOB` (BLOB requires server 7.0+)

Example:

```go
  idxTask, err := client.CreateIndex(nil, "test", "demo", "indexName", "binName", as.NUMERIC)
  panicOnErr(err)

  // wait until index is created.
  // OnComplete() channel will return nil on success and an error on errors
  err = <- idxTask.OnComplete()
  if err != nil {
    panic(err)
  }
```

<!--
################################################################################
dropindex()
################################################################################
-->
<a name="dropindex"></a>
### DropIndex(  policy *WritePolicy,  namespace string,  setName string,  indexName string) Error

Drops an index.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `namespace`         – Namespace
- `setName`           – Name of the Set.
- `indexName`         – Name of index

```go
  err := client.DropIndex(nil, "test", "demo", "indexName")
```

<!--
################################################################################
registerudf()
################################################################################
-->
<a name="registerudf"></a>

### RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, Error)

Registers the given UDF on the server.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `udfBody`     – UDF source code
- `serverPath`  – Path on which the UDF should be put on the server-side
- `language`    – Only 'LUA' is currently supported


Example:

```go
  const udfBody = `function testFunc1(rec)
     local ret = map()                     -- Initialize the return value (a map)

     local x = rec['bin1']               -- Get the value from record bin named "bin1"

     rec['bin2'] = (x / 2)               -- Set the value in record bin named "bin2"

     aerospike:update(rec)                -- Update the main record

     ret['status'] = 'OK'                   -- Populate the return status
     return ret                             -- Return the Return value and/or status
  end`

  regTask, err := client.RegisterUDF(nil, []byte(udfBody), "udf1.lua", as.LUA)
  panicOnErr(err)

  // wait until UDF is created
  err = <-regTask.OnComplete()
  if err != nil {
    panic(err)
  }
```

<!--
################################################################################
registerudffromfile()
################################################################################
-->
<a name="registerudffromfile"></a>

### RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, Error)

Read the UDF source code from a file and registers it on the server.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `clientPath`  – full file path for UDF source code
- `serverPath`  – Path on which the UDF should be put on the server-side
- `language`    – Only 'LUA' is currently supported


Example:

```go
  regTask, err := client.RegisterUDFFromFile(nil, "/path/udf.lua", "udf1.lua", as.LUA)
  panicOnErr(err)

  // wait until UDF is created
  err = <- regTask.OnComplete()
  if err != nil {
    panic(err)
  }
```

<!--
################################################################################
execute()
################################################################################
-->
<a name="execute"></a>

### Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (any, Error)

Executes a UDF on a record with the given key, and returns the results.

Parameters:

- `policy`       – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `packageName`  – server path to the UDF
- `functionName` – UDF name
- `args`         – (optional) UDF arguments

Example:

Considering the UDF registered in RegisterUDF example above:

```go
    res, err := client.Execute(nil, key, "udf1", "testFunc1")

    // res will be a: map[any]any{"status": "OK"}
```
<!--
################################################################################
executeudf()
################################################################################
-->
<a name="executeudf"></a>

### ExecuteUDF(policy *QueryPolicy,  statement *Statement,  packageName string,  functionName string,  functionArgs ...Value) (*ExecuteTask, Error)

Executes a UDF on all records which satisfy filters set in the statement. If there are filters, it will run on all records in the database.

Parameters:

- `policy`       – (optional) A [Query Policy object](policies.md#QueryPolicy) to use for this operation.
                Pass `nil` for default values.
- `statement`    – [Statement object](datamodel.md#statement) to narrow down records.
- `packageName`  – server path to the UDF
- `functionName` – UDF name
- `functionArgs` – (optional) UDF arguments

Example:

Considering the UDF registered in RegisterUDF example above:

```go
  statement := as.NewStatement("namespace", "set")
  exTask, err := client.ExecuteUDF(nil, statement, "udf1", "testFunc1")
  panicOnErr(err)

  // wait until UDF is run on all records
  err = <- exTask.OnComplete()
  if err != nil {
    panic(err)
  }
```

<!--
################################################################################
query()
################################################################################
-->
<a name="query"></a>

### Query(policy *QueryPolicy, statement *Statement) (*Recordset, Error)

Performs a query on the cluster, and returns the results in a [Recordset object](datamodel.md#recordset)


Parameters:

- `policy`       – (optional) A [Query Policy object](policies.md#QueryPolicy) to use for this operation.
                Pass `nil` for default values.
- `statement`    – [Statement object](datamodel.md#statement) to narrow down records.

Refer to [Recordset object](datamodel.md#recordset) documentation for details on how to retrieve the data.


Example:

```go
  stm := as.NewStatement("namespace", "set")
  stm.SetFilter(as.NewRangeFilter("binName", value1, value2))

  recordset, err := client.Query(nil, stm)

  // consume recordset and check errors
  for res := range recordset.Results() {
    if res.Err != nil {
      // handle error, or close the recordset and break
    }

    // process record
    fmt.Println(res.Record)
  }
```

<!--
################################################################################
batchoperate()
################################################################################
-->
<a name="batchoperate"></a>

### BatchOperate(policy *BatchPolicy, records []BatchRecordIfc) Error

Reads and writes multiple records in one batch call. Each entry can be a `*BatchRead`, `*BatchWrite`, `*BatchDelete`, or `*BatchUDF`. Requires server version 6.0+.

Parameters:

- `policy`      – (optional) A [BatchPolicy object](policies.md#BatchPolicy). Pass `nil` for defaults.
- `records`     – Batch records to read or write.

Example:

```go
  key, err := as.NewKey("test", "demo", 123)
  if err != nil {
    panic(err)
  }

  bwrite := as.NewBatchWrite(nil, key,
    as.PutOp(as.NewBin("bin", "value")),
    as.GetBinOp("bin"),
  )
  err = client.BatchOperate(nil, []as.BatchRecordIfc{bwrite})
```

Related methods: `BatchGetOperate`, `BatchDelete`, `BatchExecute`.

<!--
################################################################################
filter-expressions()
################################################################################
-->
<a name="filter-expressions"></a>

### Filter expressions

Attach an expression to `QueryPolicy.FilterExpression` (or the `FilterExpression` field on other policies) to filter records on the server. This is separate from a secondary-index `Statement` filter.

Example:

```go
  queryPolicy := as.NewQueryPolicy()
  queryPolicy.FilterExpression = as.ExpGreater(
    as.ExpIntBin("occurred"),
    as.ExpIntVal(20210101),
  )

  stm := as.NewStatement("test", "demo")
  recordset, err := client.Query(queryPolicy, stm)
```

<!--
################################################################################
multi-record-transactions()
################################################################################
-->
<a name="multi-record-transactions"></a>

### Multi-record transactions

`Commit` and `Abort` coordinate a multi-record transaction (`*Txn`). Assign the transaction to `WritePolicy.Txn` or `BatchPolicy.Txn` on each command in the transaction. Requires server version 8.0+.

```go
  txn := as.NewTxn()
  writePolicy := as.NewWritePolicy(0, 0)
  writePolicy.Txn = txn

  key, err := as.NewKey("test", "demo", 123)
  if err != nil {
    panic(err)
  }

  err = client.Put(writePolicy, key, as.BinMap{"bin": 1})
  if err != nil {
    client.Abort(txn)
    panic(err)
  }

  status, err := client.Commit(txn)
  _ = status
```

- `Commit(txn *Txn) (CommitStatus, Error)`
- `Abort(txn *Txn) (AbortStatus, Error)`

<!--
################################################################################
object-api()
################################################################################
-->
<a name="object-api"></a>

### Object API

The reflection Object API maps Go structs to records. These methods are omitted when you build with the `as_performance` tag.

- `PutObject(policy *WritePolicy, key *Key, obj any) Error`
- `GetObject(policy *BasePolicy, key *Key, obj any) Error`
- `BatchGetObjects(policy *BatchPolicy, keys []*Key, objects []any) ([]bool, Error)`
- `ScanAllObjects`, `ScanNodeObjects`, `ScanPartitionObjects`
- `QueryObjects`, `QueryNodeObjects`, `QueryPartitionObjects`

Example:

```go
  type User struct {
    Name string `as:"name"`
    Age  int    `as:"age"`
  }

  key, err := as.NewKey("test", "demo", "user-1")
  if err != nil {
    panic(err)
  }

  err = client.PutObject(nil, key, &User{Name: "Ada", Age: 36})
  user := User{}
  err = client.GetObject(nil, key, &user)
```

<!--
################################################################################
cdt-operations()
################################################################################
-->
<a name="cdt-operations"></a>

### CDT operations

List and Map (CDT) operations run through `Operate` and `BatchOperate`. They are documented in [CDT Operations](cdt_operations.md).

# `sdk` — Specification

Version: 0.1.0-alpha · Status: alpha (the API may change before general availability)

`sdk` is the high-level, ergonomics-first Aerospike client for Go. It sits on top
of the core client in the parent package and is the Go counterpart of the Rust
`aerospike-sdk` crate, the Python `aerospike_sdk` package and the Java
`com.aerospike.client.sdk` package: the same connect → session → fluent-builder
model, the same Behavior policy system, and the same result and error semantics.

It requires **Go 1.27 or newer**: the target-polymorphic verbs use
parameterized methods. The core client keeps its own, lower, Go floor.

---

## Table of contents

1. [Object model](#1-object-model)
2. [Connecting](#2-connecting)
3. [Sessions](#3-sessions)
4. [Keys and datasets](#4-keys-and-datasets)
5. [The fast path](#5-the-fast-path)
6. [The builder chain](#6-the-builder-chain)
7. [Per-bin builders](#7-per-bin-builders)
8. [CDT navigation](#8-cdt-navigation)
9. [CDT path expressions](#9-cdt-path-expressions)
10. [Results and streams](#10-results-and-streams)
11. [Client-side sorting and pagination](#11-client-side-sorting-and-pagination)
12. [Errors and dispositions](#12-errors-and-dispositions)
13. [Queries](#13-queries)
14. [Row-oriented writes](#14-row-oriented-writes)
15. [Background tasks](#15-background-tasks)
16. [Secondary indexes](#16-secondary-indexes)
17. [User-defined functions](#17-user-defined-functions)
18. [Transactions](#18-transactions)
19. [The Behavior policy model](#19-the-behavior-policy-model)
20. [System settings and the configuration file](#20-system-settings-and-the-configuration-file)
21. [Info commands](#21-info-commands)
22. [Typed object mapping](#22-typed-object-mapping)
23. [Capability probes and server requirements](#23-capability-probes-and-server-requirements)
24. [Examples](#24-examples)
25. [Testing](#25-testing)
26. [Deviations from the other SDKs](#26-deviations-from-the-other-sdks)

---

## 1. Object model

```text
ClusterDefinition ──Connect()──▶ Cluster ──CreateSession(behavior)──▶ Session
                                    │                                   ├── Get/Put                  (fast path)
                                    └── Client() ──▶ sdk.Client         ├── Upsert/Insert/Update/…   ▶ WriteSegmentBuilder ▶ WriteBinBuilder ▶ Cdt*Builder
                                          │                             ├── UpsertRows(ds)           ▶ RowWriteBuilder
                                          └── UnderlyingClient()        ├── Query(key|keys|dataset)  ▶ QueryBuilder ▶ QueryBinBuilder
                                              ▶ *as.Client (escape)     ├── ExecuteUDF(key|keys)     ▶ UDFFunctionBuilder ▶ UDFBuilder
                                                                        ├── Index(ds)                ▶ IndexBuilder
                                                                        ├── UpsertTyped(tds)         ▶ ObjectWriteBuilder[T]
                                                                        └── Transaction()            ▶ TransactionalSession
                                                                                    │
                                              Execute()/Stream() ──────────────────▶ RecordStream ▶ RecordResult
                                                                                    │
                                              IntoNavigatable() ────────────────────▶ NavigatableRecordStream
```

Normative properties:

- A `Cluster` owns one connection. Closing it closes the connection for every
  session derived from it.
- A `Session` is cheap and safe for concurrent use, and binds one `Behavior` for
  its lifetime. Create many per cluster.
- Builders hold the first argument error and surface it from the terminal, so a
  chain never returns an error mid-way.
- Nothing reaches the server until a terminal runs: `Execute`, `Stream`,
  `Create`, `Drop`, `Commit`, a background task, or a fast-path `Get`/`Put`.

## 2. Connecting

```go
cluster, err := sdk.NewClusterDefinition("localhost", 3000).
    WithNativeCredentials("admin", "secret").
    ValidateClusterNameIs("prod-dc1").
    PreferringRacks(1, 2).
    UsingServicesAlternate().
    AppID("billing-service").
    Connect()
defer cluster.Close()
```

| Method | Description |
| --- | --- |
| `NewClusterDefinition(host, port)` / `WithHosts(hosts...)` | Start a definition |
| `WithNativeCredentials(user, pass)` | Internal authentication |
| `WithExternalCredentials(user, pass)` | External authentication; requires TLS |
| `WithExternalInsecureCredentials(user, pass)` | External authentication without TLS — the explicit opt-in |
| `WithCertificateCredentials()` | PKI authentication; requires TLS and a TLS name on every host |
| `ValidateClusterNameIs(name)` | Refuse a differently-named cluster |
| `PreferringRacks(ids...)` | Rack awareness with an ordered preference |
| `UsingServicesAlternate()` | Alternate service endpoints |
| `FailIfNotConnected(bool)` | Default true |
| `WithIPMap(m)` | Translate server-reported addresses |
| `WithSystemSettings(s)` | Cluster-wide settings ([§20](#20-system-settings-and-the-configuration-file)) |
| `TendTimeout(d)` / `LoginTimeout(d)` | Handshake and tend timeouts |
| `WithTLSConfigOf()` / `WithTLSConfig(cfg)` | TLS |
| `Connect()` | Validate, apply the configuration file, connect |

`AEROSPIKE_USE_SERVICES_ALTERNATE` (`true`/`1`/`yes`) enables services-alternate
without a code change.

TLS misconfiguration surfaces from `Done()`, not at connect: a missing CA file,
an unreadable PEM, or a rejected certificate and key pair all fail while
building.

## 3. Sessions

| Group | Methods |
| --- | --- |
| Accessors | `Behavior()`, `Client()`, `CurrentTransaction()`, `SessionFor(b)` |
| Fast path | `Get(key, bins)`, `Put(key, bins)` |
| Write verbs | `Upsert`, `Insert`, `Update`, `Replace`, `ReplaceIfExists`, `Delete`, `Touch`, `Exists` |
| Row writes | `UpsertRows`, `InsertRows`, `UpdateRows`, `ReplaceRows`, `ReplaceIfExistsRows` |
| Reads | `Query(target)` |
| Typed | `QueryTyped`, `UpsertTyped`, `InsertTyped`, `UpdateTyped`, `ReplaceTyped`, `ReplaceIfExistsTyped` |
| UDF | `ExecuteUDF`, `RegisterUDF`, `RegisterUDFFromFile`, `RemoveUDF`, `ListUDF` |
| Indexes | `Index(ds)`, `ListIndexes()` |
| Transactions | `Transaction()`, `TransactionWithTimeout(d)`, `DoInTransaction(fn, attempts, sleep)` |
| Info | `Info(cmd)`, `InfoOnAllNodes(cmd)`, `InfoCommands()`, `IsNamespaceSC(ns)`, `NamespaceScStatus(ns)` |
| Admin | `Truncate(ds, beforeNanos)` |

A session's cached policies are rebuilt automatically when its Behavior changes
through a configuration reload; an in-flight operation sees either the old or
the new snapshot, never a mix.

## 4. Keys and datasets

```go
users, err := sdk.DataSetOf("test", "users")

k1 := users.Key("user-123")     // string    -- no error
k2 := users.Key(int64(42))      // integer
k3 := users.Key([]byte{1, 2})   // blob
many := users.Keys([]int64{1, 2, 3})

byDigest, err := users.IDFromDigest(k1.Digest())
```

`Key` and `Keys` constrain the identifier to `KeyValue`:

```go
type KeyValue interface {
    ~string |
        ~int | ~int8 | ~int16 | ~int32 | ~int64 |
        ~uint8 | ~uint16 | ~uint32 |
        ~[]byte
}
```

Restricting the type at compile time is what removes the "unsupported key type"
failure mode, and with it the error return: every admitted type converts, and
the approximation constraints admit named types, so `type UserID int64` works.
This is where Go improves on the Python and Rust SDKs, whose `DataSet.id` must
return a fallible result because their identifier types are unbounded.

The unsigned types stop at `uint32` deliberately. An Aerospike integer user key
is a signed 64-bit value, so `uint` and `uint64` could carry values above
`MaxInt64` that would silently wrap onto a negative key — admitting them would
trade the error return for a worse failure. Use a `string` or `[]byte`
identifier for values beyond `int64`.

Because Go has no overloading, that one Python/Rust method becomes two here:

| Method | Identifier | Fails? |
| --- | --- | --- |
| `Key(id)` / `Keys(ids)` | statically typed, constrained to `KeyValue` | no |
| `ID(any)` | known only at run time | yes -- an inadmissible type is an error |

`ID` is what the internals use when an identifier arrives as `any` (a mapped
entity's user key, a `RowWriteBuilder` row identifier). Prefer `Key` in
application code.

`IDFromDigest` accepts 20 raw bytes or 40 hex characters.

### Targets

The verbs are generic over a target union, so one method covers what the other
SDKs express as an overload set:

| Union | Accepts |
| --- | --- |
| `WriteTarget` | `*as.Key`, `[]*as.Key` |
| `QueryTarget` | `*as.Key`, `[]*as.Key`, `*DataSet` |
| `Predicate` | `*as.Expression`, `string` (AEL source) |
| `BinsArg` | `Bins` (`AllBins`, `NoBins`), `[]string` |

## 5. The fast path

`Get` and `Put` bypass the builder chain and the record stream, so one call
reaches the server. Use them for a single key; use the builders when you need
multi-operation atomicity, expirations, generation guards, filters or streaming.

```go
err := session.Put(key, as.BinMap{"name": "Ada", "age": 36})

rec, err := session.Get(key, sdk.AllBins)
proj, err := session.Get(key, []string{"name"})
head, err := session.Get(key, sdk.NoBins)   // metadata only
```

A missing record is an error on this path, not an empty result.

> A header-only read must not carry the user key: the server rejects it. The core
> client clears `SendKey` on a copy of the policy for header reads, so `NoBins`
> works even though the DEFAULT behavior enables `SendKey`.

## 6. The builder chain

Eight verbs open a `WriteSegmentBuilder`:

| Verb | Record-exists semantics |
| --- | --- |
| `Upsert` | Create or update (the server default) |
| `Insert` | Create only; fails when present |
| `Update` | Update only; fails when absent |
| `Replace` | Create or replace; unwritten bins are removed |
| `ReplaceIfExists` | Replace only; fails when absent |
| `Delete` | Delete the record |
| `Touch` | Reset the expiration, bump the generation |
| `Exists` | Existence check; read it with `RecordResult.AsBool` |

Segment modifiers: `Where`, `ExpireRecordAfterSeconds`, `ExpireRecordAfter`,
`ExpireRecordAt`, `NeverExpire`, `WithNoChangeInExpiration`,
`ExpiryFromServerDefault`, `EnsureGenerationIs`, `WithDurableDelete`,
`WithoutDurableDelete`, `DefaultWithDurableDelete`,
`DefaultWithoutDurableDelete`, `IncludeMissingKeys`, `FailOnFilteredOut`,
`ReplaceOnly`, `WithTxn`.

Operations: `Put`, `SetTo`, `SetBinsTo`, `Add`, `IncrementBy`, `Append`,
`Prepend`, `Get`, `RemoveBin`, `DeleteRecord`, `TouchRecord`, `AddOperation`,
and `Bin(name)` to descend.

### Chaining segments

Verbs chain; each call finalizes the current segment and opens the next. A chain
with several segments executes as **one batch**, unless it holds a UDF segment,
which forces sequential execution.

```go
stream, err := session.Update(k1).Add("counter", 5).
    Delete([]*as.Key{k2}).
    Insert(k3).SetTo("status", "new").
    Execute()
```

### Terminals

| Terminal | Semantics |
| --- | --- |
| `Execute()` | Buffered. Writes are complete when it returns. |
| `ExecuteOnError(onErr)` | `Execute` with an explicit disposition |
| `Stream()` | Lazy. Rows arrive in **completion order**; no writes-complete guarantee |
| `StreamOnError(onErr)` | Lazy with a disposition |
| `ExecuteBackgroundTask()` and friends | Server-side work over a dataset query ([§15](#15-background-tasks)) |

`Stream` falls back to the buffered path for a dataset query (already lazy
server-side), a chain that needs sequencing, and a batch wrapped in an implicit
transaction.

## 7. Per-bin builders

`WriteSegmentBuilder.Bin(name)` returns a `WriteBinBuilder`;
`QueryBuilder.Bin(name)` returns the read-only `QueryBinBuilder`. Every terminal
returns the parent, so chains flow back:

```go
stream, err := session.Upsert(key).
    Bin("counter").SetTo(100).
    Bin("counter").Add(11).
    Bin("counter").Get().
    Execute()
```

Families: scalar (`SetTo`, `SetToGeoJSON`, `Add`, `Append`, `Prepend`, `Remove`,
`Get`), expression (`SelectFrom`, `WriteFrom`), list, map, HyperLogLog
(init, add, count, describe, `HLLSetUnion`, `HLLFold`, `HLLRefreshCount`,
`HLLGetUnion`, `HLLGetUnionCount`, `HLLGetIntersectCount`,
`HLLGetSimilarity`), bitwise, and the **complete** server-side string family (server 8.1.3+): the
reads (`StrLen`, `StrSubstr`, `StrSubstrFrom`, `StrCharAt`, `StrFind`,
`StrFindNth`, `StrContains`, `StrStartsWith`, `StrEndsWith`, `StrToInteger`,
`StrToDouble`, `StrByteLength`, `StrIsNumeric`, `StrIsNumericTyped`,
`StrIsUpper`, `StrIsLower`, `StrToBlob`, `StrToString`, `StrSplit`,
`StrSplitBySeparator`, `StrB64Decode`, `StrRegexCompare`,
`StrRegexCompareWithFlags`) and the modifications (`StrInsert`,
`StrOverwrite`, `StrConcat`, `StrConcatList`, `StrAppend`, `StrPrepend`,
`StrUpper`, `StrLower`, `StrCaseFold`, `StrNormalizeNFC`, `StrTrim`,
`StrTrimStart`, `StrTrimEnd`, `StrPadStart`, `StrPadEnd`, `StrRepeat`,
`StrSnip`, `StrReplace`, `StrReplaceAll`, `StrRegexReplace`).

> **Several operations on one bin.** The bin map holds only the last value, so
> read them positionally with `RecordResult.OperationResult(i)` or
> `OperationResults()`. Only operations that *produce* a value are represented:
> a chain of put, add and get yields one result, from the get.

## 8. CDT navigation

`OnMapKey`, `OnMapIndex`, `OnMapRank`, `OnMapValue`, `OnListIndex`,
`OnListRank` and `OnListValue` select inside a collection and return a builder
that can navigate deeper. The range and multi-value selectors — `OnMapKeyRange`, `OnMapValueRange`,
`OnMapIndexRange`, `OnMapRankRange`, `OnMapKeyList`, `OnMapValueList`,
`OnMapKeyRelativeIndexRange`, `OnMapValueRelativeRankRange`,
`OnListIndexRange`, `OnListValueRange`, `OnListRankRange`, `OnListValueList`,
`OnListValueRelativeRankRange` — have no server-side context form, so the builders they return have **no
navigation methods at all**, making a further `On*` call a compile error rather
than a runtime failure.

Those builders offer the ordinary terminals (`GetValues`, `GetKeys`,
`GetKeysAndValues`, `Count`, `GetIndexes`, `GetRanks`, `Remove`, `RemoveAnd`)
and their **inverted** counterparts, which address everything the selection did
*not* match: `GetAllOtherValues`, `GetAllOtherKeys`,
`GetAllOtherKeysAndValues`, `CountAllOthers`, `GetAllOtherIndexes`,
`GetAllOtherRanks`, `RemoveAllOthers` and `RemoveAllOthersAnd`. A map selection
can additionally report itself as a collection with `GetAsOrderedMap` or
`GetAsUnorderedMap`, or answer `GetExists`.

A **relative** selection anchors on a value and walks an index or rank offset
from wherever that value sorts, which is how you express "the three entries
after this key" without knowing its position. The anchor need not be present —
the server uses where it *would* sort — and a negative count runs to the end:

```go
// The two entries after wherever "b" sorts.
stream, err := session.Query(key).
    Bin("m").OnMapKeyRelativeIndexRange("b", 1, 2).GetKeys().
    Execute()
```

```go
keyOrdered := as.MapOrder.KEY_ORDERED

// Two levels deep, creating the intermediates on the way.
stream, err := session.Upsert(key).
    Bin("doc").OnMapKey("mid", &keyOrdered).OnMapKey("leaf", &keyOrdered).SetTo(99).
    Execute()

// Per-key write modes.
stream, err = session.Update(key).
    Bin("prefs").OnMapKey("theme", nil).Insert("dark", true).   // create-only, silent
    Bin("scores").OnListIndexRange(0, 2).RemoveAnd().Count().   // remove and report
    Execute()
```

`SetTo`, `Insert`, `Update` and `Add` need an `OnMapKey` selection, the only
form the server resolves to one writable slot; off any other selection the error
surfaces from the terminal.

`RemoveAnd()` returns a builder whose terminal picks the report: `Count`,
`GetValues`, `GetKeys`, `GetKeysAndValues`.

> A per-key write mode does **not** impose a map ordering: `Insert` and `Update`
> leave the bin unordered, because a write mode is orthogonal to the ordering.
> Choose an ordering explicitly with `MapSetPolicy` or `MapCreate`.

## 9. CDT path expressions

Everything above addresses one place. Path expressions address *every* matching
place. They need server 8.1.1+.

```go
// Read the title of every catalogue entry.
stream, err := session.Query(key).
    Bin("catalog").OnEachChild().OnMapKey("title").NoFail().CollectValues().
    Execute()

// Raise every price by half, tolerating non-numeric leaves.
stream, err = session.Update(key).
    Bin("catalog").OnEachChild().OnMapKey("price").NoFail().
    ModifyBy(as.ExpNumMul(...)).
    Execute()
```

A path chain need not start at the bin root. Fixed navigation and path
iteration compose, because `OnEachChild` and `OnEachChildWhere` live on the
navigable CDT builders as well as on the bin builders:

```go
// Only the titles under the "book" key.
stream, err := session.Query(key).
    Bin("catalog").OnMapKey("book", nil).
    OnEachChild().OnMapKey("title").NoFail().CollectValues().
    Execute()
```

Path steps: `OnMapKey`, `OnMapValue`, `OnMapKeysIn` (a multi-key context only
path expressions accept), `OnListIndex`, `OnListValue`, plus further
`OnEachChild` / `OnEachChildWhere` steps.

The builders come in pairs by what the last step selected: a map-like or
not-yet-narrowed step yields a builder with `CollectKeys` and
`CollectKeysAndValues`; a list step yields one **without** them, so asking for
map keys after descending into a list does not compile.

| Terminal | Result |
| --- | --- |
| `CollectValues()` | The values of the finally selected nodes |
| `CollectTree()` | A structure-preserving tree from the root to the matches |
| `CollectKeys()` † | The map keys of the selected entries |
| `CollectKeysAndValues()` † | The selected entries as interleaved keys and values |
| `CollectValuesAsExpressionRead(binType, resultType, ignoreEvalFailure)` | The same selection evaluated as an expression read stored under this chain's bin name |
| `ModifyBy(exp)` ‡ | Apply an expression at every match |
| `RemoveMatches()` ‡ | Remove every match |

`NoFail()` is a modifier, not a terminal.

† Map-like steps only. ‡ Write chains only.

## 10. Results and streams

`RecordStream` delivers `*RecordResult` rows. Failures arrive on two channels,
deliberately: a cluster-level failure is an error from `Next`, while a
per-record failure rides on the row.

```go
for row := range stream.Iter() {
    if row.IsOK() { /* ... */ }
}
if err := stream.Err(); err != nil { /* cluster-level */ }
```

| Method | Semantics |
| --- | --- |
| `Next()` | `(nil, nil)` when exhausted or closed |
| `Iter()` | Range-over-function; check `Err()` afterwards |
| `Pop()` / `PopOrRaise()` | One row; the stream stays open |
| `First()` / `FirstOrRaise()` | One row, then **close** |
| `FirstUDFResult()` / `FirstUDFResultAs[T]()` | The first function result, raw or mapped |
| `FindRecord(key)` | Forward scan by digest; earlier rows are consumed |
| `Collect()` / `Failures()` | Drain, all rows or only the failed ones |
| `HasMoreChunks()` | Chunked-query cursor ([§13](#13-queries)) |
| `IntoNavigatable()` / `IntoNavigatableLimit(n)` | Drain for client-side sorting |
| `Close()` | Idempotent; always defer it |

`RecordResult` carries `Key`, `Record`, `ResultCode`, `InDoubt`, `Index`, `Err`
and `UDFResult`, with `IsOK`, `OrRaise`, `RecordOrRaise`, `AsBool`,
`OperationResult(i)`, `OperationResults()`, `OperationResultAt(i)`,
`GetHLLConfig(bin)` and `UDFResultAsObject[T]()`.

`Index` is the row's position in the originating batch: 0 for a single key, −1
for a query. Because `Stream` delivers in completion order, `Index` is the way
back to input order.

## 11. Client-side sorting and pagination

`IntoNavigatable` drains a stream into an in-memory result set that can be
re-sorted and re-paged with no further server round trips.

```go
nav, err := stream.IntoNavigatable()
nav.SortBy(sdk.Desc("age"), sdk.AscIgnoreCase("lastName")).PageSize(20)

for nav.HasMorePages() {
    for nav.HasNext() {
        row := nav.Next()
        _ = row
    }
}
```

`SortBy` replaces the criteria, re-sorts **stably** and rewinds.
`HasMorePages` is deliberately mutating, so the loop shape above works; once it
ends, `CurrentPage()` is one past `MaxPages()`.

`CompareValues(a, b, caseInsensitive)` is public and implements the server's
ordering: `NIL < BOOLEAN < INTEGER < STRING < LIST < MAP < BYTES < DOUBLE <
GEOJSON`, with no numeric promotion across types (every integer sorts before
every float), HyperLogLog compared as bytes, lists element-wise with the shorter
prefix first, maps by length then keys then values, and case folding applied at
every nesting level. Rows whose sort bin is missing — including rows that carry
no record, as a per-key failure does — sort first ascending and last descending
rather than failing.

## 12. Errors and dispositions

`sdk.Error` wraps the core error, so `errors.Is` and `errors.As` reach the core
chain. It adds `Kind()`, `Message()`, `ResultCode()`, `InDoubt()`, `SubCode()`,
`ServerMessage()`, `Matches(codes...)` and the family predicates
`IsSecurity`, `IsBinError`, `IsElementError`, `IsCapacityError`,
`IsSecondaryIndexError`, `IsBackoffError`, `IsTransactionError`.

How per-record failures surface:

| `OnError` | Single key | Batch |
| --- | --- | --- |
| `nil` (default) | error from the terminal | embedded as non-OK rows |
| `sdk.InStream()` | embedded | embedded |
| `sdk.Handler(fn)` | dispatched to `fn`, excluded | dispatched, excluded |

Two result codes are context-dependent rather than unconditional failures:
`KEY_NOT_FOUND` is actionable only for verbs that need an existing record
(`Update`, `ReplaceIfExists`), and `FILTERED_OUT` only under
`FailOnFilteredOut()`. A `Delete` always publishes its not-found rows, because
deleting an absent key is a benign per-row outcome; reads need
`IncludeMissingKeys()`.

## 13. Queries

```go
// Point and batch reads.
stream, err := session.Query(key).Execute()
stream, err = session.Query(keys).Bins("name", "age").IncludeMissingKeys().Execute()

// Set-wide, with a filter and a secondary-index filter.
stream, err = session.Query(users).
    Where(as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(25))).
    Filter(as.NewRangeFilter("age", 22, 24)).
    OnPartitionRange(0, 2048).
    Limit(500).
    RecordsPerSecond(1000).
    Execute()

// AEL source text; the server compiles it (8.1.3+).
stream, err = session.Query(users).Where("$.age >= 25 and $.status == 'active'").Execute()
```

The client refuses to send AEL to a cluster with any node below 8.1.3, naming
the required version, rather than shipping a filter the server cannot parse. AEL
itself is never validated client-side: the server is the parser, so a syntax
error is a server error at execution time.

### Chunked iteration

`ChunkSize(n)` turns a dataset query into a paged cursor:

```go
stream, err := session.Query(users).ChunkSize(100).Execute()
for more, err := stream.HasMoreChunks(); more && err == nil; more, err = stream.HasMoreChunks() {
    for {
        row, err := stream.Next()
        if err != nil || row == nil { break }
    }
}
```

`HasMoreChunks` reports true on its first call for every stream shape, so one
loop serves chunked and non-chunked queries alike.

## 14. Row-oriented writes

For many records of the same shape, declare the bins once:

```go
stream, err := session.UpsertRows(users).
    Bins("name", "age").
    Row(int64(1), "Alice", 34).
    Row(int64(2), "Bob", 41).
        ExpireRecordAfterSeconds(3600).   // row 2 only
    DefaultExpireRecordAfterSeconds(60).  // rows without their own
    Execute()
```

Each row becomes its own write segment, so guards stay per-record and rows come
back in insertion order. The chain is infallible: a missing `Bins`, a
value-count mismatch, a guard before the first row, and a past
`ExpireRecordAt` are all reported from `Execute`.

## 15. Background tasks

Server-side work over every record a dataset query matches.

```go
task, err := session.Query(ds).
    Where(as.ExpGreater(as.ExpIntBin("score"), as.ExpIntVal(5))).
    WithWriteOperations(as.PutOp(as.NewBin("tier", "gold"))).
    ExecuteBackgroundTask()
err = <-task.OnComplete()
```

| Terminal | Effect |
| --- | --- |
| `ExecuteBackgroundTask()` | Applies the operations from `WithWriteOperations` |
| `ExecuteBackgroundDelete()` | Deletes every match; injects its own operation |
| `ExecuteBackgroundTouch()` | Resets every match's expiration |
| `ExecuteUDFBackgroundTask(pkg, fn, args...)` | Applies a registered function |

Rules: the builder must target a dataset, not keys; `ExecuteBackgroundTask`
needs at least one write operation while the others must not have any; and
`FailOnFilteredOut` and `IncludeMissingKeys` are rejected eagerly, because a
background task returns no per-record rows.

## 16. Secondary indexes

```go
task, err := session.Index(users).OnBin("age").Named("users_age_idx").Numeric().Create()
err = <-task.OnComplete()

indexes, err := session.ListIndexes()
err = session.Index(users).Named("users_age_idx").Drop()
```

`Numeric`, `String`, `Geo2DSphere` and `Blob` set the value type; `Blob` needs
server 7.0+. `Collection(t)` indexes inside a collection. A missing name or
value type fails before any I/O.

## 17. User-defined functions

```go
task, err := session.RegisterUDF(source, "echo.lua", as.LUA)
err = <-task.OnComplete()

stream, err := session.ExecuteUDF(key).
    Function("echo", "echo").
    Passing(as.NewValue(42)).
    Execute()
result, err := stream.FirstUDFResult()

modules, err := session.ListUDF()
_, err = session.RemoveUDF("echo.lua")
```

A batch function produces one row per key, with the Lua value lifted out of the
server's `SUCCESS` bin into `RecordResult.UDFResult`, so batch and single-key
rows have the same shape. Map a table result to a struct with
`UDFResultAsObject[T]()` or `FirstUDFResultAs[T]()`.

## 18. Transactions

Multi-record transactions need a strong-consistency namespace on a server that
supports them (8.0+).

```go
tx, err := session.Transaction()
_, err = tx.Upsert(a).SetTo("balance", 100).Execute()
_, err = tx.Upsert(b).SetTo("balance", 200).Execute()
status, err := tx.Commit()   // or tx.Abort() / tx.Rollback()
```

`TransactionalSession` embeds `*Session`, so every verb works inside the
transaction and the builders stamp it onto the policies they issue. Use
`WithTxn(nil)` on a builder to run one operation outside it.

Go has no destructor, so finalization is explicit: an unfinalized transaction is
left to expire server-side. Finalizing twice is an error.

The verify and roll phases use the policies resolved from the session's Behavior
(`ScopeSystemTxnVerify` and `ScopeSystemTxnRoll`), so their timeouts, retries,
replica choice and read consistency are configurable per Behavior.

`DoInTransaction(fn, maxAttempts, sleep)` runs a function in a transaction,
committing on success and aborting on failure, retrying the transient conflicts.

### Implicit batch-write transactions

A multi-key **write** batch is wrapped in an implicit transaction — so its
writes commit atomically — when the namespace is strong-consistency, the batch
contains writes, no explicit transaction is active and none was declined, every
node supports transactions, and the setting is enabled (the default). No API
change is needed: ordinary batch writes gain atomicity.

The implicit transaction's verify and roll phases resolve their policies from the
session's Behavior, exactly as an explicit transaction does, so the
`SystemTxnVerify` and `SystemTxnRoll` scopes apply to both paths.

## 19. The Behavior policy model

A `Behavior` is an immutable, named bundle of operation policies. It resolves by
scope, inherits from a parent, and caches the resolved matrix eagerly, so a
lookup on the operation path is a map read behind one atomic load.

Every operation resolves against three coordinates: `OpKind` (`OpRead`,
`OpWriteRetryable`, `OpWriteNonRetryable`), `OpShape` (`ShapePoint`,
`ShapeBatch`, `ShapeQuery`) and `Mode` (`ModeAP`, `ModeSC`, resolved and cached
per namespace).

Resolution layers scopes least- to most-specific:

| Coordinates | Order |
| --- | --- |
| read / point / AP | `All` → `Reads` → `ReadsAP` → `ReadsPoint` |
| read / batch / SC | `All` → `Reads` → `ReadsSC` → `ReadsBatch` |
| non-retryable write / query / SC | `All` → `Writes` → `WritesSC` → `WritesNonRetryable` → `WritesQuery` |
| transaction verify | `All` → `SystemTxnVerify` |
| transaction roll | `All` → `SystemTxnRoll` |

`Settings` fields are pointers; nil means "not set — inherit".

```go
tuned := sdk.DefaultBehavior().DeriveWithChanges("tuned", map[sdk.Scope]sdk.Settings{
    sdk.ScopeAll:        {TotalTimeout: sdk.DurationPtr(5 * time.Second)},
    sdk.ScopeReadsBatch: {MaxConcurrentNodes: sdk.IntPtr(8)},
    sdk.ScopeWritesSC:   {DurableDelete: sdk.BoolPtr(true)},
})
```

> **Inheritance.** A child's patches layer over the parent's fully **resolved**
> matrix, so a child's `ScopeAll` patch beats a parent's more specific scope.
> This matches the Python SDK.

Predefined: `DefaultBehavior()`, `ReadFastBehavior()`,
`StrictlyConsistentBehavior()`, `FastRackAwareBehavior()`. Registry:
`GetBehavior`, `GetBehaviorOrDefault`, `AllBehaviors`. Inspection: `Settings`,
`SystemSettingsFor`, `Name`, `Parent`, `Children`, `FindBehavior`, `Explain`,
`ClearCache`, `Generation`.

> **`ScopeWritesRetryable` resolves correctly but never applies.** Every write
> the SDK issues is classified `OpWriteNonRetryable`, so that scope — and the
> `retryableWrites:` config block that maps to it — parses, resolves and shows up
> in `Explain()` while changing nothing on the command path. Put write settings
> in `ScopeWritesNonRetryable` or `ScopeWrites` if you want them to take effect.
>
> This is not a Go-only gap: the Rust SDK is in the identical state (its
> `SPEC.md` §21.2 carries the same warning, and its `TODO.md` lists the item as
> open), and the resolution is undecided upstream — either classify retryable
> writes properly, or drop the scope and its YAML key. Go deliberately keeps the
> scope so that a configuration file written for another SDK still loads; the
> classification decision belongs upstream, and this port does not invent one.

## 20. System settings and the configuration file

`SystemSettings` holds cluster-wide values that cannot vary per Behavior:
connection sizing, socket idle time, tend interval, circuit-breaker thresholds,
and the `Transactions` group (`ImplicitBatchWriteTransactions`,
`NumberOfAttempts`, `SleepBetweenAttempts`).

Set `AEROSPIKE_SDK_CONFIG_URL` to a path or `file://` URL:

```yaml
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 10
      maximumConnectionsPerNode: 300
      maximumSocketIdleTime: 30s
    circuitBreaker:
      numTendIntervalsInErrorWindow: 1
      maximumErrorsInErrorWindow: 100
    refresh:
      tendInterval: 1s
    transactions:
      implicitBatchWriteTransactions: true
      numberOfAttempts: 5
      sleepBetweenAttempts: 1s
  prod-dc1:
    connections:
      maximumConnectionsPerNode: 600

behaviors:
  low_latency_reads:
    allOperations:
      abandonCallAfter: 2s
      waitForCallToComplete: 200ms
      maximumNumberOfCallAttempts: 3
      replicaOrder: PREFER_RACK
    query:
      recordQueueSize: 10000
  derived:
    parent: low_latency_reads
    consistencyModeReads:
      readConsistency: LINEARIZE
```

Selector blocks map to scopes: `allOperations`→`All`,
`retryableWrites`→`WritesRetryable`, `nonRetryableWrites`→`WritesNonRetryable`,
`consistencyModeReads`→`ReadsSC`, `availabilityModeReads`→`ReadsAP`,
`batchReads`→`ReadsBatch`, `batchWrites`→`WritesBatch`, `query`→`ReadsQuery`,
`systemTxnVerify` and `systemTxnRoll` to the two system scopes.

Field names: `abandonCallAfter`→total timeout, `waitForCallToComplete`→socket
timeout, `delayBetweenRetries`→retry delay,
`maximumNumberOfCallAttempts`→max retries (**attempts − 1**),
`replicaOrder`, `sendKey`, `useCompression`, `useDurableDelete`,
`resetTtlOnReadAtPercent`, `readConsistency`, `migrationReadConsistency`,
`maxConcurrentServers`, `allowInlineMemoryAccess`, `allowInlineSsdAccess`,
`recordQueueSize`, `errorDetailVerbosity`.

Durations accept `250ms`, `1s`, `5m`, `2h`, `1d` and the long forms.

Precedence: `file cluster profile > file DEFAULT profile > WithSystemSettings >
hard defaults`. Parsing is fail-soft: an unknown key or a bad value is logged
and skipped rather than failing the connect.

A monitor polls the file about once a second, using three gates in order —
modification time, raw bytes, resolved equality — and keeps the last-good
settings when a reload fails. The `behaviors:` section is fully live; connection
sizing, tend interval and circuit-breaker thresholds are applied at connect only,
so changing them in practice means reconnecting.

## 21. Info commands

```go
info := session.InfoCommands()

names, err := info.Namespaces()
builds, err := info.Build()
size, err := info.ClusterSize()
stable, err := info.IsClusterStable()

detail, err := info.NamespaceDetail("test")          // merged across nodes
perNode, err := info.NamespaceDetailPerNode("test")
sets, err := info.SetDetails("test")
indexes, err := info.SindexList("test")

raw, err := session.Info("statistics")
all, err := session.InfoOnAllNodes("build")
```

`InfoStats` is the foundation: `ParseInfoStats(body, sep)` splits a `key=value`
body — `";"` for a whole response, `":"` for the entries of a multi-item one —
and offers `Get`, `GetInt`, `GetFloat`, `GetBool`, `Len`, `IsEmpty`, `Raw`.
Lookups are dash- and underscore-flexible, because the server mixes the two.

`MergeInfoStats(perNode, overrides)` combines bodies. With no override the
strategy is sniffed from the value shape: integers sum, floats average, booleans
require agreement, anything else takes the most common value. `MergeStrategy`
also offers `MergeMinimum`, `MergeMaximum`, `MergeOr`, `MergeMustMatch` and
`MergeFirst`.

`NamespaceDetail` wraps the whole stat map plus curated getters, because the
namespace response varies by server version. A merged `Sindex` reads as
write-only while *any* node is still populating it.

## 22. Typed object mapping

```go
type Customer struct {
    ID   int64  `as:",key"`
    Name string `as:"name"`
    Age  int64  `as:"age"`
    Note string `as:"-"`
    Gen  uint32 `asm:"gen"`
}

customers, err := sdk.TypedDataSetOf[Customer]("test", "customers")

_, err = session.UpsertTyped(customers).
    Object(&alice).
        EnsureGenerationIs(3).
        ExpireRecordAfterSeconds(600).
    Object(&bob).
    DefaultExpireRecordAfterSeconds(60).
    Execute()

people, err := session.QueryTyped(customers).Where("$.age > 25").Execute()
objs, err := people.IntoObjects()
```

Mapping uses the core client's struct tags plus one addition:

| Tag | Effect |
| --- | --- |
| `as:"name"` | Bin name; the field name by default |
| `as:",key"` | **New.** The user key. Not stored as a bin; recovered from the key on reads, which needs `SendKey` (on in DEFAULT) |
| `as:"-"` | Never written or read |
| `as:",omitempty"` | Skip a zero value on write |
| `asm:"gen"` | Filled with the generation on reads, never written |

A type may take control instead by implementing `RecordMapper` on its pointer
receiver — `ToBins`, `SetFromRecord`, `ID` — which overrides reflection
entirely.

`TypedRecordStream[T]`: `NextObject`, `IntoObjects`, `FirstObject`,
`NextObjectWithMetadata`, `Failures`, `Next`, `Untyped`, `IntoNavigatable`.
`TypedNavigatableRecordStream[T]` mirrors the untyped navigatable surface and
adds `NextObject` and `Objects`.

### Typed keys

`TypedDataSet[T]` carries the entity type for a whole set; `TypedKey[T]` carries
it for one record, so a read by key produces objects without being handed the
dataset again:

```go
key := customers.TypedKey(42)                  // TypedKey[Customer]
stream, err := session.QueryTypedKey(key)      // *TypedRecordStream[Customer]
alice, err := stream.FirstObject()

keys := customers.TypedKeys([]int64{1, 2, 3})  // TypedKeyList[Customer]
stream, err = session.QueryTypedKeyList(keys)
```

| Member | Effect |
| --- | --- |
| `TypedDataSet.TypedKey(id)` / `TypedKeys(ids)` | Mint typed keys; cannot fail, like `DataSet.Key` |
| `TypedDataSet.TypedKeyForObject(obj)` | The typed key of an instance, from its key field |
| `TypedKeyOf[T](key)` / `TypedKeysOf[T](keys)` | Attach an entity type to existing keys |
| `TypedKey.Key()` / `TypedKeyList.Keys()` | Unwrap into the untyped world |
| `TypedKey.IsZero` / `Namespace` / `SetName` / `String` | Inspection; safe on the zero value |
| `Session.QueryTypedKey(k)` / `QueryTypedKeyList(ks)` | Typed reads needing no dataset argument |

The type is *asserted*, not verified: nothing on a key records what was written
under it, so `TypedKeyOf[T]` says "read this as a `T`" and a mismatch surfaces
when the record is mapped.

Two consequences of Go's type system, both deviations from the other SDKs:

- **A write from a typed key unwraps.** `WriteTarget` is a union of concrete
  types and cannot admit `TypedKey[T]` for a free `T`, so writes go through
  `key.Key()`. Little is lost — Java's `upsert(TypedKey<?>)` also returns an
  untyped builder, because a write carries no entity type either way.
- **The single-key and many-key reads are separate methods.** Go cannot infer `T`
  through a union of `TypedKey[T] | TypedKeyList[T]`, so the target-polymorphic
  shape used everywhere else is unavailable here. `Session.QueryTypedKeys(ds,
  keys)` remains for raw keys, where the dataset supplies the type.

## 23. Capability probes and server requirements

Six probes on `Cluster` and `Client` report what the **whole** cluster supports,
so a feature can be avoided before the server rejects it. All are
min-across-cluster: one node below the requirement makes the answer false.

| Probe | Requires |
| --- | --- |
| `SupportsMRT()` | 8.0 |
| `SupportsCDTPathExpressions()` | 8.1.1 |
| `SupportsExpressionIndex()` | 8.1.2 |
| `SupportsStringOperations()` | 8.1.3 |
| `SupportsExtendedErrorDetail()` | 8.1.3 |
| `SupportsServerCompiledAEL()` | 8.1.3 |
| `SupportsBlobIndex()` | 7.0 |

Only AEL is gated client-side; branch on the rest yourself. Durable delete needs
Enterprise Edition.

## 24. Examples

`sdk/examples/` holds ports of all 13 Rust SDK examples, which are themselves
ports of the Java SDK's `com.aerospike.examples`, plus a short `basic` tour.

Each example is a package exposing `Run(*exrun.Env) error` with a `cmd/` main,
and the suite in that directory drives every `Run` against a live cluster:

```bash
go1.27rc2 test ./sdk/examples/ -args -h 127.0.0.1 -p 3000 -n test \
    -sc-namespace testsc -use-services-alternate

AEROSPIKE_HOSTS=127.0.0.1:3000 go1.27rc2 run ./sdk/examples/ecommerce/cmd
```

That is the point of the convention: an example no test executes rots silently
as the API moves. See `examples/README.md` for what each one shows.

## 25. Testing

The suite is ginkgo, matching the core client, so the repo's usual invocation
works:

```bash
focus='SDK typed layer' port=3000 make test          # via the Makefile
go1.27rc2 test ./sdk/ -args -h 127.0.0.1 -p 3000 -n test \
    -sc-namespace testsc -use-services-alternate      # directly
```

| Flag | Meaning |
| --- | --- |
| `-h`, `-p`, `-n` | Seed host, port, namespace |
| `-sc-namespace` | A strong-consistency namespace; the transaction and implicit-transaction specs **self-skip** without it |
| `-use-services-alternate` | Alternate service addresses |

Specs self-skip rather than fail when a capability is absent: string operations
below 8.1.3, CDT path expressions below 8.1.1, transactions without a
strong-consistency namespace. Every spec uses a uniquely named set, so runs are
parallel-safe and repeatable.

## 26. Deviations from the other SDKs

| Other SDKs | Go | Why |
| --- | --- | --- |
| Overloads or `impl Into<Target>` | Generic methods over union constraints | One method per verb, compile-time-closed argument set |
| Rust's consuming `self` builders | Pointer builders with a `finalized` guard | Go has no move semantics; re-executing a chain is an error |
| Rust's `Result<Self>` and panicking builder steps | Uniformly deferred errors, surfaced from the terminal | One error path, and the idiomatic Go shape |
| Rust's typestate `PhantomData` markers | Paired concrete builder types | Go generics cannot gate a method on a type parameter's traits |
| Rust's `Deref` on the transactional session | Struct embedding | The closest Go analogue; generic verbs promote through it |
| Rust's weak-reference policy push | Generation-counter revalidation | Go has no weak references; this needs no registration lifecycle |
| `async`/`await` | Blocking calls | Goroutines are the concurrency model; the core client is synchronous |
| Session subclassing | Plain composition | Wrapping a `Session` in your own struct is just Go |
| `context.Context` | Not in v1 | The core client cannot honor cancellation yet, so a parameter would be decorative. Timeouts come from Behavior policies |
| A typed key convertible into a write target | `TypedKey.Key()` unwraps explicitly | A union cannot admit `TypedKey[T]` for a free `T`. Java's `upsert(TypedKey<?>)` also yields an untyped builder |
| One `query` overload set covering typed keys and key lists | `QueryTypedKey` and `QueryTypedKeyList` | Go cannot infer `T` through a `TypedKey[T] \| TypedKeyList[T]` union, so the target-polymorphic shape is unavailable here |

Behavioral parity is preserved where it matters: scope resolution and
inheritance including the child-`All`-beats-parent-specific rule, the error
dispositions and actionable-code rules, the expiration sentinels, durable-delete
resolution order, configuration precedence and fail-soft parsing, the
implicit-transaction gate, and the result and stream semantics.

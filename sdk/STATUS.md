# `sdk` — implementation status

Tracks [DESIGN.md](DESIGN.md) against what is built; [SPEC.md](SPEC.md) is the API
reference. Verified 2026-08-12 against
Aerospike **8.1.3.0-55 Enterprise** (podman `v8.1.3-dev`, port 3000, namespaces
`test` and `testsc`) with the `go1.27rc2` toolchain.

**Build and test**

```bash
go1.27rc2 build ./sdk/
go1.27rc2 vet   ./sdk/
go1.27rc2 test  ./sdk/ -count=1 -sc-namespace=testsc
```

The package is `//go:build go1.27` throughout, so an older toolchain excludes it
cleanly (`build constraints exclude all Go files`) and the core client keeps its
own Go floor — both verified.

## Built and passing

| Area | Files | Notes |
| --- | --- | --- |
| Errors | `errors.go` | 34-kind classification, result-code table, family predicates, `Unwrap` into the core chain |
| DataSet and keys | `dataset.go`, `keyvalue.go` | `KeyValue` constraint (`~string`, all signed ints, `~uint8`/`~uint16`/`~uint32`, `~[]byte` — unsigned stops at 32 bits so no identifier can exceed `MaxInt64` and wrap negative); named types work via the reflection fallback. `DataSet.Key`/`Keys` are generic methods returning no error, since the constraint makes failure unreachable; `DataSet.ID(any)` stays fallible for run-time-typed identifiers |
| Target unions | `targets.go` | `WriteTarget`, `QueryTarget`, `Predicate`, `BinsArg` |
| Behavior | `behavior*.go`, `policy_mapper.go` | 17 scopes, resolution order, child-`All`-beats-parent-specific inheritance, eager 18-entry cache in `atomic.Pointer`, registry, 4 predefined behaviors, generation-counter revalidation |
| Cluster and client | `cluster_definition.go`, `client.go` | Seeds, auth modes, TLS builder, rack awareness, capability probes, metrics passthrough, namespace AP/SC cache |
| Session | `session.go` | Behavior binding, cached point policies, `SessionFor`, fast-path `Get`/`Put`, `Truncate` |
| Chain and verbs | `chain.go`, `verbs.go`, `write_segment.go`, `query_builder.go`, `execute.go` | 8 write verbs plus `Query` as generic methods, segment chaining, deferred errors, single/batch/sequential dispatch, actionable-code and include rules |
| Streams | `record_stream.go`, `record_result.go`, `operation_result.go` | Six sources, `Next`/`Iter`/`Collect`/`Failures`, closing vs non-closing reads, `HasMoreChunks` first-call-true contract |
| Dispositions | `error_strategy.go` | `InStream` and `Handler`, per-segment resolution |
| Bin builders | `bin_builders.go`, `map_return.go` | Scalar, expression, list, map, HLL, bitwise families |
| Dataset queries | `dataset_query.go` | Set-wide scan and index query, partition scoping, chunked cursor |
| UDF | `udf.go` | Two-state function builder, batch UDF result lifting |
| Transactions | `transaction.go`, `implicit_txn.go` | Embedded session, double-finalize guard, `DoInTransaction` with the retryable-conflict set; **every** commit and abort path — explicit, `DoInTransaction`, and implicit batch-write — resolves its verify and roll policies from the session's Behavior |
| Index and info | `index.go` | Index create/drop, `Info`, `InfoOnAllNodes`, `IsNamespaceSC` |
| Config | `config_loader.go`, `config_monitor.go` | YAML schema, precedence, duration grammar, fail-soft parsing, three-gate polling monitor |
| Typed layer | `typed.go`, `unmarshal.go` | `TypedDataSet`/`TypedRecordStream`/`ObjectWriteBuilder`, `as:",key"` tag, `RecordMapper` override, typed verbs as generic methods |
| CDT ranges and inverted terminals | `cdt_ranges.go`, `map_return.go` | Every range and multi-value selector on both bin builders — key/value/index/rank ranges, key and value lists, and the **relative** index and rank ranges — all landing on action builders with no navigation methods; plus `GetAllOther*`, `CountAllOthers`, `RemoveAllOthers`, `RemoveAllOthersAnd`, `GetAsOrderedMap`, `GetAsUnorderedMap`, `GetExists` |
| HyperLogLog | `bin_builders.go`, `cdt_ranges.go` | Init, add, count, describe, `HLLSetUnion`, `HLLFold`, `HLLRefreshCount`, `HLLGetUnion`, `HLLGetUnionCount`, `HLLGetIntersectCount`, `HLLGetSimilarity` |
| CDT navigation | `cdt.go` | Navigable read/write selections (`OnMapKey`, `OnMapIndex`, `OnMapRank`, `OnMapValue`, `OnListIndex`, `OnListRank`, `OnListValue`), deep navigation with create-on-missing, per-key write modes (`SetTo`/`Insert`/`Update`/`Add`), `RemoveAnd()` result builder, and range selections on a type with no navigation methods |
| CDT path expressions | `cdt_path.go` | `OnEachChild`, `OnEachChildWhere`, `CollectValues`/`CollectTree`/`CollectKeys`/`CollectKeysAndValues`, `ModifyBy`, `NoFail`; paired map/list builder types make `CollectKeys` after a list step a compile error |
| String operations | `bin_builders_string.go` | The read and modify families (8.1.3+) on both bin builders |
| Row writes | `row_writes.go` | `UpsertRows` and the four siblings, shared bin names, per-row guards, one segment per row |
| Background tasks | `background.go` | `ExecuteBackgroundTask`, `ExecuteBackgroundDelete`, `ExecuteBackgroundTouch`, `ExecuteUDFBackgroundTask`, with the dataset-only and no-per-record-rows rules enforced eagerly |
| Implicit transactions | `implicit_txn.go` | Five-condition gate, commit/abort per attempt, retry on the transient conflicts |
| Navigatable streams | `navigatable_stream.go` | `PageSize`, `SortBy`, mutating `HasMorePages`, `SetPageTo`, `Remaining`, `CompareValues` with the server's type ordering, plus `TypedNavigatableRecordStream[T]` |
| Lazy streaming | `execute.go` | `Stream()` delivers batch rows over a channel in completion order; dataset queries, sequenced chains and implicit-transaction batches fall back to buffered |
| Admin and typed UDF | `admin.go` | `RegisterUDF`, `RegisterUDFFromFile`, `RemoveUDF`, `ListUDF`, `ListIndexes`, `NamespaceScStatus`, `FirstUDFResultAs[T]`, `UDFResultAsObject[T]` |
| Info objects | `info_objects.go` | `InfoStats` with dash/underscore-flexible lookup, `MergeInfoStats` with sniffed defaults and overrides, `NamespaceDetail`, `SetDetail`, `Sindex`, merged and per-node |
| Tests | `sdk_suite_test.go`, `core_test.go`, `features_test.go`, `ranges_test.go` | **84 ginkgo specs** plus **13 example specs**; `-sc-namespace` gates the transaction and implicit-transaction specs, capability probes gate the rest |
| Docs | `SPEC.md`, `examples/README.md` | Full API reference; the examples README records what each example shows and what porting them found |
| Examples | `examples/` | **All 13 Rust SDK examples ported** (themselves ports of the Java `com.aerospike.examples`), plus `basic`. Each is a package exposing `Run(*exrun.Env) error` with a `cmd/` main; **`examples/examples_test.go` drives every `Run` against a live cluster as its own ginkgo suite**, so an example cannot rot as the API moves |

## Core-client additions made for this SDK

Three gaps in the core client blocked the SDK; all three are now closed in the
core client itself, with tests, rather than worked around:

1. **`ExpAEL(source)`** (`expression_ael.go`) builds a filter expression from
   Aerospike Expression Language source text, packing the two-element form
   `[128, "<source>"]` that server 8.1.3+ compiles. `Expression.IsAEL()`
   discriminates it. The wire format is pinned by unit tests mirroring the Rust
   client's, and verified end-to-end against a live 8.1.3 server.
   **Note:** the source must be packed with `packRawString`, not `packString` —
   the latter prefixes the particle-type byte used for bin values, which the
   server rejects with `PARAMETER_ERROR`.
2. **`CommitWithPolicies` / `AbortWithPolicy`** (`client_txn_policy.go`) accept
   per-call transaction verify and roll policies; `Commit`/`Abort` now delegate
   with nil, so there is one implementation. The SDK resolves these from the
   session's Behavior, so the `SystemTxnVerify` and `SystemTxnRoll` scopes take
   effect per Behavior.
   **Note:** the policies must be resolved *inside* the state switch, not before
   it — the terminal states return without using one, and a caller may hold a
   client whose defaults are unset (`TestAbortBlockedAfterCommitFailedState`
   constructs a bare `&Client{}`).
3. **`MapOrderType` / `MapReturnTypeEnum`** (`cdt_exported_types.go`) are
   aliases exposing the previously unexported types behind the `MapOrder` and
   `MapReturnType` namespace variables, so another package can name them in a
   signature. Aliases, so every existing call site is unaffected.
4. **`Record.OpResults` and `Record.OperationResult(i)`** (`record.go`,
   populated in `record_parser.go` and `batch_command_operate.go`) give an
   operate a positional view of its results, so several operations on one bin are
   all addressable rather than collapsing into the bin map.
   **Note:** only operations that *produce* a value appear. A put or a touch
   sends nothing back, so positions follow the returned results, not the
   request — a put/add/get chain yields one result. The doc comments say so
   explicitly, because the natural assumption is wrong.

## Still open

**Blocks a stable release.** Every file here is `//go:build go1.27` and needs
`go1.27rc2` to build, because the design rests on parameterized methods. The
package cannot ship stable until Go 1.27 is generally available. The core client
keeps its own, lower, version floor, so nothing else is affected.

**Inherited from upstream, not a Go defect.**

- **`ScopeWritesRetryable` resolves but never applies.** Every write is
  classified `OpWriteNonRetryable`, so that scope and its `retryableWrites:`
  config block parse, resolve, populate the cache and appear in `Explain()`
  without affecting a single command. The Rust SDK is in the identical state and
  lists the item as open in its `TODO.md`, with the fix undecided: classify
  retryable writes properly, or remove the scope and its YAML key. This port
  keeps the scope so configuration written for another SDK still loads, and does
  not invent a classification of its own. `SPEC.md` §19 says so in place.

**Rough edges, documented rather than fixed.**

- **A hand-mapped type cannot name its key field `ID`**, because
  `RecordMapper.ID()` would collide with it. Reflection-mapped types are
  unaffected; `examples/README.md` carries the note.

**Not ported.**

- **The tutorial book** — 38 chapters, wired to Rust doc tests so its snippets
  cannot drift from the API. Go has no doc-test mechanism that reproduces that
  guarantee; `examples/` covers some of the same ground as runnable tests.

## Comparison against the Java SDK

`com.aerospike.client.sdk` (in `_debug/aerospike-client-java-sdk`) is **a
complete standalone client, not an ergonomics layer**: 356 files and 95,633
lines, including its own `command/` (65 files), `tend/`, `util/Pack`, `Packer`,
`Unpacker` and `Crypto`, plus its own `Cluster`, `Node`, `Key`, `Value`,
`Record` and `ResultCode`. The package imports nothing from
`com.aerospike.client.*` — only `com.aerospike.ael.*` — and that checkout has no
legacy client beside it.

This port is the other shape: 43 files and 15,412 lines layered over the v8 core
client. Most of the size difference is transport, not ergonomics.

**The overload collapse.** Java's `Session` is 2,198 lines with 106 public
members, carrying 8 overloads for each write verb and 6 for the rest. `verbs.go`
is 100 lines with 9 generic methods. For `upsert`, Java's eight map onto:
`Upsert` (covering `Key`, `List<Key>` and the `Key...` varargs form),
`UpsertRows(ds)`, `UpsertTyped(tds)`, and the three `TypedKey` forms now covered
by `QueryTypedKey`/`QueryTypedKeyList` and `TypedKey.Key()`.

**Closed by this port after the comparison:** `TypedKey<T>` and
`TypedKeyList<T>`, which Java and Rust (`typed.rs`) both have and Go lacked —
`TypedDataSet[T].Key` returned an untyped key, so the entity type was dropped at
the key boundary. See `SPEC.md` §22.

**Absent from Go, deliberately, matching Rust:**

| Java | Size | Why not |
| --- | --- | --- |
| `ael/` — client-side AEL materializer | 42 files | The server compiles AEL |
| `query/plan/`, `QueryHint`, `IndexesMonitor` | 6 files | Index-selection planner |
| `RecordMappingFactory`, `DefaultRecordMappingFactory` | 2 files | Mappers resolve at compile time |
| `SessionExtension` | 1 file | Embedding a `Session` is the whole feature |
| `ExpressionTrace` | 1 file | Resolved server-side |

**Present under other names — parity, not gaps:**
`NavigatableRecordStream` → `navigatable_stream.go`; `BackgroundOperationBuilder`
and `BackgroundTaskSession` → `background.go`; `ChunkedRecordStream` → chunking
inside `record_stream.go`; `BehaviorRegistry` / `BehaviorYamlLoader` /
`BehaviorFileMonitor` → `behavior_registry.go` / `config_loader.go` /
`config_monitor.go`; `TlsBuilder` → `ClusterDefinition.WithTLSConfig`;
`metrics/LatencyType` → `EnableMetrics` / `DisableMetrics`.

**Open questions from the comparison, no decision yet:**

- **`AerospikeComparator` + `AerospikeList<T>` + `AerospikeMap<K,V>`** —
  client-side collections reproducing the *server's* value ordering (strictly by
  type ordinal, with no numeric promotion between INTEGER and DOUBLE), so a
  caller can pre-sort a list before writing it or compare values after reading.
  Go has no equivalent: callers pass `[]any` / `map[any]any` and rely on the
  server. Whether Go wants this is a design question, not an oversight.
- **`SpecialValue{NULL, INFINITY, WILDCARD}`** — Go's CDT ranges take `nil` for
  an open bound, which covers the NULL and INFINITY cases, but the SDK surfaces
  no WILDCARD (the core client has `as.NewWildCardValue()`).
- **`SystemSettingsRegistry`** — a singleton registry with no Go counterpart;
  `system_settings.go` has the settings but no registry.

**Dropped, not deferred.**

- **`SessionExtension`** — the other SDKs need machinery for it because they
  subclass or wrap with traits; in Go, embedding a `Session` in your own struct
  is the whole feature.

## What porting the examples found

Running the 13 Rust examples against the API surfaced fifteen defects the SDK's
own specs had missed, including **four genuine bugs**. Every one is now fixed and
covered by a regression spec; `examples/README.md` carries the full table.

The four bugs, because they are the argument for keeping the examples:

1. **An `Exists` segment poisoned its whole batch.** It was built as a *write*
   carrying a header-read operation, so the server answered `NO_RESPONSE` for
   every row in the batch, not just that segment's. It is now built as a batch
   header read.
2. **Pointer fields crashed on write.** `objectToBins` handed a raw pointer to
   the core client's value conversion, which panics on one — so the natural way
   to model an optional value was a crash. Both directions now handle pointers,
   with a nil pointer meaning an absent bin.
3. **`WithNoBins()` was silently ignored on a set-wide query**, which returned
   whole records. A header-only scan is a policy setting
   (`MultiPolicy.IncludeBinData`), not a projection: an empty bin list reads
   everything, and a header-read *operation* returns no rows at all. Both wrong
   turns were tried before the right mechanism.
4. **A nil bit policy segfaulted.** The core dereferences the policy unguarded;
   the bit builders now default it.

Plus eleven gaps: 19 unwrapped string operations against a `SPEC.md` that
claimed otherwise, three CDT path gaps (no `RemoveMatches`, no
`CollectValuesAsExpressionRead`, and no way to enter a path from a fixed step),
read-side CDT asymmetries, unexported typed mapping (`BinsOf`, `IDOf`,
`ObjectFromRecord`), a nested struct that panicked instead of erroring, and
`Explain()` dropping four fields while rendering enums as integers.

## Behavioral findings worth keeping

- **A header-only read must not carry the user key.** The server returns
  `PARAMETER_ERROR` when `SendKey` is set on a `GetHeader`, and the DEFAULT
  behavior enables `SendKey` because the typed layer needs it to recover keys.
  The SDK originally cleared the flag on a copy of the policy; the core client
  now does this itself in `setReadHeader`, so the SDK-side workaround was removed
  and `sendkey_policy_copy_test.go` pins the core's copy. The finding is kept
  here because the failure is remote from its cause.
- `mapReturnType` was unexported in the core client, so it could not be named in
  an SDK signature. The core now aliases it as `MapReturnTypeEnum`;
  `map_return.go` keeps an SDK-level `MapReturnType` enum anyway, because the
  SDK wants its own documented surface rather than re-exporting the core's.
- **The inverted bit can be applied without naming its type.** `orFlag[T ~int]`
  in `map_return.go` ORs `INVERTED` into a core return-type value with the type
  parameter inferred from the arguments. That works; what does *not* work is a
  helper that merely **returns** the core value, because its result type would
  have to be named. Hence one switch per operation family: the core constant can
  only appear as an argument to the operation constructor.
- **The list *relative* removal constructors take the return type first**
  (`ListRemoveByValueRelativeRankRangeOp(bin, returnType, value, rank, ...)`)
  while their get counterparts take it last. The same inversion as the plain
  range removals below, and just as easy to miss.
- **The map range removal constructors take the return type after the bounds**
  (`MapRemoveByKeyRangeOp(bin, begin, end, returnType, ctx...)`), while the list
  ones take it before (`ListRemoveByValueRangeOp(bin, returnType, begin, end,
  ctx...)`). Getting this backwards compiles when the bounds are `any`, and only
  fails at the type check on the return-type argument.
- **A per-key write mode must not impose a map ordering.** `Insert` and `Update`
  originally built their policies with `KEY_ORDERED`, which silently reordered
  the bin and changed the shape the server returned it in (an ordered map comes
  back as `[]as.MapPair`, not `map[any]any`). They now use `UNORDERED`: the
  write mode is orthogonal to the ordering.

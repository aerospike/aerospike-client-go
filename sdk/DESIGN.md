# Design: `sdk` — the high-level Aerospike Go SDK

Status: **implemented** — see [STATUS.md](STATUS.md) for as-built detail and [SPEC.md](SPEC.md) for the API reference · Date: 2026-08-12

A port of the Rust `aerospike-sdk` ergonomics layer (itself the Rust counterpart of the
Python `aerospike_sdk` and Java `com.aerospike.client.sdk` packages) to Go, layered on
the existing v8 core client. Reference specification:
`_debug/aerospike-client-rust/aerospike-sdk/SPEC.md`.

This design uses **generic methods over union constraints** — the direct Go analogue of
Rust's `impl Into<Target>` — via the Go 1.27 parameterized-methods feature. Every 1.27
behavior the design rests on was verified empirically on `go1.27rc2` (see
[Verified 1.27 semantics](#verified-127-semantics-the-design-rests-on)).

---

## Locked decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Location | `github.com/aerospike/aerospike-client-go/v8/sdk` subpackage. Module go.mod stays `go 1.23`; **every sdk file carries `//go:build go1.27`**, so the SDK needs a 1.27+ toolchain while the core client keeps its 1.23 floor. Verified: older-toolchain consumers importing `sdk` get a clear "build constraints exclude all Go files" error; the core client is unaffected |
| 2 | Context | No `context.Context` in v1; timeouts/retries come from Behavior policies. Added when the core client supports ctx |
| 3 | Typed engine | Reflection via `as:`/`asm:` tags + new `as:",key"` option; `RecordMapper` interface overrides reflection when implemented |
| 4 | Scope | Full nine-phase port, one continuous effort |
| 5 | Target polymorphism | **Generic methods with union constraints**, mimicking Rust's `Into<WriteTarget>`/`Into<QueryTarget>`/`Into<Predicate>`/`Into<Bins>` — one method per verb, compile-time-closed argument sets. No variadics, no `any`, no wrapper interfaces |
| 6 | Typed surface | Generic methods only (`session.QueryTyped(ds)`); the whole package assumes Go 1.27 |

## Object model

```text
ClusterDefinition ──Connect()──▶ Cluster ──CreateSession(behavior)──▶ Session
                                    │                                   ├── Get/Put                (fast path)
                                    └── Client() ──▶ sdk.Client         ├── Upsert/Insert/Update/… ▶ WriteSegmentBuilder ▶ WriteBinBuilder ▶ Cdt*Builder
                                          │                             ├── UpsertRows(ds)         ▶ RowWriteBuilder
                                          └── UnderlyingClient()        ├── Query(key|keys|ds)     ▶ QueryBuilder ▶ QueryBinBuilder
                                              ▶ *as.Client (escape)     ├── ExecuteUDF(key|keys)   ▶ UdfFunctionBuilder ▶ UdfBuilder
                                                                        ├── Index(ds)              ▶ IndexBuilder
                                                                        └── Transaction()          ▶ TransactionalSession
                                                                                    │
                                              Execute()/Stream() ──────────────────▶ RecordStream ▶ RecordResult
                                                                                    │
                                              IntoNavigatable() ────────────────────▶ NavigatableRecordStream
```

## The target unions

```go
type WriteTarget interface { *as.Key | []*as.Key }
type QueryTarget interface { *as.Key | []*as.Key | *DataSet }
type Predicate   interface { *as.Expression | string }   // string = AEL text, server-compiled (8.1.3+)
type BinsArg     interface { []string | Bins }            // Bins: sdk.AllBins / sdk.NoBins sentinels
```

Session verbs collapse to one method each, exactly as in Rust:

| Rust | Go |
|---|---|
| `upsert(impl Into<WriteTarget>)` ×8 verbs | `Upsert[T WriteTarget](t T)` ×8 — `Upsert(key)`, `Upsert(keys)` both compile; `Upsert(42)` does not |
| `query(impl Into<QueryTarget>)` | `Query[T QueryTarget](t T)` — key, keys, or dataset; `DataSetOf` covers Rust's `(ns, set)` tuple |
| `execute_udf(impl Into<WriteTarget>)` | `ExecuteUDF[T WriteTarget](t T)` |
| `where_(impl Into<Predicate>)` | `Where[P Predicate](p P)` — typed expression or AEL text in one method |
| `get(&key, impl Into<Bins>)` | `Get[B BinsArg](key *as.Key, bins B)` — `[]string{"name"}`, `sdk.AllBins`, `sdk.NoBins` |

The same pattern applies to builder segment transitions (`.Delete(...)`, `.Query(...)`,
`.ExecuteUDF(...)` on `WriteSegmentBuilder`/`QueryBuilder`/`UdfBuilder`) and
`DefaultWhere`.

Two union limits, handled the way Rust handles them with `Into` impls it cannot write:
union terms cannot be parameterized, so `TypedKey[T]` unwraps via `.Key()` before
entering untyped verbs, and `Truncate` takes `*DataSet` with `TypedDataSet[T].DataSet()`
as the bridge.

## Surface sketch

```go
import (
    as  "github.com/aerospike/aerospike-client-go/v8"
    sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

cluster, err := sdk.NewClusterDefinition("localhost", 3000).
    WithNativeCredentials("admin", "secret").
    Connect()
defer cluster.Close()

session, err := cluster.CreateSession(nil)          // nil → Behavior DEFAULT

users, _ := sdk.DataSetOf("test", "users")
key      := users.Key("user-1")     // constrained to KeyValue, so it cannot fail

// Fast path — one round trip, no builder, no stream.
err  = session.Put(key, as.BinMap{"name": "Ada", "age": 36})
rec, err := session.Get(key, sdk.AllBins)
proj, err := session.Get(key, []string{"name"})

// One verb, both cardinalities — the Rust shape.
stream, err := session.Upsert(key).
    Bin("counter").SetTo(100).
    Bin("counter").Add(11).
    Bin("counter").Get().
    Execute()
defer stream.Close()

stream, err = session.Update(k1).Add("counter", 5).
    Delete([]*as.Key{k2}).
    Insert(k3).SetTo("status", "new").
    Execute()                                        // one batch, three segments

// CDT navigation — compile-time-safe like Rust's six builder types.
stream, err = session.Update(key).
    Bin("prefs").OnMapKey("theme", nil).Insert("dark", true).
    Bin("scores").OnListIndexRange(0, 2).RemoveAnd().Count().
    Execute()

// One Where, both predicate forms.
stream, err = session.Query(users).
    Where(as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(25))).
    ChunkSize(100).
    Execute()
stream, err = session.Query(users).
    Where("$.age >= 25 and $.status == 'active'").   // AEL, version-gated client-side
    Execute()

for res := range stream.Iter() { ... }               // or pull: res, err := stream.Next()
nav, err := stream.IntoNavigatable()
nav.PageSize(20).SortBy(sdk.Descending("age"))

// Typed layer — generic methods, inference end to end.
type Customer struct {
    ID   int64  `as:",key"`
    Name string `as:"name"`
    Age  int64  `as:"age"`
}
customers := sdk.TypedDataSetOf[Customer]("test", "customers")
_, err      = session.UpsertTyped(customers).Object(&alice).Execute()
people, err := session.QueryTyped(customers).Where("$.age > 25").Execute().IntoObjects()
c, err      := stream.FirstUDFResultAs[Customer]()   // explicit instantiation — verified

// Transactions — generic methods promote through the embedded *Session (verified).
tx, err := session.Transaction()
_, err   = tx.Upsert(a).SetTo("balance", 100).Execute()
status, err := tx.Commit()
err = session.DoInTransaction(func(tx *sdk.Session) error { ... }, 5, 50*time.Millisecond)
```

## Verified 1.27 semantics the design rests on

All confirmed empirically on `go1.27rc2` (probe programs were run during design review):

1. Generic methods with union constraints + type switch inside — the verb pattern.
2. Inference from arguments through full fluent chains
   (`session.QueryTyped(ds).Where(…).Execute()`).
3. Explicit instantiation on methods (`stream.FirstUDFResultAs[Customer]()`).
4. Method values and method expressions with instantiation.
5. **Promotion through struct embedding** — `TransactionalSession{*Session}` gets every
   generic verb, including with explicit instantiation.
6. Per-file language gating: `//go:build go1.27` files in a `go 1.23` module compile
   under 1.27 and are cleanly excluded below it; a package whose files are all gated
   produces a clear "build constraints exclude all Go files" error for old-toolchain
   consumers.
7. Restrictions accepted: interfaces **cannot** declare generic methods
   (`interface method must have no type parameters`) — `Session` stays a concrete type
   and cannot be mocked behind an interface that includes the verbs; generic methods are
   invisible to `reflect` (`reflect.NumMethod() == 0`) — irrelevant here, reflection is
   only used on user record structs.

Method generics are a plain language feature in 1.27 (no `GOEXPERIMENT` flag), gated by
the file's language version.

## Core-client additions this design required

Four gaps in the core client were closed as part of the work, rather than worked
around in the SDK (each with tests):

1. **`ExpAEL(source)` and `Expression.IsAEL()`** — a filter expression from
   Aerospike Expression Language source text, packing the two-element
   `[128, "<source>"]` form that server 8.1.3+ compiles. The source must be
   packed with `packRawString`, not `packString`: the latter prefixes the
   particle-type byte used for bin values, which the server rejects.
2. **`CommitWithPolicies` / `AbortWithPolicy`** — per-call transaction verify and
   roll policies, so the `SystemTxnVerify` and `SystemTxnRoll` scopes take effect
   per Behavior. `Commit`/`Abort` delegate with nil.
3. **`MapOrderType` / `MapReturnTypeEnum`** — aliases exposing the previously
   unnameable types behind the `MapOrder` and `MapReturnType` namespace
   variables, so another package can declare a parameter of that type.
4. **`Record.OpResults` and `Record.OperationResult(i)`** — a positional view of
   an operate's results, so several operations on one bin are all addressable.
   Only operations that *produce* a value appear, so positions follow the
   returned results rather than the request.

## Risk statement

This design bets the primary API on an RC-status language feature. If the feature's
semantics shift between rc2 and GA (or a GA-blocking issue pulls it), the SDK cannot
ship stable until resolved. Mitigation is structural: all implementations live in plain
non-generic functions operating on the `chain` core; the generic methods are thin
dispatch wrappers (type switch → internal call), so any GA adjustment touches a small,
mechanical layer. The integration suite runs under the release toolchain in CI the
moment 1.27 goes final.

## Subsystem designs

**Chain core.** One unexported `chain` struct accumulating `[]operationSpec` segments
(keys, ops, verb, TTL, generation, filter, durable-delete, UDF triple); every public
builder — `WriteSegmentBuilder`, `QueryBuilder`, `UdfBuilder`, bin and CDT builders — is
a thin view over it, exactly as in Rust. Multi-segment chains execute as one batch
unless a UDF segment forces sequential runs. TTL sentinels (−1 never / −2 no-change /
0 server default) and the `OpType → RecordExistsAction` mapping port verbatim.

**Deferred errors, uniformly.** Every chaining method and session verb returns the
builder; the first argument error (`pendingErr`) surfaces at `Execute()`/`Stream()`.
This covers what Rust splits across `Result<Self>` methods, verb `Result<Builder>`
returns, and runtime panics (e.g. `SetTo` off a non-map-key CDT selector). A `finalized`
flag makes double-execution and touching a finalized segment deferred errors too.
Generic verbs add nothing here: a wrong argument type is a compile error rather than a
deferred one.

**RecordStream.** Pull-based `Next() (*RecordResult, sdk.Error)` — `(nil, nil)` means
exhausted; cluster failures come from `Next`, per-record failures ride as data on the
row (`ResultCode`, `InDoubt`, `Exception`). Plus `Iter() iter.Seq[*RecordResult]` for
`range`, `Collect`, `Failures`, closing `First`/`FirstOrRaise` vs non-closing
`Pop`/`PopOrRaise`, `FindRecord`, `FirstUDFResult`, and explicit `Close()` (documented
`defer`). Six internal sources (single/list/chain/batch/query/chunked); batch streams
deliver in completion order with `Index` mapping back to input order;
`HasMoreChunks()` keeps the first-call-always-true contract so one loop shape serves
chunked and non-chunked queries alike.

**Error dispositions.** `ExecuteOnError(onErr)` / `StreamOnError(onErr)` with
`sdk.InStream()` or `sdk.Handler(func(key *as.Key, index int64, err sdk.Error))`.
Resolution per segment: handler wins; else in-stream for batch/explicit; else single-key
errors return from the terminal. Actionable-code rules preserved: `KEY_NOT_FOUND` is
fatal only for Update/ReplaceIfExists, `FILTERED_OUT` only under `FailOnFilteredOut()`,
delete always publishes not-found rows, reads need `IncludeMissingKeys()`.

**CDT navigation.** Six concrete builder structs generic over the parent
(`CdtWriteBuilder[P opSink]`, …) reproduce Rust's capability lattice: range/multi-value
selections land on action builders that have no `On*` methods — invalid navigation does
not compile. `RemoveAnd()` returns a `CdtRemoveResultBuilder`. Path expressions
(`OnEachChild`, `OnEachChildWhere`) use paired concrete types instead of Rust's
`StepKind` phantom marker: list-steps return a variant without
`CollectKeys`/`CollectKeysAndValues`; map/any steps return the full one. Map-key-only
write terminals (`SetTo`/`Insert`/`Update`/`Add`) validate at execute time via deferred
error.

**Behavior.** Immutable named policy bundles; 17 scopes; `Settings` with pointer fields
as the `Option` analogue; per-field merge; resolution order
`All → Reads/Writes → mode → (retryability) → shape`, system scopes resolving as
`[All, scope]` only; **inheritance layers a child's patches over the parent's
fully-resolved matrix** (child-`All` beats parent-specific — Python parity). Eager
18-entry cache in `atomic.Pointer`; global registry; predefined DEFAULT / READ_FAST /
STRICTLY_CONSISTENT / FAST_RACK_AWARE with the exact factory patch sets. Hot-reload
propagation deviates from Rust's weak-ref push: each Behavior carries an atomic
generation counter; sessions revalidate their cached four-policy snapshot on a counter
change (one atomic load on the fast path, no registration lifecycle, GC-friendly).
Policy mapping (`toReadPolicy`, `toWritePolicy`, `toQueryPolicy`, batch/txn variants,
`resolveDurableDelete`) ports 1:1.

**Session.** Cheap handle binding a Behavior; pre-resolved AP/SC point read/write
policies; per-namespace AP/SC mode cache on the client; `SessionFor(behavior)` for
siblings. Fast-path `Get`/`Put` bypass builders and streams entirely. Session extensions
need no machinery in Go — embed/wrap `Session` yourself; `CreateSessionWith` is dropped.

**Transactions.** `TransactionalSession` embeds `*Session` (promotion of generic verbs
verified); `Commit`/`Abort`/`Rollback` guard double-finalization with an atomic flag;
verify/roll policies resolve from the `SystemTxnVerify`/`SystemTxnRoll` scopes per call.
`DoInTransaction(fn, maxAttempts, sleep)` retries on
`MRT_BLOCKED`/`MRT_VERSION_MISMATCH`/commit failure. Implicit batch-write MRTs keep the
five-condition gate (SC namespace, has writes, no explicit/opted-out txn, cluster-wide
MRT support, setting enabled — default on, 5 attempts, 1s between attempts).

**Errors.** `sdk.Error` wraps the core error (`Unwrap()` preserves `errors.Is/As`),
adds `Kind()` (34 `ErrorKind` values via the result-code table), family predicates
(`IsSecurity()`, …), `InDoubt()`, and extended detail (`SubCode()`, `ServerMessage()`,
`ExpTrace()`) gated by `errorDetailVerbosity` on 8.1.3+ servers. `SubCode` is a
constants block, not a type — subcodes are scoped to their parent result code.

**Config file + hot reload.** Same env var (`AEROSPIKE_SDK_CONFIG_URL`), same camelCase
YAML vocabulary (`system:` profiles, `behaviors:` with `parent:`), same precedence
(file cluster-profile > file DEFAULT > programmatic > hard defaults), same duration
grammar, fail-soft parsing, `maximumNumberOfCallAttempts = maxRetries + 1`. Monitor
goroutine polls ~1s with three gates (mtime → raw bytes → resolved equality), keeps
last-good on failure, stops on `Cluster.Close()`. Connection-pool/tend/circuit-breaker
sizing remains connect-time-only; `behaviors:` is fully live.

**Info.** `session.InfoCommands()` with raw (`Info`, `InfoOnAllNodes`) and typed
accessors in merged and `PerNode` forms; `InfoStats` (dash/underscore-flexible lookups),
`MergeStrategy` (sum/average/AND/most-common defaults plus the Java annotation
overrides), `NamespaceDetail`/`SetDetail`/`Sindex`/`SindexDetail` with their specific
merge rules (e.g. merged sindex state is WriteOnly while any node populates).

**Navigatable streams.** `IntoNavigatable()` drains and closes the stream; `PageSize`,
`SortBy(props...)` (replace-and-resort, stable sort), mutating `HasMorePages()`,
page-gated `HasNext()`/`Next()`, `SetPageTo` (1-based), `Reset`, `Remaining`.
`CompareValues` implements the server type ordering
(`NIL < BOOLEAN < INTEGER < STRING < LIST < MAP < BYTES < DOUBLE < GEOJSON`), no
cross-type numeric promotion, HLL compared as bytes, case-insensitivity applied at every
nesting level; error rows sort as missing bins.

**Typed layer.** `TypedDataSet[T]`, `TypedKey[T]`, `TypedQueryBuilder[T]` (no bin
projection — the mapper needs all bins), `TypedRecordStream[T]` (`NextObject`,
`IntoObjects`, `FirstObject`, `GetObject`, `NextObjectWithMetadata`, `Failures`,
`IntoNavigatable`), `ObjectWriteBuilder[T]` (per-object guards apply to the most recent
`Object()`; one write segment per object; insertion-order results). Mapping: reflection
engine reusing the core marshal machinery — `as:"name"`, `as:",key"` (new; key not
stored as a bin, recovered from the key's user value on reads — requires `send_key`, on
in Behavior DEFAULT), `asm:"gen"`, `as:"-"`, `,omitempty`, `,inline`. If `*T` implements
`RecordMapper` (`ToBins() (as.BinMap, error)`,
`SetFromRecord(bins as.BinMap, key *as.Key, gen uint32) error`, `ID() as.Value`), it
overrides reflection. All typed verbs are session methods: `QueryTyped`, `UpsertTyped`,
`InsertTyped`, `UpdateTyped`, `ReplaceTyped`, `ReplaceIfExistsTyped`.

## Not ported (same non-goals as Rust)

Client-side AEL parsing (the server compiles AEL on 8.1.3+; the client gates on
`SupportsServerCompiledAEL()` before sending), `AsyncPool`, index-metadata monitor,
stream-UDF aggregation, `SessionExtension`/`CreateSessionWith` (plain Go composition
suffices).

## Implementation plan

| Phase | Contents |
|---|---|
| 1. Foundation | package skeleton (all files `//go:build go1.27`); `Error`/`ErrorKind`/`SubCode`; `DataSet`; target-union types; `ClusterDefinition`/`TlsBuilder`/`Cluster`/`Client`; Behavior core (scopes, Settings, resolution, predefined, registry, policy mapper); `Session` + fast-path `Get`/`Put`; AP/SC namespace cache |
| 2. Chain | `chain`/`operationSpec`; the 8 generic verbs + `Query` + transitions; `WriteSegmentBuilder`; `QueryBuilder` (point/batch); `Execute`/`Stream`; `RecordStream` (six sources); `RecordResult`/`OperationResult`; dispositions |
| 3. Bin ops | `WriteBinBuilder`/`QueryBinBuilder`: scalar, expression, list, map, HLL, bitwise, string ops; `HllConfig` |
| 4. CDT | six navigation builders; `CdtRemoveResultBuilder`; path-expression builders |
| 5. Queries+ | dataset queries, index filters, partitions, chunked iteration; `NavigatableRecordStream` + `CompareValues`; row writes; UDF builders; `IndexBuilder`; background tasks |
| 6. Txn & ops | `TransactionalSession`; `DoInTransaction`; implicit MRT; capability probes; info commands + typed info objects; metrics passthrough |
| 7. Config | `SystemSettings`; YAML loader + precedence; hot-reload monitor |
| 8. Typed | `,key` tag; `RecordMapper`; generic typed family as session methods; typed navigatable |
| 9. Spec & tests | Go `SPEC.md`; examples; ginkgo integration suite mirroring the Rust test areas (unique set names, capability self-skip), run via `focus='…' port=3100 make test` with the `go1.27rc2` toolchain |

Each phase lands with unit tests plus ginkgo integration tests following the existing
repo conventions.

# `sdk` Examples

Ports of the Rust SDK's examples, which are themselves ports of the Java SDK's
`com.aerospike.examples`. Each example is a package exposing
`Run(*exrun.Env) error` plus a `cmd/` main that runs it standalone; the test in
this directory drives every `Run` against a live server, so every example is
exercised on each test run and cannot silently rot as the API evolves.

```bash
# All of them, as tests.
go1.27rc2 test ./sdk/examples/ -args -h 127.0.0.1 -p 3000 -n test \
    -sc-namespace testsc -use-services-alternate

# One of them, standalone.
AEROSPIKE_HOSTS=127.0.0.1:3000 go1.27rc2 run ./sdk/examples/ecommerce/cmd
```

Every example owns its set names in the `test` namespace and truncates them on
entry, so runs are repeatable and set names do not accumulate on the server.

## Available examples

| Example | Java source | Shows |
| --- | --- | --- |
| `batch` | `BatchExample` | Multi-key insert, then one chain mixing insert / update-add / delete |
| `studentscores` | `StudentScoresExample` | Map bins plus a server-compiled AEL filter over map values |
| `stringops` | `StringOperationsExample` | Server-side string operations through the bin builders, as raw operations, and as expressions |
| `mapremoverange` | `MapRemoveByKeyRangeTest` | A map removal used as a *read* expression under six return types |
| `typedmapping` | `TypedMappingExamples` | Both mapping routes — reflection over struct tags, and a hand-written `RecordMapper` — with typed writes, typed queries and a heterogeneous batch |
| `behaviorhierarchy` | `BehaviorHierarchicalExample` | Deriving behaviors, inspecting resolved settings per (kind, shape, mode), `Explain()` |
| `cdtpath` | `CdtPathExpressionExample` | The path builders: filtered each-child reads, `ModifyBy`, `RemoveMatches`, expression reads |
| `txnprocessing` | `TransactionProcessingExample` | A posting as one chained multi-record write, then the same work inside a transaction with rollback |
| `opdifferences` | `OperationDifferences` | AEL semantics as the *server* compiles them: map keys, shifts, `exists()`, casting |
| `roster` | `RosterExample` | The `roster:` info command, `roster-set` and `recluster:` across nodes |
| `ecommerce` | `ecommerce/EcommerceExample` | A domain model: typed bulk load, concurrent reads, one batch spanning three sets, error dispositions, dashboards, a background price pass |
| `yamlconfig` | `BehaviorYamlExample`, `YamlConfigExample`, `CompleteYamlConfigExample`, `YamlConfigConnectionExample` | The configuration file: `system:` and `behaviors:` sections, connecting from file configuration, hot reload |
| `queryexamples` | `QueryExamples` | The broad tour: seeding, batches, expressions, sorting and pagination, expirations, CDT, bit operations, background queries |
| `basic` | — | Not a port: a short tour of the fast path, builder chain, batch, row writes, sorting and the typed layer |

## Notes on the ports

- **`yamlconfig` consolidates four Java examples**, as the Rust port does. The
  configuration path comes from `AEROSPIKE_SDK_CONFIG_URL`, which is
  process-global, so four examples each setting it would interfere with one
  another. It also connects its own cluster rather than using the suite's,
  because the file is read once, at connect.
- **Rust's `#[derive(RecordMapper)]` has no Go counterpart.** Where the Rust
  ports derive a mapper, the Go ports map by struct tag (`as:",key"`,
  `as:"name"`, `as:"-"`, `asm:"gen"`), and `typedmapping` and `ecommerce` also
  show the `RecordMapper` interface for a type that wants control.
- **Rust's `async`/`await` becomes goroutines.** Where the Rust example reads
  concurrently, `ecommerce` uses an `errgroup` over one session.
- **Skipped, as out of scope for this SDK:** client-side AEL materialization and
  the index-selection planner, which is also why `opdifferences` ports only the
  checks whose behavior is observable through server-compiled AEL.
- **Capability gating.** `stringops` needs server 8.1.3+, `cdtpath` needs
  8.1.1+, and the AEL filters in `studentscores`, `opdifferences`,
  `queryexamples` and `ecommerce` need 8.1.3+. Each prints a skip line and
  returns cleanly on an older cluster, so the suite stays green.
- **Strong consistency.** `txnprocessing` needs a strong-consistency namespace
  for its transaction half and `roster` for its write half; both skip cleanly
  otherwise. Supply `-sc-namespace` (or `AEROSPIKE_SC_NAMESPACE` when running
  standalone).

## What porting these found

Running someone else's programs against the API surfaced eleven gaps that the
SDK's own tests did not, which is the argument for keeping them:

| Found by | Gap | Closed |
| --- | --- | --- |
| `stringops` | Only 3 of 22 string *modifications* were wrapped; `SPEC.md` claimed "and the rest" | All 43 core string operations now wrapped; the spec enumerates them |
| `stringops` | `StrFindNth`, `StrIsNumericTyped`, `StrRegexCompareWithFlags`, `StrToString` missing from the read-side builder | Added |
| `cdtpath` | No `RemoveMatches()` terminal | Added |
| `cdtpath` | A path chain could only start at the bin root, so `Bin(x).OnMapKey(k).OnEachChild()` was inexpressible | `OnEachChild`/`OnEachChildWhere` added to the navigable CDT builders |
| `cdtpath` | No `CollectValuesAsExpressionRead` terminal | Added |
| `ecommerce` | A typed entity could not join a mixed batch: the mapping was unexported | `BinsOf[T]` and `IDOf[T]` exported |
| `ecommerce` | `OnMapRank` / `OnMapValue` / `OnListRank` missing from the read-side builder | Added |
| `ecommerce` | `CdtReadBuilder` lacked `GetKeysAndValues`, `GetIndexes`, `GetRanks` | Added |
| `typedmapping` | No per-row mapping entry point for a heterogeneous batch | `ObjectFromRecord[T]` exported |
| `typedmapping` | `Behavior.Explain()` silently dropped four settings fields, and rendered enums as raw integers | Fields added; enums render by name |
| `typedmapping` | **Pointer fields crashed on write** and failed on read, so an optional value could not be modeled | Both directions handle pointers; a nil pointer means an absent bin |

Two remaining rough edges are documented rather than fixed: a hand-mapped type
cannot name its key field `ID`, because `RecordMapper.ID()` would collide with
it; and `ScopeWritesRetryable` resolves but never applies, so the write tuning
the Java hierarchy puts there has no effect in Go — `behaviorhierarchy` says so
in a comment rather than quietly moving the scope.

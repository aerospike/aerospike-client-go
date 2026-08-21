# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The official Aerospike Go client (`github.com/aerospike/aerospike-client-go/v8`). It implements the Aerospike wire protocol directly (no C client dependency), is fully goroutine-safe, and is distributed as a single large `package aerospike` at the repo root, with a handful of supporting packages (`types`, `logger`, `config`, `internal/*`).

Current branch `v8` is the main development branch for the v8 major version. Go 1.23+ is required (see `go.mod` / `.tool-versions`).

## Build tags

The codebase uses build tags to produce slimmed-down variants. Any change to reflection-based code, Lua/UDF code, or `unsafe` usage must be compatible with these:

- `as_performance` — strips the reflection-based Object API (`*_reflect.go` files, `marshal.go`, `value_reflect.go`). Verify with: `go build -tags as_performance .`
- `app_engine` — strips `unsafe` package usage and Lua/UDF aggregation support (`internal/lua/*`, `query_aggregate_command.go`, `client_appengine_exclusions.go`) for Google App Engine compatibility. Verify with: `go build -tags app_engine .`
- `as_proxy` — used when building tools (e.g. `tools/benchmark`) against the Aerospike proxy.
- `multinode` — gates some tests to only run against a multi-node cluster.
- `tools` — gates `tools.go`, which pins tool dependencies (`gocovmerge`) without importing them into the main build.

When adding a new file to the reflection-based Object API or to Lua/UDF support, add the matching `//go:build` constraint so both slimmed builds keep compiling.

## Common commands

Build:
```
go build .
go build -tags as_performance .   # without reflection/Object API
go build -tags app_engine .       # without unsafe / Lua
```

Tests use Ginkgo/Gomega and require an install of the `ginkgo` CLI (`go install github.com/onsi/ginkgo/v2/ginkgo`). Most test files live at the repo root in `package aerospike_test` and talk to a real Aerospike server — **a running server is required for most of the suite** (default `127.0.0.1:3000`, override with `-hosts` or `-h`/`-p`).

Run the full suite against a local server:
```
ginkgo -r -race
```

Run the root-package suite only, with common flags (mirrors CI):
```
go run github.com/onsi/ginkgo/v2/ginkgo -race -keep-going -succinct -randomize-suites -skip="HyperLogLog" -- -hosts=127.0.0.1:3000
```

Run a single spec/file by focusing it (Ginkgo focus, not `go test -run`):
```
ginkgo -r --focus "the specific Describe/It text" -- -hosts=127.0.0.1:3000
```
or set `ginkgo.FDescribe`/`ginkgo.FIt` temporarily in the test file.

Test-only packages that don't need a live server (CI runs these separately):
```
ginkgo -r internal/lua
ginkgo -r types
ginkgo -r pkg
```

Useful test flags (see `aerospike_suite_test.go` for the full flag list): `-hosts`, `-h`/`-p`, `-U`/`-P` (auth), `-A` (auth mode), `-n` (namespace, default `test`), `-use-replicas`, `-use-services-alternate`, `-debug`, and TLS flags (`-cert_file`, `-key_file`, `-key_file_passphrase`, `-node_tls_name`, `-root_ca`).

Coverage thresholds/exclusions are defined in `.testcoverage.yml` (checked via `vladopajic/go-test-coverage` in CI, profile `cover_all.out` produced by merging `covprofile_*.out` with `gocovmerge`).

Building the auxiliary tools:
```
cd tools/benchmark && go build -tags as_proxy -o benchmark .
cd tools/asinfo && go build -o asinfo .
cd tools/cli && go build -o cli .
```

Building examples (each is a standalone `main`):
```
find examples -name "*.go" -type f -print0 | xargs -0 -n1 go build
```

## Architecture

### Cluster/node layer
- `cluster.go` — `Cluster` owns cluster membership: seed hosts, node discovery/tending (background goroutine), partition maps, and metrics. Talks to nodes via `peers.go`/`peers_parser.go` (parses the `peers` info command) and `partition_parser.go`/`partitions.go`/`partition.go` (partition-to-node maps, used for smart client routing).
- `node.go` — a single cluster node: connection pool management, health/error-rate tracking, info-command execution.
- `connection.go`/`connection_heap.go`/`buffered_connection.go` — TCP connection lifecycle and per-node pooling.
- `node_validator.go` — validates a node during discovery (auth handshake, TLS name, feature detection).

### Command layer (wire protocol)
- `command.go` is the largest file in the repo — it builds and parses the Aerospike wire protocol buffers for essentially every operation type. Most `*_command.go` files (e.g. `read_command.go`, `write_command.go`, `batch_command_*.go`, `query_*_command.go`, `scan_*_command.go`, `execute_command.go`, `txn_*.go`) embed/compose the logic in `command.go` and add operation-specific request building + response parsing.
- `single_command.go`, `multi_command.go`, `base_read_command.go`, `base_write_command.go` are shared base types that concrete commands build on.
- `batch_node.go`/`batch_node_list.go`/`batch_offsets.go`/`batch_executer.go` handle splitting a batch request across nodes based on partition ownership, then recombining results.

### Public API surface
- `client.go` is the main entry point (`Client`) — CRUD, batch, scan, query, UDF execute, admin operations. It largely delegates to the command layer.
- `client_reflect.go`/`read_command_reflect.go`/`batch_command_*_reflect.go`/`marshal.go`/`value_reflect.go`/`packer_reflect.go` implement the reflection-based "Object API" (`*Object` methods, e.g. `PutObject`/`GetObject`) — all gated by `!as_performance`.
- `value.go` defines the `Value` interface hierarchy used to box Go values for the wire protocol; `packer.go`/`unpacker.go` handle MessagePack-style encoding for CDT (list/map) values.
- Policies (`policy.go`, `client_policy.go`, `batch_policy.go`, `write_policy.go`, `scan_policy.go`, `query_policy.go`, `txn_*_policy.go`, etc.) are plain structs controlling per-call behavior (timeouts, consistency, retry).
- `expression.go`/`exp_*.go` and `filter.go` implement the server-side filter expression DSL used in policies and secondary-index queries.
- `cdt.go`/`cdt_list.go`/`cdt_map.go`/`cdt_bitwise.go`/`cdt_context.go`/`cdt_operation.go` implement Complex Data Type (list/map/bitwise) sub-operations for `Operate`.
- `hll_operation.go`/`exp_hll.go` — HyperLogLog operations.

### Multi-record transactions (MRT)
- `txn.go` is the client-side transaction state; `txn_monitor.go`, `txn_roll*.go`, `txn_verify*.go`, `txn_close.go`, `txn_mark_roll_forward.go`, `txn_add_keys_command.go` implement the commit/abort/roll-forward/roll-back protocol against the server-side transaction monitor.

### Dynamic configuration
- `config/` is a separate, independently-versioned config subsystem: `config/dynconfig.go` defines a YAML-serializable `Config` (static + dynamic sections mirroring client/read/write/query/scan/batch/txn/metrics policies), `config/provider/` loads it (e.g. from a file), and `config/registry/registry.go` presumably wires it into the running client. This is distinct from the per-call `*Policy` structs and is meant for live-reloadable settings.

### Supporting packages
- `internal/atomic/` — atomic/typed-value/guarded-map primitives used throughout the cluster/node layer for lock-light concurrent state.
- `internal/lua/` — embeds `github.com/yuin/gopher-lua` to run user-defined stream aggregation UDFs (`query_aggregate_command.go`); excluded under `app_engine`.
- `internal/seq/`, `internal/version/` — small internal helpers (sequence numbers, client version string sent to the server).
- `types/` — wire-level constants and shared value types: `result_code.go` (server error/status codes — keep in sync with `error.go`), `particle_type/` (bin value type tags), `pool/`, `histogram/`, `rand/`.
- `pkg/bcrypt`, `pkg/ripemd160` — vendored crypto primitives used for user/password auth hashing (avoids extra external deps).
- `logger/` — pluggable logger facade (`asl.Logger.SetLogger`/`SetLevel`) used across the client instead of a hard dependency on a specific logging library.

### Tests
- Nearly all root-package tests are Ginkgo specs in `package aerospike_test`, bootstrapped from `aerospike_suite_test.go` (`TestAerospike` → `RunSpecs`), and require a live Aerospike server — there is no mock server in this repo. `bench_*_test.go` files are Go benchmarks (`go test -bench`), not Ginkgo specs.
- `test/resources` holds fixture files (e.g. UDF Lua scripts, certs) referenced by the specs.
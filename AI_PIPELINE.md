## Overview

aerospike-client-go is the official Aerospike Go client library. It provides a full-featured client for interacting with Aerospike clusters, including CRUD operations, batch operations, queries, scans, UDFs, and secondary index management.

The library supports two modes via build tags:
- **Default mode** — full feature set including reflection-based object marshaling/unmarshaling (`PutObject`, `GetObject`, etc.)
- **Performance mode** (`as_performance` build tag) — disables reflection-based APIs for lower allocation overhead

## Build

```bash
go build ./...
```

## Test

Tests use [Ginkgo v2](https://onsi.github.io/ginkgo/) with Gomega matchers. Integration tests require a running Aerospike server.

```bash
# Run all tests against a local Aerospike server
go test ./... -h 127.0.0.1 -p 3000 -n test -timeout 30m

# Run tests matching a specific pattern
go test ./... -h 127.0.0.1 -p 3000 -n test -timeout 30m -run "TestAerospike" -ginkgo.focus "PutObject"
```

Test flags:
- `-h` — Aerospike server host (default: `127.0.0.1`)
- `-p` — Aerospike server port (default: `3000`)
- `-n` — Namespace (default: `test`)
- `-U` / `-P` — Username / password (if auth is enabled)
- `-timeout` — Go test timeout (use `30m` for full suite)

## Architecture

All source files live in the repository root (package `aerospike`). No `src/` or nested package hierarchy for the main client code.

Key files for struct marshaling/unmarshaling (the reflection-based object API):

- `marshal.go` — Struct-to-bins serialization (`marshal`, `structToMap`, `setBinMap`), tag parsing (`fieldAlias`, `stripOptions`), and field mapping cache (`syncMap`, `fillMapping`, `cacheObjectTags`)
- `read_command_reflect.go` — Bins-to-struct deserialization (`parseObject`, `setObjectField`, `setValue`, `setStructValue`)
- `batch_command_get_reflect.go` — Batch object deserialization (uses `setObjectField`)
- `batch_command_reflect.go` — Multi-record batch deserialization (uses `setObjectField`)
- `client_reflect.go` — Public API entry points: `PutObject`, `GetObject`, `BatchGetObjects`, etc.

Struct tag system:
- `as:"binName"` — maps a struct field to an Aerospike bin name
- `as:"-"` — excludes a field from persistence
- `as:"binName,omitempty"` — omits the field if it has a zero value
- `asm:"ttl"` / `asm:"gen"` — metadata tags for TTL and generation

Anonymous (embedded) struct fields are automatically flattened into the parent's bins. Named struct fields are stored as nested maps.

## Conventions

- Go 1.23+
- Module path: `github.com/aerospike/aerospike-client-go/v8`
- Tests are in the `aerospike_test` package (external test package)
- Test files use Ginkgo `Describe`/`Context`/`It` blocks with Gomega matchers
- Import aliases: `as` for the client package, `gg` for Ginkgo, `gm` for Gomega
- Errors use the custom `Error` interface (not stdlib `error`)
- Files guarded with `//go:build !as_performance` contain reflection-based code
- No external dependencies beyond the standard library and the project's own sub-packages (plus test framework)
- Follow existing code patterns — match the style of surrounding code

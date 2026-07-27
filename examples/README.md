# Aerospike Go Client — Code Examples

Runnable, verified code examples in a documentation-ready form: each example
file contains only the code being demonstrated, while the machinery that runs
and verifies it lives in separate files that never appear in documentation.

## Layout

| Path | Role |
|---|---|
| `put.go`, `get.go`, `batch.go`, … | Documentation-ready examples — one topic per file, plain client API code only |
| `main.go` | CLI entry point: selects examples, connects, reports results |
| `example_executor.go` | Configuration (flags/env), example lifecycle, server-capability gates, result reporting |
| `registry.go` | The central registry pairing each example with its verification fixture and server requirements |
| `fixtures/` | Verification: per-example Setup/Validate/Cleanup steps and shared helpers |
| `lua/` | Lua modules used by the query-aggregate examples |
| `examples_test.go` | `go test` bridge: exposes the registry as one subtest per example |

## Running the examples

Requires a reachable Aerospike server (e.g. `docker run -d -p 3000:3000 aerospike/aerospike-server`).
Run from the repository root:

```sh
# All examples
go run ./examples all

# Selected examples
go run ./examples put get scan_serial

# List available examples and flags
go run ./examples
```

The same registry is also exposed as a standard `go test` suite, one subtest
per example, so it can run through ordinary Go test tooling (`go test -v`,
`gotestsum`, IDE test runners):

```sh
go test ./examples -run TestExamples -v
```

Configuration flags fall back to environment variables, so CI pipelines need
no arguments:

| Flag | Environment variable | Default |
|------|----------------------|---------|
| `-h` | `AEROSPIKE_HOST` | `127.0.0.1` |
| `-p` | `AEROSPIKE_PORT` | `3000` |
| `-U` | `AEROSPIKE_USER` | (none) |
| `-P` | `AEROSPIKE_PASSWORD` | (none) |
| `-n` | `AEROSPIKE_NAMESPACE` | `test` |
| `-s` | `AEROSPIKE_SET` | `testset` |
| `-tlsName` | `AEROSPIKE_TLS_NAME` | (none) |
| `-encryptOnly` | `AEROSPIKE_TLS_ENCRYPT_ONLY` | false |
| `-useSystemCerts` | `AEROSPIKE_TLS_SYSTEM_CERTS` | false |
| `-serverCertDir` | `AEROSPIKE_TLS_SERVER_CERT_DIR` | (none) |
| `-clientCertFile` | `AEROSPIKE_TLS_CLIENT_CERT` | (none) |
| `-clientKeyFile` | `AEROSPIKE_TLS_CLIENT_KEY` | (none) |
| — | `AEROSPIKE_LUA_PATH` | the `lua/` directory next to this package |

The run prints a `PASS`/`SKIP`/`FAIL` summary and exits non-zero only when an
example fails. Examples whose server requirements are not met — Enterprise
Edition, a strong-consistency namespace, TTL support, TLS configuration, or a
security-enabled server — are reported as `SKIP` with the reason, never as
failures. CI runs the `go test` bridge through
[`gotestsum`](https://github.com/gotestyourself/gotestsum) against a live
server on every pull request, publishing the results as a JUnit-reported PR
check.

## How an example is executed

Every registry entry runs through the lifecycle:

```
Requires check → Setup → Run → Validate → Cleanup
```

- **Requires** — declared server capabilities (e.g.
  `EnterpriseEdition().AndStrongConsistency()`), checked against a
  once-probed server snapshot; unmet → SKIP.
- **Setup** — seeds required state; written cleanup-first so reruns are safe.
- **Run** — the documentation-ready code from the example file.
- **Validate** — reads the database back and asserts the state Run produced.
- **Cleanup** — removes everything the example created; runs even when an
  earlier step fails, and tolerates partial state.

## Adding an example

1. Create `<name>.go` with a single focused `run<Name>() error` function
   (plus small helpers if the topic needs them). Use only client-library
   types and the ambient `client`, `ns`, `set` values; return errors — no
   `log.Fatal`, `os.Exit`, panics, or assertions in example code.
2. Add its verification to the matching file in `fixtures/` (`record.go`,
   `query.go`, `udf.go`, `txn.go`): a factory returning
   `Fixture{Setup, Validate, Cleanup}`. Keep example-specific data out of
   shared helpers; factories may take parameters when they need the
   example's data (see `ListIter`).
3. Register it in `registry.go`, declaring any server requirements.

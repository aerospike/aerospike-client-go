# AI_PIPELINE.md — aerospike-client-go (v8)

## Overview

The official Aerospike client library for Go. Single Go module
(`github.com/aerospike/aerospike-client-go/v8`) whose public API lives in the repo
root package `aerospike`. Requires Go 1.23+. Integration tests require a running
Aerospike server.

## Build

Run from the repo root.

```bash
# Compile everything (library, subpackages, tools, examples)
go build ./...

# Library with reflection/Object-API code removed (build tag)
go build -tags=as_performance .

# Library built for Google App Engine (unsafe package removed)
go build -tags=app_engine .

# Command-line tools
cd tools/benchmark && go build -tags as_proxy -o benchmark .
cd tools/asinfo    && go build -o asinfo .
cd tools/cli       && go build -o cli .

# Example programs (each *.go is a standalone main)
find examples -name "*.go" -type f -print0 | xargs -0 -n1 go build
```

> CI vendors dependencies first (`go mod vendor`) and passes `-mod=vendor` /
> `GOFLAGS=-mod=vendor` to every command. There is **no** `vendor/` directory checked
> in, so for local work omit `-mod=vendor` and let the module cache resolve deps
> (`go mod download` if needed).

## Test

Tests use **Ginkgo v2 + Gomega**. The main integration suite is the root package
(`aerospike_suite_test.go` and the many `*_test.go` files in the root); additional
suites live under `internal/lua`, `types`, `pkg`, `internal/atomic`, and
`types/histogram`.

**A live Aerospike server is required for the root integration suite.** The suite
connects to `127.0.0.1:3000` by default; override with flags passed after `--`.

```bash
# Full suite, matching CI (race detector, skip HyperLogLog which needs enterprise feature)
go run github.com/onsi/ginkgo/v2/ginkgo -r -race -keep-going -succinct \
  -randomize-suites -skip="HyperLogLog"

# If the ginkgo CLI is installed on PATH (go install github.com/onsi/ginkgo/v2/ginkgo@v2.22.2):
ginkgo -r -race -skip="HyperLogLog"

# Point at a non-default server / cluster (flags go after `--`)
go run github.com/onsi/ginkgo/v2/ginkgo -r -race -- -h 127.0.0.1 -p 3000 -n test
go run github.com/onsi/ginkgo/v2/ginkgo -r -race -- -hosts=s1:3000,s2:3000 -use-services-alternate=true

# Run a single suite / package
go run github.com/onsi/ginkgo/v2/ginkgo -race types
go run github.com/onsi/ginkgo/v2/ginkgo -race internal/lua

# Focus a subset of specs (Ginkgo description regexp)
go run github.com/onsi/ginkgo/v2/ginkgo -r -race --focus="Expression"
```

Test flags (defined in `aerospike_suite_test.go`): `-h` host (default `127.0.0.1`),
`-p` port (default `3000`), `-hosts` comma-separated seed list, `-U`/`-P`
user/password, `-A` auth mode (`internal`|`external`), `-n` namespace (default
`test`), TLS flags (`-cert_file`, `-key_file`, `-node_tls_name`, `-root_ca`),
`-use-services-alternate`, `-debug`.

### Coverage

CI merges per-suite profiles with `gocovmerge` and checks them with
`vladopajic/go-test-coverage` against `.testcoverage.yml` (project threshold is
currently `0`). Reproduce locally:

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -output-dir=./ \
  -coverprofile=covprofile_native.out -covermode=atomic -coverpkg=./... \
  -race -keep-going -succinct -randomize-suites -skip="HyperLogLog"
go run github.com/wadey/gocovmerge covprofile_*.out > cover_all.out
```

## Lint / Format / Type-check

The repo configures **no** linter (no `.golangci.yml`), formatter config, or
type-checker beyond the Go toolchain. Keep code `gofmt`-clean and `go vet`-clean by
convention.

## Architecture & key directories

- **Repo root** (`package aerospike`) — the public client API: `client.go`,
  `policy*.go`, `*_command.go`, `operation.go`, expression/CDT builders, batch,
  scan/query, transactions. Also holds the root Ginkgo integration suite
  (`*_test.go`, entry point `aerospike_suite_test.go`).
- `types/` — public value/error/particle types (also has its own test suite).
- `pkg/` — supporting packages (its own test suite).
- `internal/` — non-public helpers: `internal/lua` (gopher-lua aggregation runtime,
  own suite), `internal/atomic` (own suite).
- `logger/` — logging abstraction (`asl.Logger`).
- `config/`, `utils/` — configuration and helpers.
- `tools/` — standalone binaries: `benchmark` (needs `as_proxy` tag), `asinfo`, `cli`.
- `examples/` — documentation-ready example programs and runner; shared setup
  in `examples/fixtures`.
- `docs/` — documentation.
- `.github/workflows/` — authoritative CI: `build.yml` (PR), `build-multi-node.yml`,
  `build-nightly.yml`, `nightly.yml`, `merge-coverage.yml`.

## Conventions

- **Go 1.23+** (`go.mod` declares `go 1.23.0`; a few files gate features behind a
  `go1.24` build tag).
- **Module path is versioned**: `github.com/aerospike/aerospike-client-go/v8`.
  Imports of the client use this `/v8` path.
- **Public-API stability matters** — this is a released client library. Avoid
  breaking exported signatures/behavior; record any breaking change in
  `CHANGELOG.md`. Retracted versions are declared in `go.mod`.
- **Build-tag variants must keep compiling**: `as_performance` (reflection/Object-API
  removed), `app_engine` (`unsafe` removed), plus `as_proxy` (benchmark tool) and
  `multinode` (multi-node-only tests). Guard tag-specific code with the matching
  `//go:build` constraint and provide the non-reflect fallback where the pattern
  already exists (e.g. `*_reflect.go`).
- Tests are **Ginkgo specs** (`Describe`/`It` + Gomega matchers), not stdlib
  `testing` tables — follow the existing spec style when adding tests.
- Dependencies are intentionally minimal (Ginkgo/Gomega, gopher-lua, x/sync, yaml).
  Don't add heavy third-party deps without cause.

## Gotchas

- **Integration tests need a live server.** Without one at `127.0.0.1:3000` (or the
  `-hosts`/`-h -p` you pass) the root suite fails immediately at connect.
- **HyperLogLog specs are skipped in CI** (`-skip="HyperLogLog"`) because they need an
  enterprise feature; keep that skip when reproducing CI.
- **No `vendor/` committed.** CI creates it on the fly (`go mod vendor` +
  `-mod=vendor`); locally use the module cache and drop the `-mod=vendor` flags.
- **FIPS in CI**: workflows set `GODEBUG=fips140=only` / `GOFIPS140=latest`. Crypto
  code must stay within the FIPS-approved set; test under those env vars if touching
  TLS/crypto.
- **Strong-consistency (SC) mode** is exercised in a separate CI matrix leg; some
  specs branch on `ConfiguredAsStrongConsistency`. Behavior can differ between AP and
  SC namespaces.
- The `benchmark` tool only builds with `-tags as_proxy`.
- Full suite is large and runs with `-race`; expect several minutes and ensure the
  server is warm.

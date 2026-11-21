#!/usr/bin/env bash
#
# The client's own L2 tests for the l2-proxy harness in aerospike-client-conformance (l2-proxy/README.md
# there). The manifest's L2_RUN runs this once per profile, in the built driver image on the client
# network, with the profile's values.env as its environment and L2_PROFILE set. The container has no
# internet, so it runs the L2 test binary that L5_BUILD compiled.
#
# Every TestL2 test picks its profile from the DISCOVERY_* variables and skips on the others, so each
# profile runs ^TestL2 and only its own tests run. A profile in which no test passed fails, so a broken
# topology never looks like a run whose tests all skipped.
#
# Env:
#   L2_PROFILE             the harness profile (required; the harness validates it)
#   DISCOVERY_TLS_CA_FILE  ee: the CA of the server certificates, empty on ce; the tests read it as AEROSPIKE_CA_CERT

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
TEST_BINARY=test/discovery/l2.test
profile="${L2_PROFILE:?L2_PROFILE is required}"

if [ ! -x "$TEST_BINARY" ]; then
	echo "ERROR: $TEST_BINARY not found; run L5_BUILD first" >&2
	exit 1
fi

export AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1
export AEROSPIKE_CA_CERT="${DISCOVERY_TLS_CA_FILE:-}"

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

rc=0
"./$TEST_BINARY" -test.run '^TestL2' -test.v -test.timeout 15m 2>&1 | tee "$log" || rc=$?

passed="$(grep -c '^--- PASS: TestL2' "$log" || true)"
echo "L2-RUN profile=$profile passed=$passed exit=$rc"

if [ "$rc" -ne 0 ]; then
	exit "$rc"
fi
if [ "$passed" -eq 0 ]; then
	echo "ERROR: no L2 test passed on profile $profile; check the values file" >&2
	exit 1
fi

#!/usr/bin/env bash
#
# Live TLS name matching against Enterprise, driven by tls/name-matching.yaml.
# CI runs this from .github/workflows/tls-matching.yaml. Not part of the SDK build.
#
# TestTlsMatchingCertificates writes a certificate set for each shape that has a
# live row. Each shape gets one Enterprise container serving its certificate and
# one TestTlsMatching run, then the container is removed. TestTlsMatchingReport
# writes conformance-report.json, with blocked rows recorded from blockedBy.
#
# Not ported from Java: Netty async (one Go connection model), CN-fallback
# warning (Go never falls back to the CN), revoked_serial (no revocation list),
# n6_proxy (N-6 is L2 EE).
#
# Required:
#   CONFORMANCE_DIR       aerospike-client-conformance 1.1.0 checkout
#   AEROSPIKE_IMAGE       Enterprise server image
#   FEATURES_CONF_PATH    feature-key file. The only source of the key.
#
# Optional:
#   TLS_MATCHING_PORT     host port mapped to container 4333 (default 4433)
#   CONFORMANCE_REPORT    report path (default test/tls/.run/conformance-report.json)
#
# Usage: test/tls/run_tls_matching.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="$ROOT_DIR/test/tls/.run"
CERT_DIR="$WORK_DIR/certificates"
RESULTS="$WORK_DIR/results.jsonl"
TEST_BIN="$WORK_DIR/tls.test"
PORT="${TLS_MATCHING_PORT:-4433}"
PREFIX="tls-matching-$$"
STARTED=()
FAILED=0

die() {
	echo "ERROR: $*" >&2
	exit 2
}

require_prereqs() {
	command -v docker >/dev/null 2>&1 || die "docker CLI not found"
	docker info >/dev/null 2>&1 || die "docker daemon not reachable"
	command -v go >/dev/null 2>&1 || die "go not found"
	[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
	[ -n "${CONFORMANCE_DIR:-}" ] || die "CONFORMANCE_DIR is not set"
	CONFORMANCE_DIR="$(cd "$CONFORMANCE_DIR" && pwd)"
	[ -f "$CONFORMANCE_DIR/VERSION" ] || die "CONFORMANCE_DIR has no VERSION"
	[ -f "$CONFORMANCE_DIR/tls/name-matching.yaml" ] || die "CONFORMANCE_DIR has no tls/name-matching.yaml"
	[ -n "${FEATURES_CONF_PATH:-}" ] || die "FEATURES_CONF_PATH is required"
	[ -r "$FEATURES_CONF_PATH" ] || die "FEATURES_CONF_PATH '$FEATURES_CONF_PATH' is not readable"
	[[ "$PORT" =~ ^[0-9]+$ ]] || die "TLS_MATCHING_PORT must be a port (got: $PORT)"

	FEATURES="$(cd "$(dirname "$FEATURES_CONF_PATH")" && pwd)/$(basename "$FEATURES_CONF_PATH")"
	REPORT="${CONFORMANCE_REPORT:-$WORK_DIR/conformance-report.json}"
	case "$REPORT" in
		/*) ;;
		*) REPORT="$PWD/$REPORT" ;;
	esac
}

cleanup() {
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
}

# run_test NAME [VAR=value ...] runs one test of the compiled binary from the repo root.
run_test() {
	local name="$1"
	shift
	(cd "$ROOT_DIR" && env "$@" "$TEST_BIN" -test.run "^${name}\$" -test.v -test.count=1)
}

build_tests() {
	echo "Compiling TLS matching tests"
	(cd "$ROOT_DIR" && CGO_ENABLED=0 go test -c -o "$TEST_BIN" .) || die "go test -c failed"
}

write_certificates() {
	run_test TestTlsMatchingCertificates TLS_MATCHING_CERTIFICATES="$CERT_DIR" || die "certificates were not written"
	[ -s "$CERT_DIR/shapes.tsv" ] || die "no live shapes in tls/name-matching.yaml"
}

write_server_conf() {
	local dir="$1" tls_name="$2"
	cat > "$dir/aerospike.conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name docker
    proto-fd-max 15000
}

logging {
    console {
        context any info
    }
}

network {
    tls $tls_name {
        cert-file /etc/aerospike/tls/server.crt
        key-file  /etc/aerospike/tls/server.key
        ca-file   /etc/aerospike/tls/ca.crt
    }
    service {
        address any
        port 3000
        tls-port 4333
        tls-authenticate-client false
        tls-name $tls_name
    }
    heartbeat {
        mode mesh
        port 3002
    }
    fabric {
        port 3001
    }
}

namespace test {
    replication-factor 1
    storage-engine memory {
        data-size 1G
    }
}
EOF
}

wait_ready() {
	local name="$1"
	for _ in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "rebalanced:" >/dev/null; then
			sleep 1
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name not ready after 60s"
}

start_server() {
	local container="$1" dir="$2"
	docker run -d --name "$container" -p "127.0.0.1:$PORT:4333" --ulimit nofile=15000 \
		-v "$dir:/etc/aerospike/tls:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file /etc/aerospike/tls/aerospike.conf >/dev/null
	STARTED+=("$container")
	wait_ready "$container"
}

# run_shape SHAPE TLS_NAME serves the shape's certificate and runs its live rows.
run_shape() {
	local shape="$1" tls_name="$2"
	local dir="$CERT_DIR/$shape" container="$PREFIX-$shape" rc=0

	write_server_conf "$dir" "$tls_name"
	echo
	echo "=== SHAPE $shape (server tls-name $tls_name)"
	start_server "$container" "$dir"
	run_test TestTlsMatching \
		TLS_MATCHING_SHAPE="$shape" \
		TLS_MATCHING_HOST=127.0.0.1 \
		TLS_MATCHING_PORT="$PORT" \
		TLS_MATCHING_CA="$dir/ca.crt" \
		TLS_MATCHING_RESULTS="$RESULTS" || rc=$?
	docker rm -f "$container" >/dev/null 2>&1 || true

	if [ "$rc" -eq 0 ]; then
		echo "SHAPE-STATUS $shape PASS"
	else
		echo "SHAPE-STATUS $shape FAIL"
		FAILED=1
	fi
}

write_report() {
	mkdir -p "$(dirname "$REPORT")"
	run_test TestTlsMatchingReport TLS_MATCHING_RESULTS="$RESULTS" CONFORMANCE_REPORT="$REPORT" || FAILED=1
	echo "report: $REPORT"
}

main() {
	require_prereqs
	trap cleanup EXIT
	trap 'exit 130' INT TERM

	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	build_tests

	export CONFORMANCE_DIR
	export AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1
	write_certificates

	: > "$RESULTS"
	local shape tls_name
	while IFS=$'\t' read -r shape tls_name <&3; do
		run_shape "$shape" "$tls_name"
	done 3< "$CERT_DIR/shapes.tsv"

	write_report
	if [ "$FAILED" -ne 0 ]; then
		echo "RESULT FAIL"
		exit 1
	fi
	echo "RESULT PASS"
}

main "$@"

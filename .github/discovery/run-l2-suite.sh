#!/usr/bin/env bash
# Runs the Go L2 discovery tests inside the L2 client container.
#
# discovery-l2.yaml compiles ./l2.test and passes the DISCOVERY_* variables
# from the topology env file. DISCOVERY_SUBSET is ce or ee. Every test uses the
# clear listeners, so they are always checked. TLS listeners exist only when
# the topology was built with them, and are checked when listed. A cluster that
# never came up fails here instead of looking like a run whose tests all skipped.
set -euo pipefail

readonly test_bin=./l2.test

fail() {
  echo "$*" >&2
  exit 1
}

check_listeners() {
  local host=$1 port
  local -a ports
  IFS=',' read -r -a ports <<< "$2"
  ((${#ports[@]} > 0)) || fail "No proxy ports to check"
  for port in "${ports[@]}"; do
    [[ "${port}" =~ ^[0-9]+$ ]] || fail "Refusing unexpected endpoint port: ${port}"
    echo "Checking TCP ${host}:${port}"
    timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
  done
}

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
clear_ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"

[[ "${host}" =~ ^[A-Za-z0-9.-]+$ ]] || fail "Refusing unexpected endpoint hostname: ${host}"

case "${subset}" in
  ce | ee) ;;
  *) fail "DISCOVERY_SUBSET must be ce or ee (got: ${subset})" ;;
esac

check_listeners "${host}" "${clear_ports}"
if [[ -n "${DISCOVERY_TLS_ENDPOINT_PORTS:-}" ]]; then
  check_listeners "${host}" "${DISCOVERY_TLS_ENDPOINT_PORTS}"
fi

[[ -x "${test_bin}" ]] || fail "L2 test binary not found or not executable: ${test_bin}"
exec "${test_bin}" -test.run '^TestL2' -test.v -test.timeout 10m

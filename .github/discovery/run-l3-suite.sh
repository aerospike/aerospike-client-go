#!/usr/bin/env bash
# Run on the runner after setup-kind-ako and, in the TLS job, enable-l3-tls.sh.
# discovery-k8s.yaml injects DISCOVERY_* and AEROSPIKE_ADMIN_PASSWORD.
# TCP-checks the ingress and service NodePorts, then runs TestL3* (skipping
# TestL3Tls* unless DISCOVERY_TLS_* is set). A cluster that never came up
# fails here instead of looking like a run whose tests all skipped; one in
# another shape fails after it, on a skip not named in EXPECTED_SKIPS
# (comma-separated test names).
# L3_REREGISTER_KIND_CLUSTER names the kind cluster whose deleted Nodes
# reregister-kind-nodes.sh registers again while the tests run.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ingress_ports="${DISCOVERY_INGRESS_PORTS:?DISCOVERY_INGRESS_PORTS is required}"
node_ports="${DISCOVERY_SERVICE_NODE_PORTS:?DISCOVERY_SERVICE_NODE_PORTS is required}"
password="${AEROSPIKE_ADMIN_PASSWORD:?AEROSPIKE_ADMIN_PASSWORD is required}"
kubeconfig="${DISCOVERY_KUBECONFIG:?DISCOVERY_KUBECONFIG is required}"

fail() {
  echo "$*" >&2
  exit 1
}

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || fail "Refusing unexpected endpoint hostname: ${host}"
[[ "$password" =~ ^[A-Za-z0-9]+$ ]] || fail "Refusing unexpected admin password characters"
[[ -f "$kubeconfig" ]] || fail "kubeconfig does not exist: ${kubeconfig}"
if [[ -n "${DISCOVERY_TLS_CA_FILE:-}${DISCOVERY_TLS_NAME:-}" ]]; then
  [[ -f "${DISCOVERY_TLS_CA_FILE:-}" ]] || fail "DISCOVERY_TLS_CA_FILE does not exist: ${DISCOVERY_TLS_CA_FILE:-}"
  [[ "${DISCOVERY_TLS_NAME:-}" =~ ^[A-Za-z0-9.-]+$ ]] || fail "Refusing unexpected tls-name: ${DISCOVERY_TLS_NAME:-}"
fi

check_ports() {
  local label=$1
  local ports=$2
  local port
  local -a port_list
  IFS=',' read -r -a port_list <<< "$ports"
  if ((${#port_list[@]} == 0)); then
    fail "${label} is empty"
  fi
  for port in "${port_list[@]}"; do
    [[ "$port" =~ ^[0-9]+$ ]] || fail "Refusing unexpected ${label} port: ${port}"
    echo "Checking TCP ${host}:${port}"
    timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
  done
}

# Prints the test output and keeps the go test JSON events in $1.
run_tests() {
  local -a selection=(-run '^TestL3' -skip '^TestL3Tls')
  [[ -z "${DISCOVERY_TLS_CA_FILE:-}" ]] || selection=(-run '^TestL3Tls')
  go test -count=1 -timeout 20m "${selection[@]}" -json . | tee "$1" |
    jq --unbuffered -rj 'select(.Action == "output") | .Output'
}

# Fails when no test ran, or a test skipped that EXPECTED_SKIPS does not name.
check_skips() {
  local ran unexpected
  ran=$(jq -s '[.[] | select(.Test and (.Action | IN("pass", "fail", "skip")))] | length' "$1")
  ((ran > 0)) || fail "No L3 test ran"
  unexpected=$(jq -rs --arg expected "${EXPECTED_SKIPS:-}" \
    '[.[] | select(.Test and .Action == "skip") | .Test] - ($expected | split(",")) | join(", ")' "$1")
  [[ -z "$unexpected" ]] || fail "Skipped but not in EXPECTED_SKIPS: ${unexpected}"
  echo "${ran} L3 tests ran, every skip expected"
}

check_ports "ingress" "$ingress_ports"
check_ports "service node" "$node_ports"

export KUBECONFIG="$kubeconfig" AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1 CGO_ENABLED=0
report="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/l3-go-test.json"
if [[ -n "${L3_REREGISTER_KIND_CLUSTER:-}" ]]; then
  bash "$(dirname "$0")/reregister-kind-nodes.sh" "$L3_REREGISTER_KIND_CLUSTER" &
  watcher=$!
  trap 'kill "$watcher" 2>/dev/null || true' EXIT
fi
run_tests "$report"
check_skips "$report"

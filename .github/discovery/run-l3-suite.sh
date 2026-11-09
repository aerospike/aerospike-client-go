#!/usr/bin/env bash
# Run on the runner after setup-kind-ako and, in the TLS job, enable-l3-tls.sh.
# discovery-k8s.yaml injects DISCOVERY_* and AEROSPIKE_ADMIN_PASSWORD.
# TCP-checks the ingress and service NodePorts, then runs TestL3* (skipping
# TestL3Tls* unless DISCOVERY_TLS_* is set). A cluster that never came up
# fails here instead of looking like a run whose tests all skipped.
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

check_ports "ingress" "$ingress_ports"
check_ports "service node" "$node_ports"

export KUBECONFIG="$kubeconfig"
export AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1
export CGO_ENABLED=0
if [[ -n "${DISCOVERY_TLS_CA_FILE:-}" ]]; then
  exec go test -count=1 -timeout 20m -run '^TestL3Tls' -v .
fi
exec go test -count=1 -timeout 20m -run '^TestL3' -skip '^TestL3Tls' -v .

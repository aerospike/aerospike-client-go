#!/usr/bin/env bash
# Run after setup-kind-ako. setup-kind-ako builds the AerospikeCluster without
# TLS, and AKO generates no certificates, so this script generates a CA and a
# server certificate whose only dNSName SAN is the tls-name, stores them in a
# secret, and patches the AerospikeCluster to serve TLS from that secret with
# tls-alternate-access configured as alternate-access is. It waits until the
# operator reports Completed with TLS, then writes ca-file and tls-name to
# GITHUB_OUTPUT (when set) and to standard output. The CA and server keys are
# deleted once the secret holds them.
#
# Under alternateAccess networkLoadBalancer it also sets
# loadBalancerTLSAlternateAccessAddress. When that differs from
# loadBalancerAlternateAccessAddress, it first adds a CoreDNS hosts entry for
# it at the node address the clear one has, and on a GitHub Actions runner the
# same line to /etc/hosts, and writes it as tls-endpoint-hostname.
#
# Optional environment:
#   L3_TLS_DIR        directory for the generated files
#                     (default: ${RUNNER_TEMP:-/tmp}/discovery-l3-tls)
#   L3_TLS_TIMEOUT    seconds to wait for Completed (default: 900)
#   DISCOVERY_TLS_ENDPOINT_HOSTNAME
#                     loadBalancerTLSAlternateAccessAddress under
#                     networkLoadBalancer (default: loadBalancerAlternateAccessAddress)
set -euo pipefail

TLS_NAME=aerospike-tls
TLS_PORT=4333
TLS_SECRET=aerospike-tls-secret
TLS_VOLUME=aerospike-tls
TLS_PATH=/etc/aerospike/tls
tls_dir="${L3_TLS_DIR:-${RUNNER_TEMP:-/tmp}/discovery-l3-tls}"
timeout_s="${L3_TLS_TIMEOUT:-900}"

error() {
  echo "Error: $*" >&2
  exit 1
}

write_output() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"
  fi
  printf '%s=%s\n' "$1" "$2"
}

[[ "$timeout_s" =~ ^[0-9]+$ ]] || error "L3_TLS_TIMEOUT must be a number of seconds (got: $timeout_s)"
for tool in kubectl openssl; do
  command -v "$tool" >/dev/null 2>&1 || error "$tool not found on PATH"
done

mapfile -t clusters < <(kubectl get aerospikecluster --all-namespaces \
  -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}')
((${#clusters[@]} == 1)) || error "expected one AerospikeCluster, found: ${clusters[*]}"
read -r namespace name <<< "${clusters[0]}"

policy() {
  kubectl -n "$namespace" get aerospikecluster "$name" -o "jsonpath={.spec.aerospikeNetworkPolicy.$1}"
}

alternate_access="$(policy alternateAccess)"
tls_endpoint=""
if [[ "$alternate_access" == networkLoadBalancer ]]; then
  clear_endpoint="$(policy loadBalancerAlternateAccessAddress)"
  [[ -n "$clear_endpoint" ]] || error "networkLoadBalancer without loadBalancerAlternateAccessAddress"
  tls_endpoint="${DISCOVERY_TLS_ENDPOINT_HOSTNAME:-$clear_endpoint}"
  [[ "$tls_endpoint" =~ ^[A-Za-z0-9.-]+$ ]] || error "Refusing unexpected TLS endpoint hostname: $tls_endpoint"
fi

mkdir -p "$tls_dir"
tls_dir="$(cd "$tls_dir" && pwd)"
(
  cd "$tls_dir"
  printf '[v3_req]\nsubjectAltName = DNS:%s\n' "$TLS_NAME" > san.ext
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 30 \
    -subj "/CN=Aerospike-L3-Test-CA"
  openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=$TLS_NAME"
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 30 \
    -extensions v3_req -extfile san.ext
  rm -f server.csr ca.srl san.ext
) > "$tls_dir/openssl.log" 2>&1 || { cat "$tls_dir/openssl.log" >&2; error "certificate generation failed"; }
echo "Server certificate:"
openssl x509 -in "$tls_dir/server.crt" -noout -subject -ext subjectAltName

kubectl -n "$namespace" create secret generic "$TLS_SECRET" \
  --from-file="ca.crt=$tls_dir/ca.crt" \
  --from-file="server.crt=$tls_dir/server.crt" \
  --from-file="server.key=$tls_dir/server.key" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f "$tls_dir/ca.key" "$tls_dir/server.key"

# The server resolves its access addresses at startup, so the name resolves before any pod restarts with TLS.
if [[ -n "$tls_endpoint" && "$tls_endpoint" != "$clear_endpoint" ]]; then
  corefile="$(kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}')"
  ip="$(awk -v name="$clear_endpoint" 'NF == 2 && $2 == name { print $1; exit }' <<< "$corefile")"
  [[ -n "$ip" ]] || error "CoreDNS has no hosts entry for $clear_endpoint"
  if ! awk -v name="$tls_endpoint" 'NF == 2 && $2 == name { found = 1 } END { exit ! found }' <<< "$corefile"; then
    awk -v name="$clear_endpoint" -v entry="        $ip $tls_endpoint" \
      '{ print } NF == 2 && $2 == name { print entry }' <<< "$corefile" > "$tls_dir/Corefile"
    kubectl -n kube-system create configmap coredns --from-file="Corefile=$tls_dir/Corefile" \
      --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n kube-system rollout restart deploy/coredns
    kubectl -n kube-system rollout status deploy/coredns --timeout=180s
  fi
  if [[ "${GITHUB_ACTIONS:-}" == true ]]; then
    echo "$ip $tls_endpoint" | sudo tee -a /etc/hosts >/dev/null
  fi
  echo "CoreDNS resolves $tls_endpoint to $ip, as $clear_endpoint"
fi

lb_tls_patch=""
if [[ -n "$tls_endpoint" ]]; then
  lb_tls_patch=',
  {"op": "add", "path": "/spec/aerospikeNetworkPolicy/loadBalancerTLSAlternateAccessAddress", "value": "'"$tls_endpoint"'"}'
fi

cat > "$tls_dir/patch.json" <<EOF
[
  {"op": "add", "path": "/spec/storage/volumes/-", "value": {
    "name": "$TLS_VOLUME",
    "source": {"secret": {"secretName": "$TLS_SECRET"}},
    "aerospike": {"path": "$TLS_PATH"}}},
  {"op": "add", "path": "/spec/aerospikeConfig/network/tls", "value": [{
    "name": "$TLS_NAME",
    "cert-file": "$TLS_PATH/server.crt",
    "key-file": "$TLS_PATH/server.key",
    "ca-file": "$TLS_PATH/ca.crt"}]},
  {"op": "add", "path": "/spec/aerospikeConfig/network/service/tls-name", "value": "$TLS_NAME"},
  {"op": "add", "path": "/spec/aerospikeConfig/network/service/tls-port", "value": $TLS_PORT},
  {"op": "add", "path": "/spec/aerospikeConfig/network/service/tls-authenticate-client", "value": "false"},
  {"op": "add", "path": "/spec/aerospikeNetworkPolicy/tlsAccess", "value": "$(policy access)"},
  {"op": "add", "path": "/spec/aerospikeNetworkPolicy/tlsAlternateAccess", "value": "$alternate_access"},
  {"op": "add", "path": "/spec/operatorClientCert", "value": {
    "secretCertSource": {"secretName": "$TLS_SECRET", "caCertsFilename": "ca.crt"}}}$lb_tls_patch
]
EOF
kubectl -n "$namespace" patch aerospikecluster "$name" --type json --patch-file "$tls_dir/patch.json"

state() {
  printf '%s|%s|%s|%s' \
    "$(kubectl -n "$namespace" get aerospikecluster "$name" -o jsonpath="{.status.aerospikeConfig.network.service['tls-name']}")" \
    "$(kubectl -n "$namespace" get aerospikecluster "$name" -o jsonpath='{.status.phase}')" \
    "$(kubectl -n "$namespace" get pods -l "aerospike.com/cr=$name" --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -c . || true)" \
    "$(kubectl -n "$namespace" get aerospikecluster "$name" -o jsonpath='{.spec.size}')"
}

deadline=$((SECONDS + timeout_s))
while :; do
  IFS='|' read -r tls_name phase running size <<< "$(state)"
  [[ "$tls_name" == "$TLS_NAME" && "$phase" == "Completed" && "$running" == "$size" ]] && break
  if ((SECONDS >= deadline)); then
    kubectl -n "$namespace" get aerospikecluster,pods -o wide >&2 || true
    error "AerospikeCluster $namespace/$name not Completed with tls-name $TLS_NAME within ${timeout_s}s:" \
      "tls-name=$tls_name phase=$phase running=$running size=$size"
  fi
  sleep 5
done
echo "AerospikeCluster $namespace/$name serves TLS: tls-name=$tls_name phase=$phase running=$running size=$size"

write_output ca-file "$tls_dir/ca.crt"
write_output tls-name "$TLS_NAME"
if [[ -n "$tls_endpoint" ]]; then
  write_output tls-endpoint-hostname "$tls_endpoint"
fi

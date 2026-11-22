#!/usr/bin/env bash
# Restart kubelet in each node container of a kind cluster whose Node object
# has been missing from the API server for L3_REREGISTER_AFTER_S seconds.
# kind's kubelet does not register a deleted Node again on its own; restarted,
# it registers a new Node that carries none of the labels set through the API.
# Prints one line when a Node goes missing, when its kubelet is restarted and
# when it is registered again. Runs until killed.
#
# Usage: reregister-kind-nodes.sh <kind cluster>
#
# Environment:
#   KUBECONFIG                  kubeconfig of the kind cluster
#   KIND_EXPERIMENTAL_PROVIDER  podman runs the podman CLI instead of docker
#   L3_REREGISTER_AFTER_S       seconds to wait for the Node first (default: 60)
set -uo pipefail

cluster="${1:?usage: reregister-kind-nodes.sh <kind cluster>}"
after_s="${L3_REREGISTER_AFTER_S:-60}"
runtime=docker
[[ "${KIND_EXPERIMENTAL_PROVIDER:-}" == podman ]] && runtime=podman

[[ "$after_s" =~ ^[0-9]+$ ]] || {
  echo "L3_REREGISTER_AFTER_S must be a number of seconds (got: $after_s)" >&2
  exit 1
}
mapfile -t nodes < <(kind get nodes --name "$cluster" 2>/dev/null)
((${#nodes[@]} > 0)) || {
  echo "No node containers in kind cluster $cluster" >&2
  exit 1
}

declare -A missing=() restarted=()
echo "L3-NODE-WATCH cluster=$cluster nodes=${nodes[*]} restartAfterS=$after_s"
while sleep 2; do
  for node in "${nodes[@]}"; do
    # An API server error is neither missing nor present.
    found="$(kubectl --request-timeout=10s get node "$node" --ignore-not-found -o name 2>/dev/null)" || continue
    if [[ -n "$found" ]]; then
      if [[ -n "${missing[$node]:-}" ]]; then
        echo "L3-NODE-REGISTERED node=$node missingS=$((SECONDS - missing[$node]))" \
          "by=${restarted[$node]:-kubelet-on-its-own}"
        unset "missing[$node]" "restarted[$node]"
      fi
    elif [[ -z "${missing[$node]:-}" ]]; then
      missing[$node]=$SECONDS
      echo "L3-NODE-MISSING node=$node"
    elif [[ -z "${restarted[$node]:-}" ]] && ((SECONDS - missing[$node] >= after_s)); then
      if "$runtime" exec "$node" systemctl restart kubelet; then
        restarted[$node]=kubelet-restart
        echo "L3-KUBELET-RESTARTED node=$node missingS=$((SECONDS - missing[$node]))"
      else
        echo "L3-KUBELET-RESTART-FAILED node=$node"
      fi
    fi
  done
done

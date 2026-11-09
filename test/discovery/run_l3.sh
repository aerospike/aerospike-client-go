#!/usr/bin/env bash
# shellcheck disable=SC2155 # export VAR="$(output ...)" must not abort when output is empty
#
# Local L3 runner. CI does not call this. Not part of the SDK build.
#
# Kubernetes discovery tests against a kind cluster running the Aerospike
# Kubernetes Operator, built the same way .github/workflows/discovery-k8s.yaml
# builds it, with shared-workflows setup-kind-ako at the commit that workflow
# pins.
#
# The default profile is that cluster: one kind node, the operator, an
# AerospikeCluster with multiPodPerHost: true and alternateAccess: configuredIP,
# the node labelled with the endpoint hostname, a CoreDNS entry for it, and
# ingress-nginx tcp-services. It then sets the variables
# .github/discovery/run-l3-suite.sh sets and runs TestL3* except TestL3Tls*.
#
# The per-node profile builds what setup-kind-ako cannot: one Aerospike pod per
# kind worker (multiPodPerHost: false), each worker labelled with its own
# configured-alternate-access-address (node-1.<L3_PER_NODE_DOMAIN>, node-2...,
# one per worker), and CoreDNS resolving each name to its worker. The operator,
# the AerospikeCluster and the secrets come from the same setup-kind-ako
# functions. Runs TestL3PassThrough* only.
#
# The tls profile builds the default profile's cluster at two pods, then runs
# .github/discovery/enable-l3-tls.sh, as the l3-tls job of discovery-k8s.yaml
# does: the AerospikeCluster serves TLS from a secret holding a server
# certificate whose only dNSName SAN is the cluster tls-name. Runs TestL3Tls*.
#
# The client dials each kind node's address, as the workflow's client does on a
# Linux runner. That address is not reachable from a macOS host, so the client
# runs in a container on the kind network, where each labelled hostname
# resolves to its node. Compiles a linux l3.test and runs it in
# debian:bookworm-slim. The action writes the endpoint hostname into /etc/hosts.
# This script never does.
#
# The script refuses to start when a kind cluster with the same name exists, and
# deletes the kind cluster, the client container and the kind network (when it
# created that network) on exit, including on failure.
#
# Needs kind, kubectl, helm, openssl, docker (podman when KIND_EXPERIMENTAL_PROVIDER=podman),
# bash 4, and go (unless L3_SKIP_BUILD=1 and a linux l3.test is in the repo root).
# openssl is for setup-kind-ako prepare (admin password) and the tls profile.
# Work directory: test/discovery/.run/l3.
#
# Usage:
#   FEATURES_CONF_PATH=/path/features.conf test/discovery/run_l3.sh
#   L3_PROFILE=per-node FEATURES_CONF_PATH=/path/features.conf test/discovery/run_l3.sh
#   L3_PROFILE=tls FEATURES_CONF_PATH=/path/features.conf test/discovery/run_l3.sh
#
# Env:
#   FEATURES_CONF_PATH          Enterprise feature-key file. Required. Never FEATURES_CONF env
#   L3_PROFILE                  default, per-node or tls (default: default)
#   L3_KIND_CLUSTER             kind cluster name (default: discovery-l3)
#   L3_CLUSTER_SIZE             initial AerospikeCluster size on default and tls, 1-8 (default: the
#                               action's default on default, 2 on tls)
#   L3_PER_NODE_WORKERS         kind workers, and Aerospike pods, on per-node, 2-8 (default: 2)
#   L3_PER_NODE_DOMAIN          domain of the per-node hostnames (default: as-endpoint.test)
#   L3_SERVER_IMAGE             server image of the AerospikeCluster, as repository:tag (default: the
#                               action's server-image default). It must already be in the local image
#                               store. The script loads it into kind, so a private image needs no pull
#                               secret. The operator's webhook rejects a tag whose version it has no
#                               config schema for
#   KIND_EXPERIMENTAL_PROVIDER  passed to kind. podman also makes this script use the podman CLI
#   SHARED_WORKFLOWS_DIR        local aerospike/shared-workflows clone instead of downloading the action
#   L3_SKIP_BUILD               1 uses ./l3.test (must already be linux)
#
# Profiles (L3_PROFILE: what it builds, tests):
#   default   shared-endpoint cluster the discovery-k8s workflow builds. TestL3* except TestL3Tls*
#   per-node  one pod per worker, own hostname per worker. TestL3PassThrough*
#   tls       default cluster at two pods, then enable-l3-tls.sh. TestL3Tls*

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# setup-kind-ako at the commit discovery-k8s.yaml pins (v5.1.0).
SHARED_WORKFLOWS_COMMIT=308f37e0cd5c7a6386a99ea7afc379d5a4ea9256
ACTION_PATH=.github/actions/setup-kind-ako
CLIENT_IMAGE=debian:bookworm-slim
PROFILE="${L3_PROFILE:-default}"
CLUSTER="${L3_KIND_CLUSTER:-discovery-l3}"
WORKERS="${L3_PER_NODE_WORKERS:-2}"
UNRESOLVED="${L3_UNRESOLVED_ENDPOINT:-0}"
PER_NODE_DOMAIN="${L3_PER_NODE_DOMAIN:-as-endpoint.test}"
WORK_DIR="$SCRIPT_DIR/.run/l3"
ACTION_DIR="$WORK_DIR/action"
KIND_NETWORK=kind
CLIENT="$CLUSTER-client"
CREATED=0
KIND_NETWORK_EXISTED=0
per_node_names=()

die() {
	echo "ERROR: $*" >&2
	exit 2
}

output() {
	grep -E "^$1=" "$2" | head -1 | cut -d= -f2-
}

validate() {
	case "$PROFILE" in
		default|per-node|tls) ;;
		*) die "L3_PROFILE must be default, per-node or tls (got: $PROFILE)" ;;
	esac
	[[ "$CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "L3_KIND_CLUSTER must be lower-case letters, digits and dashes (got: $CLUSTER)"
	[[ "$WORKERS" =~ ^[2-8]$ ]] || die "L3_PER_NODE_WORKERS must be 2-8 (got: $WORKERS)"
	[[ "$PER_NODE_DOMAIN" =~ ^[a-z0-9.-]+$ ]] || die "L3_PER_NODE_DOMAIN must be a DNS name (got: $PER_NODE_DOMAIN)"
	[[ "$UNRESOLVED" =~ ^[01]$ ]] || die "L3_UNRESOLVED_ENDPOINT must be 0 or 1 (got: $UNRESOLVED)"
	[ "$UNRESOLVED" = "0" ] || die "L3_UNRESOLVED_ENDPOINT=1 waits on a Go unresolved L3 test. Do not use it with this runner"

	for tool in kind kubectl helm; do
		command -v "$tool" >/dev/null 2>&1 || die "$tool not found on PATH"
	done
	if [ "${KIND_EXPERIMENTAL_PROVIDER:-}" = "podman" ]; then
		DOCKER=podman
	else
		DOCKER=docker
	fi
	command -v "$DOCKER" >/dev/null 2>&1 || die "$DOCKER not found on PATH"
	"$DOCKER" info >/dev/null 2>&1 || die "$DOCKER is not reachable"
	command -v openssl >/dev/null 2>&1 || die "openssl not found on PATH"
	((BASH_VERSINFO[0] >= 4)) || die "bash 4 or later is required by setup-kind-ako"

	[ -n "${FEATURES_CONF_PATH:-}" ] || die "FEATURES_CONF_PATH is required: the AerospikeCluster runs Enterprise"
	[ -r "$FEATURES_CONF_PATH" ] || die "FEATURES_CONF_PATH '$FEATURES_CONF_PATH' is not readable"
	if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		die "kind cluster $CLUSTER already exists; delete it or set L3_KIND_CLUSTER"
	fi
}

compile_tests() {
	if [ "${L3_SKIP_BUILD:-0}" = "1" ]; then
		[ -x "$ROOT_DIR/l3.test" ] || die "L3_SKIP_BUILD=1 but $ROOT_DIR/l3.test is missing"
		cp "$ROOT_DIR/l3.test" "$WORK_DIR/l3.test"
		chmod +x "$WORK_DIR/l3.test"
		return
	fi
	command -v go >/dev/null 2>&1 || die "go not found"
	arch="$("$DOCKER" version -f '{{.Server.Arch}}')"
	case "$arch" in
		amd64|arm64) ;;
		*) die "unsupported docker architecture '$arch'" ;;
	esac
	echo "Compiling L3 tests for linux/$arch"
	(cd "$ROOT_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -c -o "$WORK_DIR/l3.test" .) ||
		die "go test -c failed"
	chmod +x "$WORK_DIR/l3.test"
}

fetch_action() {
	local f
	if [ -z "${SHARED_WORKFLOWS_DIR:-}" ]; then
		command -v curl >/dev/null 2>&1 || die "curl not found on PATH"
	fi
	for f in entrypoint.sh action.yaml templates/aerospike-cluster-values.yaml templates/ingress-values.yaml templates/rbac.yaml; do
		mkdir -p "$ACTION_DIR/$(dirname "$f")"
		if [ -n "${SHARED_WORKFLOWS_DIR:-}" ]; then
			git -C "$SHARED_WORKFLOWS_DIR" show "$SHARED_WORKFLOWS_COMMIT:$ACTION_PATH/$f" > "$ACTION_DIR/$f" ||
				die "$f not found at $SHARED_WORKFLOWS_COMMIT in $SHARED_WORKFLOWS_DIR"
		else
			curl -sfL "https://raw.githubusercontent.com/aerospike/shared-workflows/$SHARED_WORKFLOWS_COMMIT/$ACTION_PATH/$f" \
				-o "$ACTION_DIR/$f" || die "failed to download $ACTION_PATH/$f at $SHARED_WORKFLOWS_COMMIT"
		fi
	done
}

# Drop the action's /etc/hosts write. macOS cannot use it; the client container
# gets --add-host instead. functions.sh is cmd_setup without the case dispatch.
strip_hosts_from_action() {
	sed '/| sudo tee -a \/etc\/hosts/d' "$ACTION_DIR/entrypoint.sh" > "$ACTION_DIR/entrypoint-local.sh"
	if grep -nE 'sudo|/etc/hosts' "$ACTION_DIR/entrypoint-local.sh"; then
		die "entrypoint.sh at $SHARED_WORKFLOWS_COMMIT still writes /etc/hosts after removing its hosts step"
	fi
	sed '/^case \${1-} in$/,$d' "$ACTION_DIR/entrypoint-local.sh" > "$ACTION_DIR/functions.sh"
	grep -q '^cmd_setup()' "$ACTION_DIR/functions.sh" || die "entrypoint.sh at $SHARED_WORKFLOWS_COMMIT has no cmd_setup"
}

read_action_defaults() {
	KIND_IMAGE="$(grep -oE 'kindest/node:[^ "]+' "$ACTION_DIR/action.yaml" | head -1)"
	SERVER_IMAGE="$(sed -n '/^  server-image:/,/default:/s/^ *default: *//p' "$ACTION_DIR/action.yaml")"
	[ -n "$KIND_IMAGE" ] || die "no kind node image in action.yaml"
	[ -n "$SERVER_IMAGE" ] || die "no server-image default in action.yaml"
	if [ -n "${L3_SERVER_IMAGE:-}" ]; then
		SERVER_IMAGE="$L3_SERVER_IMAGE"
		"$DOCKER" image inspect "$SERVER_IMAGE" >/dev/null 2>&1 || die "L3_SERVER_IMAGE $SERVER_IMAGE is not in the local image store; pull it first"
	fi
	DEFAULT_SIZE="$(sed -n '/^  cluster-size:/,/default:/s/^ *default: *"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p' "$ACTION_DIR/action.yaml")"
	[ "$PROFILE" = "tls" ] && DEFAULT_SIZE=2
	CLUSTER_SIZE="${L3_CLUSTER_SIZE:-$DEFAULT_SIZE}"
	if [ "$PROFILE" = "per-node" ]; then
		CLUSTER_SIZE="$WORKERS"
		local i
		for i in $(seq 1 "$WORKERS"); do
			per_node_names+=("node-$i.$PER_NODE_DOMAIN")
		done
	fi
}

cleanup() {
	"$DOCKER" rm -f "$CLIENT" >/dev/null 2>&1 || true
	if [ "$CREATED" = "1" ]; then
		echo "Deleting kind cluster $CLUSTER"
		kind delete cluster --name "$CLUSTER" --kubeconfig "$WORK_DIR/kubeconfig" || true
	fi
	if [ "$KIND_NETWORK_EXISTED" = "0" ] && "$DOCKER" network inspect "$KIND_NETWORK" >/dev/null 2>&1; then
		"$DOCKER" network rm "$KIND_NETWORK" >/dev/null || true
	fi
}

set_discovery() {
	export DISCOVERY_ENDPOINT_HOSTNAME="$1"
	export DISCOVERY_INGRESS_PORTS="$2"
	export DISCOVERY_SERVICE_NODE_PORTS="$3"
	export DISCOVERY_KUBECONFIG=/kubeconfig
	export AEROSPIKE_ADMIN_PASSWORD="$PASSWORD"
	L3_SEED="$4"
	TEST_RUN="$5"
	TEST_SKIP="$6"
}

prepare() {
	echo "Preparing setup-kind-ako (shared-workflows $SHARED_WORKFLOWS_COMMIT), $PROFILE profile"
	ADMIN_PASSWORD="" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" SERVER_IMAGE="$SERVER_IMAGE" \
		GITHUB_OUTPUT="$WORK_DIR/prepare.out" bash "$ACTION_DIR/entrypoint-local.sh" prepare >/dev/null ||
		die "setup-kind-ako prepare failed"
	PASSWORD="$(output admin-password "$WORK_DIR/prepare.out")"
}

create_kind_cluster() {
	local kind_args=() i
	if [ "$PROFILE" = "per-node" ]; then
		{
			printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n'
			for i in $(seq 1 "$WORKERS"); do
				printf '  - role: worker\n'
			done
		} > "$WORK_DIR/kind-config-per-node.yaml"
		kind_args=(--config "$WORK_DIR/kind-config-per-node.yaml")
	fi
	echo "Creating kind cluster $CLUSTER ($KIND_IMAGE)"
	CREATED=1
	kind create cluster --name "$CLUSTER" --image "$KIND_IMAGE" "${kind_args[@]}" --wait 180s \
		--kubeconfig "$KUBECONFIG" || die "kind create cluster failed"
}

load_server_image() {
	[ -n "${L3_SERVER_IMAGE:-}" ] || return 0
	echo "Loading $SERVER_IMAGE into kind cluster $CLUSTER"
	"$DOCKER" save -o "$WORK_DIR/server-image.tar" "$SERVER_IMAGE" || die "cannot save $SERVER_IMAGE"
	kind load image-archive "$WORK_DIR/server-image.tar" --name "$CLUSTER" || die "cannot load $SERVER_IMAGE into $CLUSTER"
	rm -f "$WORK_DIR/server-image.tar"
}

# Shared-endpoint cluster the discovery-k8s workflow builds (default and tls).
setup_shared() {
	local host ports node_ports
	ADMIN_PASSWORD="$PASSWORD" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" \
		SERVER_IMAGE="$SERVER_IMAGE" GITHUB_OUTPUT="$WORK_DIR/setup.out" \
		bash "$ACTION_DIR/entrypoint-local.sh" setup 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log" ||
		die "setup-kind-ako setup failed (log: $WORK_DIR/setup.log)"
	host="$(output endpoint-hostname "$WORK_DIR/setup.out")"
	ports="$(output ingress-ports "$WORK_DIR/setup.out")"
	node_ports="$(output service-node-ports "$WORK_DIR/setup.out")"
	set_discovery "$host" "$ports" "$node_ports" "$host:${ports%%,*}" '^TestL3' '^TestL3Tls'
}

# One pod per worker, own hostname per worker. setup-kind-ako cannot build this.
setup_per_node() {
	local status port
	# Not in an && or || list, so set -e holds inside the subshell.
	set +e
	(
		# shellcheck source=/dev/null
		source "$ACTION_DIR/functions.sh"
		# shellcheck disable=SC2034 # read by the action's functions
		ADMIN_PASSWORD="$PASSWORD"
		CLUSTER_SIZE="$WORKERS"
		mkdir -p "$WORK_DIR"

		eval "action_$(declare -f render_cluster_manifest)"
		# shellcheck disable=SC2329 # called by the action's deploy_cluster
		render_cluster_manifest() {
			action_render_cluster_manifest
			sed 's/^    multiPodPerHost: true$/    multiPodPerHost: false/' "$WORK_DIR/aerospike-cluster.yaml" \
				> "$WORK_DIR/aerospike-cluster.per-node.yaml"
			mv "$WORK_DIR/aerospike-cluster.per-node.yaml" "$WORK_DIR/aerospike-cluster.yaml"
			grep -q '^    multiPodPerHost: false$' "$WORK_DIR/aerospike-cluster.yaml" ||
				error "AerospikeCluster manifest has no multiPodPerHost to set to false"
		}

		mapfile -t workers < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
			-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
		((${#workers[@]} == WORKERS)) || error "expected $WORKERS kind workers, found ${workers[*]}"

		hosts_block="    hosts {"$'\n'
		for i in "${!workers[@]}"; do
			name="${per_node_names[$i]}"
			ip="$(kubectl get node "${workers[$i]}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
			kubectl label node "${workers[$i]}" "${ALTERNATE_ACCESS_LABEL}=${name}" --overwrite
			hosts_block+="        $ip $name"$'\n'
			echo "Worker ${workers[$i]} ($ip) labelled ${ALTERNATE_ACCESS_LABEL}=${name}"
		done
		hosts_block+="        fallthrough"$'\n'"    }"

		corefile="$(kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}')"
		BLOCK="$hosts_block" awk '
			{ print }
			! done && /^\.:53[[:space:]]*\{[[:space:]]*$/ { print ENVIRON["BLOCK"]; done = 1 }
			END { exit done ? 0 : 1 }
		' <<< "$corefile" > "$WORK_DIR/Corefile" || error "Corefile has no '.:53 {' server block"
		kubectl -n kube-system create configmap coredns --from-file="Corefile=$WORK_DIR/Corefile" \
			--dry-run=client -o yaml | kubectl apply -f -
		kubectl -n kube-system rollout restart deploy/coredns
		kubectl -n kube-system rollout status deploy/coredns --timeout=180s

		run_group "Install cert-manager and AKO" install_ako
		kubectl -n "$AKO_NAMESPACE" rollout status deploy/aerospike-kubernetes-operator --timeout=180s
		run_group "Apply AerospikeCluster" deploy_cluster
	) 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log"
	status=("${PIPESTATUS[@]}")
	set -e
	[ "${status[0]}" = "0" ] && [ "${status[2]}" = "0" ] || die "per-node setup failed (log: $WORK_DIR/setup.log)"

	port="$(kubectl get aerospikecluster -A -o jsonpath='{.items[0].spec.aerospikeConfig.network.service.port}')"
	set_discovery "${per_node_names[0]}" "" "" "${per_node_names[0]}:$port" '^TestL3PassThrough' ''
}

enable_tls() {
	L3_TLS_DIR="$WORK_DIR/tls" GITHUB_OUTPUT="$WORK_DIR/tls.out" \
		bash "$ROOT_DIR/.github/discovery/enable-l3-tls.sh" 2>&1 | tee "$WORK_DIR/tls.log" ||
		die "enable-l3-tls.sh failed (log: $WORK_DIR/tls.log)"
	export DISCOVERY_TLS_CA_FILE="$(output ca-file "$WORK_DIR/tls.out")"
	export DISCOVERY_TLS_NAME="$(output tls-name "$WORK_DIR/tls.out")"
	TEST_RUN='^TestL3Tls'
	TEST_SKIP=''
}

dump_cluster() {
	echo "Cluster:"
	kubectl get nodes -o wide
	kubectl get nodes -L "aerospike.com/configured-alternate-access-address"
	kubectl -n aerospike get aerospikecluster,pods,svc -o wide
	local pod
	for pod in $(kubectl -n aerospike get pods -l aerospike.com/cr -o jsonpath='{.items[*].metadata.name}'); do
		echo "Rendered by the operator for $pod:"
		kubectl --request-timeout=30s -n aerospike exec "$pod" -c aerospike-server -- \
			grep -E 'access-(address|port)' /etc/aerospike/aerospike.conf || true
	done
}

run_client() {
	local log="$WORK_DIR/l3.log" rc client_args test_args name address
	echo "Running $TEST_RUN, $PROFILE profile, seed $L3_SEED"
	set +e
	"$DOCKER" cp "$CLUSTER-control-plane:/usr/bin/kubectl" "$WORK_DIR/bin/kubectl" &&
		kind get kubeconfig --internal --name "$CLUSTER" > "$WORK_DIR/kubeconfig-internal"
	rc=$?
	chmod +x "$WORK_DIR/bin/kubectl" 2>/dev/null || true
	client_args=(--rm --name "$CLIENT" --network "$KIND_NETWORK"
		-v "$WORK_DIR/l3.test:/l3.test:ro" -v "$WORK_DIR/bin/kubectl:/usr/local/bin/kubectl:ro"
		-v "$WORK_DIR/kubeconfig-internal:/kubeconfig:ro"
		--add-host "$CLUSTER-control-plane:$("$DOCKER" inspect -f "{{(index .NetworkSettings.Networks \"$KIND_NETWORK\").IPAddress}}" "$CLUSTER-control-plane")")
	if [ -n "${DISCOVERY_TLS_CA_FILE:-}" ]; then
		client_args+=(-v "$DISCOVERY_TLS_CA_FILE:/tls/ca.crt:ro")
		export DISCOVERY_TLS_CA_FILE=/tls/ca.crt
	fi
	while read -r name address; do
		[ -n "$name" ] || continue
		client_args+=(--add-host "$name:$address")
		echo "Client resolves $name to $address"
	done < <(kubectl get nodes -l "aerospike.com/configured-alternate-access-address" -o \
		jsonpath='{range .items[*]}{.metadata.labels.aerospike\.com/configured-alternate-access-address} {.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}')
	test_args=(/l3.test -test.v -test.count=1 -test.timeout 20m -test.run "$TEST_RUN")
	if [ -n "${TEST_SKIP:-}" ]; then
		test_args+=(-test.skip "$TEST_SKIP")
	fi
	if [ $rc -eq 0 ]; then
		"$DOCKER" run "${client_args[@]}" \
			-e DISCOVERY_ENDPOINT_HOSTNAME \
			-e DISCOVERY_INGRESS_PORTS \
			-e DISCOVERY_SERVICE_NODE_PORTS \
			-e DISCOVERY_KUBECONFIG \
			-e DISCOVERY_TLS_CA_FILE \
			-e DISCOVERY_TLS_NAME \
			-e AEROSPIKE_ADMIN_PASSWORD \
			-e AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1 \
			"$CLIENT_IMAGE" \
			"${test_args[@]}" > "$log" 2>&1
		rc=$?
	fi
	set -e
	grep -E '^=== RUN|^--- |^FAIL|^ok ' "$log" || true
	echo "test log: $log"
	if [ $rc -ne 0 ]; then
		echo "RESULT FAIL"
		exit 1
	fi
	echo "RESULT PASS"
}

validate
rm -rf "$WORK_DIR"
mkdir -p "$ACTION_DIR/templates" "$WORK_DIR/bin" "$WORK_DIR/helm"
fetch_action
strip_hosts_from_action
read_action_defaults
compile_tests

"$DOCKER" network inspect "$KIND_NETWORK" >/dev/null 2>&1 && KIND_NETWORK_EXISTED=1
trap cleanup EXIT
trap 'exit 130' INT TERM

export KUBECONFIG="$WORK_DIR/kubeconfig"
export RUNNER_TEMP="$WORK_DIR"
export HELM_CONFIG_HOME="$WORK_DIR/helm/config"
export HELM_CACHE_HOME="$WORK_DIR/helm/cache"
export HELM_DATA_HOME="$WORK_DIR/helm/data"
FEATURES_CONTENT="$(cat "$FEATURES_CONF_PATH")"

prepare
create_kind_cluster
load_server_image
if [ "$PROFILE" = "per-node" ]; then
	setup_per_node
else
	setup_shared
fi
[ "$PROFILE" = "tls" ] && enable_tls
dump_cluster
run_client

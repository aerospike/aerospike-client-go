#!/usr/bin/env bash
# shellcheck disable=SC2155 # export VAR="$(output ...)" must not abort when output is empty
#
# Local L2 runner. CI does not call this. Not part of the SDK build.
#
# Same Toxiproxy split-network topology as discovery-l2.yaml: nodes have no
# published ports, the client has no route to node IPs, one listener per node
# behind one hostname. Compiles a linux l2.test and runs
# .github/discovery/run-l2-suite.sh in debian:bookworm-slim. Removes every
# container and network it created on exit.
#
# Needs docker, bash 4 and go (unless L2_SKIP_BUILD=1 and a linux l2.test is in
# the repo root). Work directory: test/discovery/.run.
#
# Usage:
#   test/discovery/run_l2.sh
#   L2_PROFILE=reachable test/discovery/run_l2.sh
#   DISCOVERY_SUBSET=ee FEATURES_CONF_PATH=/path/features.conf test/discovery/run_l2.sh
#
# Every TestL2* skips unless its profile's DISCOVERY_* variable is set. Default
# rows also skip when another profile's variable is set.
#
# Env:
#   L2_PROFILE                  profile below (default: default)
#   DISCOVERY_SUBSET            ce (default) or ee
#   FEATURES_CONF_PATH          Enterprise feature-key file. Required for ee. Never FEATURES_CONF env
#   L2_DNS                      1 starts DNS for N-4 (default 1 on default, 0 otherwise)
#   L2_NODE_CONTROL             1 lets N-11 start a node. Some profiles turn this on
#   L2_SKIP_BUILD               1 uses ./l2.test (must already be linux)
#   AEROSPIKE_IMAGE             server image override
#   SHARED_WORKFLOWS_DIR        local shared-workflows clone instead of downloading the action
#   L2_DISABLE_LISTENERS        1 disables every Toxiproxy listener (N-1 guard). Tests then fail
#   L2_UNRESOLVED_HOSTNAME      pass-through-ports only. 1 gives the nodes no host entry for the
#                               access hostname, so the servers cannot resolve it (default 0)
#   L2_VERBATIM_ACCESS_ADDRESS  unresolved-access only. 1 tells the tests the server image
#                               advertises access addresses verbatim (default 0)
#   L2_SUBNET                   port-map-range node subnet, three octets (default 10.213.66)
#   L2_PORT_MAP_PORT_BASE       port-map-range listener port is this base plus the node's last
#                               octet (default 31000)
#   L2_CLIENT_NETWORK_MTU       low-mtu client network MTU (default 1200)
#   L2_PROXY_PORT_RANGE         port-range Toxiproxy source ports, "<first> <last>"
#                               (default "40000 40009")
#
# Profiles (L2_PROFILE: what it builds, tests):
#   default             one hostname, DNS on, node control opt-in. N-1..N-14, F-4. N-6 on ee
#   reachable           last node reachable as as-reachable.test. N-10
#   pass-through-names  each node node-N.pass-through.test. N-15
#   pass-through-ports  one hostname, own ports, node control. N-16, N-18
#   port-map-range      fixed subnet, listener per IP, node control. N-19
#   low-mtu             client network MTU 1200. F-2
#   port-range          Toxiproxy source ports 40000 40009. F-3
#   idle-reset          default topology plus DISCOVERY_IDLE_RESET. F-1
#   cn-only             ee only. Certificate is CN, no SAN. N-17
#   unresolved-access   no access-address, node control. Later tests, not CI
#
# On ee, every profile except cn-only uses a SAN-only certificate (aerospike-tls
# as dNSName, CN does not match).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

SHARED_WORKFLOWS_COMMIT=318e453bea7f3c068ead24057dab07312a7fe62e
ACTION_PATH=.github/actions/setup-discovery-topology
CLIENT_IMAGE=debian:bookworm-slim
CURL_IMAGE=curlimages/curl:8.11.1
DNS_IMAGE="${L2_DNS_IMAGE:-coredns/coredns:1.11.3}"
SUBSET="${DISCOVERY_SUBSET:-ce}"
PROFILE="${L2_PROFILE:-default}"
REACHABLE_HOSTNAME=as-reachable.test
PASS_THROUGH_HOSTNAME=as-pass-through.test
PASS_THROUGH_DOMAIN=pass-through.test
UNRESOLVED_HOSTNAME=as-unresolved.test
ACCESS_HOSTNAME=""
UNRESOLVED="${L2_UNRESOLVED_HOSTNAME:-0}"
VERBATIM="${L2_VERBATIM_ACCESS_ADDRESS:-0}"
SUBNET="${L2_SUBNET:-10.213.66}"
RANGE_FIRST=11
RANGE_LAST=26
PORT_MAP_PORT_BASE="${L2_PORT_MAP_PORT_BASE:-31000}"
CLIENT_NETWORK_MTU=""
PROXY_SYSCTLS=""
IDLE_RESET=""
PROXY_PORT_RANGE=""
MAX_RECORD_SIZE=""
NODE_CONTROL="${L2_NODE_CONTROL:-0}"
CERT_SHAPE=""
PREFIX="discovery-l2-$$"
WORK_DIR="$ROOT_DIR/test/discovery/.run"
AS_NETWORK="$PREFIX-aerospike-net"
CLIENT_NETWORK="$PREFIX-client-net"
PROXY="$PREFIX-toxiproxy"
CLIENT="$PREFIX-client"
DNS="$PREFIX-dns"
CONTROL_DIR="$WORK_DIR/control"
NODES=3
ENABLE_DNS="${L2_DNS:-}"
STARTED=()
STARTED_NETWORKS=()
CONTROL_PID=""
CONTROL_EXTRA=""
TLS_DIR=""
FEATURES=""
container_names=""
mesh_seeds=""
client_args=()
node_hosts=()
run_args=()

configure_subset() {
	case "$SUBSET" in
		ce)
			IMAGE="${AEROSPIKE_IMAGE:-aerospike/aerospike-server:latest}"
			TLS_LISTEN_PORT_BASE=""
			;;
		ee)
			IMAGE="${AEROSPIKE_IMAGE:-aerospike/aerospike-server-enterprise:latest}"
			TLS_LISTEN_PORT_BASE=43331
			[ -n "${FEATURES_CONF_PATH:-}" ] || die "DISCOVERY_SUBSET=ee needs FEATURES_CONF_PATH"
			[ -r "$FEATURES_CONF_PATH" ] || die "FEATURES_CONF_PATH '$FEATURES_CONF_PATH' is not readable"
			;;
		*)
			die "DISCOVERY_SUBSET must be ce or ee (got: $SUBSET)"
			;;
	esac
}

configure_profile() {
	case "$PROFILE" in
		default)
			[ -n "$ENABLE_DNS" ] || ENABLE_DNS=1
			;;
		reachable|pass-through-names) ;;
		idle-reset) IDLE_RESET=1 ;;
		pass-through-ports)
			NODE_CONTROL=1
			ACCESS_HOSTNAME="$PASS_THROUGH_HOSTNAME"
			;;
		port-map-range) NODE_CONTROL=1 ;;
		unresolved-access)
			NODE_CONTROL=1
			ACCESS_HOSTNAME="$UNRESOLVED_HOSTNAME"
			;;
		cn-only) [ "$SUBSET" = "ee" ] || die "L2_PROFILE=cn-only needs DISCOVERY_SUBSET=ee" ;;
		low-mtu)
			CLIENT_NETWORK_MTU="${L2_CLIENT_NETWORK_MTU:-1200}"
			MAX_RECORD_SIZE="max-record-size 8M"
			[[ "$CLIENT_NETWORK_MTU" =~ ^[0-9]+$ ]] || die "L2_CLIENT_NETWORK_MTU must be a number (got: $CLIENT_NETWORK_MTU)"
			;;
		port-range)
			PROXY_PORT_RANGE="${L2_PROXY_PORT_RANGE:-40000 40009}"
			[[ "$PROXY_PORT_RANGE" =~ ^[0-9]+\ [0-9]+$ ]] || die "L2_PROXY_PORT_RANGE must be '<first> <last>' (got: $PROXY_PORT_RANGE)"
			PROXY_SYSCTLS="net.ipv4.ip_local_port_range=$PROXY_PORT_RANGE"
			;;
		*) die "L2_PROFILE must be default, reachable, pass-through-names, pass-through-ports, port-map-range, cn-only, low-mtu, port-range, idle-reset or unresolved-access (got: $PROFILE)" ;;
	esac
	[ -n "$ENABLE_DNS" ] || ENABLE_DNS=0
	[[ "$ENABLE_DNS" =~ ^[01]$ ]] || die "L2_DNS must be 0 or 1 (got: $ENABLE_DNS)"
	[[ "$UNRESOLVED" =~ ^[01]$ ]] || die "L2_UNRESOLVED_HOSTNAME must be 0 or 1 (got: $UNRESOLVED)"
	[ "$UNRESOLVED" = "0" ] || [ "$PROFILE" = "pass-through-ports" ] || die "L2_UNRESOLVED_HOSTNAME=1 needs L2_PROFILE=pass-through-ports"
	[[ "$VERBATIM" =~ ^[01]$ ]] || die "L2_VERBATIM_ACCESS_ADDRESS must be 0 or 1 (got: $VERBATIM)"
	[[ "$SUBNET" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "L2_SUBNET must be three octets (got: $SUBNET)"
	[[ "$PORT_MAP_PORT_BASE" =~ ^[0-9]+$ ]] || die "L2_PORT_MAP_PORT_BASE must be a port (got: $PORT_MAP_PORT_BASE)"
}

require_prereqs() {
	command -v docker >/dev/null 2>&1 || die "docker CLI not found"
	docker info >/dev/null 2>&1 || die "docker daemon not reachable"
	((BASH_VERSINFO[0] >= 4)) || die "bash 4 or later is required by setup-discovery-topology"
}

cleanup() {
	if [ -n "$CONTROL_PID" ]; then
		kill "$CONTROL_PID" >/dev/null 2>&1 || true
		wait "$CONTROL_PID" 2>/dev/null || true
	fi
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for c in $(docker ps -aq --filter "name=^$PREFIX-" 2>/dev/null); do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for n in "${STARTED_NETWORKS[@]+"${STARTED_NETWORKS[@]}"}"; do
		docker network rm "$n" >/dev/null 2>&1 || true
	done
}

compile_l2_test() {
	if [ "${L2_SKIP_BUILD:-0}" = "1" ]; then
		[ -x "$ROOT_DIR/l2.test" ] || die "L2_SKIP_BUILD=1 but $ROOT_DIR/l2.test is missing"
		cp "$ROOT_DIR/l2.test" "$WORK_DIR/l2.test"
		chmod +x "$WORK_DIR/l2.test"
		return
	fi
	command -v go >/dev/null 2>&1 || die "go not found"
	arch="$(docker version -f '{{.Server.Arch}}')"
	case "$arch" in
		amd64|arm64) ;;
		*) die "unsupported docker architecture '$arch'" ;;
	esac
	echo "Compiling L2 tests for linux/$arch"
	(cd "$ROOT_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -c -o "$WORK_DIR/l2.test" .) ||
		die "go test -c failed"
	chmod +x "$WORK_DIR/l2.test"
}

fetch_topology_action() {
	local f
	for f in entrypoint.sh proxy_map.sh; do
		if [ -n "${SHARED_WORKFLOWS_DIR:-}" ]; then
			git -C "$SHARED_WORKFLOWS_DIR" show "$SHARED_WORKFLOWS_COMMIT:$ACTION_PATH/$f" > "$WORK_DIR/action/$f" ||
				die "$f not found at $SHARED_WORKFLOWS_COMMIT in $SHARED_WORKFLOWS_DIR"
		else
			curl -sfL "https://raw.githubusercontent.com/aerospike/shared-workflows/$SHARED_WORKFLOWS_COMMIT/$ACTION_PATH/$f" \
				-o "$WORK_DIR/action/$f" || die "failed to download $ACTION_PATH/$f at $SHARED_WORKFLOWS_COMMIT"
		fi
	done
}

write_curl_shim() {
	# Toxiproxy's control API is on the node network. This curl runs there so
	# the action works when the host cannot route to container addresses.
	cat > "$WORK_DIR/bin/curl" <<EOF
#!/usr/bin/env bash
exec docker run --rm --network "$AS_NETWORK" "$CURL_IMAGE" "\$@"
EOF
	chmod +x "$WORK_DIR/bin/curl"
}

generate_ee_tls() {
	# san-only: aerospike-tls as a dNSName, CN does not match. cn-only: CN only.
	if [ "$PROFILE" = "cn-only" ]; then
		CERT_SHAPE=cn-only
		cert_cn=aerospike-tls
	else
		CERT_SHAPE=san-only
		cert_cn=not-the-tls-name
	fi
	TLS_DIR="$WORK_DIR/tls"
	mkdir -p "$TLS_DIR"
	{
		echo "[v3_req]"
		echo "subjectAltName = @alt_names"
		echo "[alt_names]"
		for i in $(seq 1 "$NODES"); do
			echo "DNS.$i = $PREFIX-$i"
		done
		echo "DNS.$((NODES + 1)) = localhost"
		echo "DNS.$((NODES + 2)) = docker"
		[ "$CERT_SHAPE" = "san-only" ] && echo "DNS.$((NODES + 3)) = aerospike-tls"
		echo "IP.1 = 127.0.0.1"
	} > "$TLS_DIR/san.ext"
	(
		cd "$TLS_DIR"
		openssl req -x509 -newkey rsa:4096 -nodes -keyout ca.key -out ca.crt -days 365 \
			-subj "/CN=Aerospike-Server-Test-CA"
		openssl req -newkey rsa:4096 -nodes -keyout server.key -out server.csr -subj "/CN=$cert_cn"
		openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 \
			-extensions v3_req -extfile san.ext
		rm -f ./*.csr ./*.srl san.ext
	) > "$WORK_DIR/openssl.log" 2>&1 || { cat "$WORK_DIR/openssl.log" >&2; die "certificate generation failed"; }
	chmod 644 "$TLS_DIR"/*
	echo "Server certificate ($CERT_SHAPE):"
	openssl x509 -in "$TLS_DIR/server.crt" -noout -subject -ext subjectAltName
	run_args+=(-v "$TLS_DIR:/etc/aerospike/tls:ro")
}

write_server_conf() {
	local feature_key="" tls_config="" tls_service="" i
	container_names=""
	mesh_seeds=""
	for i in $(seq 1 "$NODES"); do
		container_names+="${container_names:+,}$PREFIX-$i"
		mesh_seeds+="        mesh-seed-address-port $PREFIX-$i 3002"$'\n'
	done
	run_args=()
	if [ "$SUBSET" = "ee" ]; then
		FEATURES="$(cd "$(dirname "$FEATURES_CONF_PATH")" && pwd)/$(basename "$FEATURES_CONF_PATH")"
		feature_key="feature-key-file /etc/aerospike/features.conf"
		run_args+=(-v "$FEATURES:/etc/aerospike/features.conf:ro")
		generate_ee_tls
		tls_config="    tls aerospike-tls {
        cert-file /etc/aerospike/tls/server.crt
        key-file  /etc/aerospike/tls/server.key
        ca-file   /etc/aerospike/tls/ca.crt
    }"
		tls_service="        tls-port 4333
        tls-authenticate-client false
        tls-name aerospike-tls"
	fi
	cat > "$WORK_DIR/conf/aerospike.conf" <<EOF
service {
    $feature_key
    cluster-name docker
}

logging {
    console {
        context any info
    }
}

network {
$tls_config
    service {
        address any
        port 3000
$tls_service
    }
    heartbeat {
        mode mesh
        address any
        port 3002
        interval 150
        timeout 10
$mesh_seeds    }
    fabric {
        address any
        port 3001
    }
}

namespace test {
    replication-factor 2
    $MAX_RECORD_SIZE
    storage-engine memory {
        data-size 1G
    }
}
EOF
}

wait_cluster_size() {
	local name="$1" size="$2" i
	for i in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "CLUSTER-SIZE $size" >/dev/null; then
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name did not reach cluster size $size after 60s"
}

start_node() {
	local name="$1" conf="$2"
	shift 2
	docker run -d --name "$name" --network "$AS_NETWORK" --ulimit nofile=15000 "$@" \
		-v "$WORK_DIR/conf:/etc/aerospike/conf:ro" "${run_args[@]+"${run_args[@]}"}" \
		--entrypoint asd "$IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
}

container_address() {
	docker inspect -f "{{(index .NetworkSettings.Networks \"$2\").IPAddress}}" "$1"
}

render_conf() {
	ACCESS="$2" awk '{print} $1 == "port" && $2 == "3000" {printf "%s", ENVIRON["ACCESS"]}' \
		"$WORK_DIR/conf/aerospike.conf" > "$WORK_DIR/conf/$1"
}

alternate_access() {
	printf '        alternate-access-address %s\n        alternate-access-port %s\n' "$1" "$2"
}

assert_unresolved() {
	local rc=0
	docker exec "$1" getent hosts "$ACCESS_HOSTNAME" > "$WORK_DIR/getent.out" 2>&1 || rc=$?
	[ "$rc" = "2" ] || die "$ACCESS_HOSTNAME resolves in $1 or the lookup failed (getent exit $rc): $(cat "$WORK_DIR/getent.out")"
	echo "Node $1: getent hosts $ACCESS_HOSTNAME exit $rc, not found"
}

control_start() {
	local arg="$1" ack="$2" name conf option error="" options=()
	CONTROL_EXTRA=$((CONTROL_EXTRA + 1))
	name="$PREFIX-$CONTROL_EXTRA"
	conf=aerospike.conf
	for option in $arg; do
		case "$option" in
			alternate-port=*)
				if [ -n "$ACCESS_HOSTNAME" ] && [[ "${option#*=}" =~ ^[0-9]+$ ]]; then
					conf="$name.conf"
					render_conf "$conf" "$(alternate_access "$ACCESS_HOSTNAME" "${option#*=}")"$'\n'
				else
					error="$option needs a port and L2_PROFILE=pass-through-ports or unresolved-access"
				fi
				;;
			address=*)
				if [[ "${option#*=}" =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
					options+=(--ip "${option#*=}")
				else
					error="$option is not an IPv4 address"
				fi
				;;
			*)
				error="unknown start option $option"
				;;
		esac
	done
	if [ -n "$error" ]; then
		printf 'error=%s\n' "$error" > "$ack"
	elif start_node "$name" "$conf" "${options[@]+"${options[@]}"}" "${node_hosts[@]+"${node_hosts[@]}"}" \
		2> "$WORK_DIR/control.err"; then
		printf 'container=%s\naddress=%s\n' "$name" "$(container_address "$name" "$AS_NETWORK")" > "$ack"
		echo "Control: started $name $arg"
	else
		printf 'error=%s\n' "$(tr '\n' ' ' < "$WORK_DIR/control.err")" > "$ack"
	fi
}

control_stop() {
	local arg="$1" ack="$2"
	if [[ "$arg" == "$PREFIX"-* ]] && docker rm -f "$arg" > /dev/null 2> "$WORK_DIR/control.err"; then
		printf 'removed=%s\n' "$arg" > "$ack"
		echo "Control: removed $arg"
	else
		printf 'error=cannot remove %s %s\n' "$arg" "$(tr '\n' ' ' < "$WORK_DIR/control.err" 2>/dev/null)" > "$ack"
	fi
}

control_status() {
	local id="$1" arg="$2" ack="$3" state lookup="" lookup_rc="" where=""
	if [[ "$arg" == "$PREFIX"-* ]] &&
		state="$(docker inspect -f '{{.State.Running}} {{.State.ExitCode}}' "$arg" 2> "$WORK_DIR/control.err")"; then
		docker logs "$arg" > "$CONTROL_DIR/$arg.log" 2>&1 || true
		if [ -n "$ACCESS_HOSTNAME" ]; then
			lookup_rc=0
			if [ "${state% *}" = "true" ]; then
				where=container
				lookup="$(docker exec "$arg" getent hosts "$ACCESS_HOSTNAME" 2>&1)" || lookup_rc=$?
			else
				where=image
				lookup="$(docker run --rm --name "$PREFIX-lookup-$id" --network "$AS_NETWORK" \
					"${node_hosts[@]+"${node_hosts[@]}"}" --entrypoint getent "$IMAGE" \
					hosts "$ACCESS_HOSTNAME" 2>&1)" || lookup_rc=$?
			fi
		fi
		printf 'running=%s\nexit-code=%s\nlog=%s\nlookup-in=%s\nlookup-exit=%s\nlookup=%s\n' \
			"${state% *}" "${state#* }" "$CONTROL_DIR/$arg.log" "$where" "$lookup_rc" \
			"$(tr '\n' ' ' <<< "$lookup")" > "$ack"
	else
		printf 'error=cannot inspect %s %s\n' "$arg" "$(tr '\n' ' ' < "$WORK_DIR/control.err" 2>/dev/null)" > "$ack"
	fi
}

control_loop() {
	local request id action arg ack
	CONTROL_EXTRA="$NODES"
	while true; do
		for request in "$CONTROL_DIR"/*.request; do
			[ -e "$request" ] || continue
			id="$(basename "$request" .request)"
			action=""
			arg=""
			read -r action arg < "$request" || true
			rm -f "$request"
			ack="$CONTROL_DIR/$id.tmp"
			case "$action" in
				start) control_start "$arg" "$ack" ;;
				stop) control_stop "$arg" "$ack" ;;
				status) control_status "$id" "$arg" "$ack" ;;
				*) printf 'error=unknown request: %s %s\n' "$action" "$arg" > "$ack" ;;
			esac
			mv "$ack" "$CONTROL_DIR/$id.ack"
		done
		sleep 0.1
	done
}

start_cluster() {
	local container octet=$RANGE_FIRST network_args=()
	echo "Starting $NODES node $SUBSET cluster on $AS_NETWORK, no published ports ($IMAGE)"
	STARTED_NETWORKS+=("$CLIENT_NETWORK" "$AS_NETWORK")
	[ "$PROFILE" = "port-map-range" ] && network_args=(--subnet "$SUBNET.0/24" --ip-range "$SUBNET.128/25")
	docker network create "${network_args[@]+"${network_args[@]}"}" "$AS_NETWORK" >/dev/null
	IFS=',' read -r -a containers <<< "$container_names"
	for container in "${containers[@]}"; do
		STARTED+=("$container")
		if [ "$PROFILE" = "port-map-range" ]; then
			start_node "$container" aerospike.conf --ip "$SUBNET.$octet"
			octet=$((octet + 1))
		else
			start_node "$container" aerospike.conf
		fi
	done
	for container in "${containers[@]}"; do
		wait_cluster_size "$container" "$NODES"
	done
}

setup_proxy_topology() {
	echo "Setting up discovery topology (shared-workflows $SHARED_WORKFLOWS_COMMIT)"
	STARTED=("$CLIENT" "$PROXY" "${STARTED[@]}")
	: > "$WORK_DIR/topology.out"
	PATH="$WORK_DIR/bin:$PATH" \
		CONTAINER_NAMES="$container_names" \
		AEROSPIKE_NETWORK_NAME="$AS_NETWORK" \
		CLIENT_NETWORK_NAME="$CLIENT_NETWORK" \
		PROXY_CONTAINER_NAME="$PROXY" \
		TLS_LISTEN_PORT_BASE="$TLS_LISTEN_PORT_BASE" \
		CLIENT_NETWORK_MTU="$CLIENT_NETWORK_MTU" \
		PROXY_SYSCTLS="$PROXY_SYSCTLS" \
		GITHUB_OUTPUT="$WORK_DIR/topology.out" \
		bash "$WORK_DIR/action/entrypoint.sh" || die "setup-discovery-topology failed"
}

output() {
	grep -E "^$1=" "$WORK_DIR/topology.out" | head -1 | cut -d= -f2-
}

export_discovery_env() {
	export DISCOVERY_SUBSET="$SUBSET"
	export DISCOVERY_ENDPOINT_HOSTNAME="$(output endpoint-hostname)"
	export DISCOVERY_ENDPOINT_PORT_BASE="$(output listen-port-base)"
	export DISCOVERY_ENDPOINT_PORTS="$(output endpoint-ports)"
	export DISCOVERY_TLS_ENDPOINT_PORTS="$(output tls-endpoint-ports)"
	export DISCOVERY_PROXY_MAP="$(output proxy-name-port-map)"
	export TOXIPROXY_URL="$(output toxiproxy-url)"
	export TOXIPROXY_HOST="$(output toxiproxy-container-name)"
	export TOXIPROXY_CONTROL_PORT=8474
	export AEROSPIKE_CONTAINER_NAMES="$container_names"
	export L2_DISABLE_LISTENERS="${L2_DISABLE_LISTENERS:-0}"
	export L2_PROFILE="$PROFILE"
	export DISCOVERY_DNS_SERVER=""
	export DISCOVERY_CONTROL_DIR=""
	export DISCOVERY_REACHABLE_NODE=""
	export DISCOVERY_PASS_THROUGH_BRANCH=""
	export DISCOVERY_PORT_MAP_RANGE=""
	export DISCOVERY_PORT_MAP_PORT_BASE=""
	export DISCOVERY_TLS_CERT_SHAPE="$CERT_SHAPE"
	export DISCOVERY_CLIENT_NETWORK_MTU="$CLIENT_NETWORK_MTU"
	export DISCOVERY_IDLE_RESET="$IDLE_RESET"
	export DISCOVERY_PROXY_PORT_RANGE="$PROXY_PORT_RANGE"
	export DISCOVERY_UNRESOLVED_HOSTNAME=""
	export DISCOVERY_VERBATIM_ACCESS_ADDRESS=""
}

listener_port_for() {
	local container="$1" entry port=""
	IFS=',' read -r -a proxies <<< "$DISCOVERY_PROXY_MAP"
	for entry in "${proxies[@]}"; do
		[ "${entry%:*}" = "$container" ] && port="${entry##*:}"
	done
	[ -n "$port" ] || die "no listener for $container in $DISCOVERY_PROXY_MAP"
	echo "$port"
}

recreate_nodes_with_access() {
	local container
	if [ "$UNRESOLVED" = "1" ]; then
		echo "Recreating the nodes with no extra host for ${hostnames[*]}"
	else
		echo "Recreating the nodes with ${hostnames[*]} -> $proxy_server_address"
	fi
	for container in "${containers[@]}"; do
		docker rm -f "$container" >/dev/null
	done
	for container in "${containers[@]}"; do
		start_node "$container" "$container.conf" "${node_hosts[@]}"
	done
	for container in "${containers[@]}"; do
		wait_cluster_size "$container" "$NODES"
	done
	if [ "$UNRESOLVED" = "1" ]; then
		for container in "${containers[@]}"; do
			assert_unresolved "$container"
		done
	fi
}

node_access_stanza() {
	local container="$1" port="$2" index="$3"
	case "$PROFILE" in
		reachable)
			if [ "$container" = "$DISCOVERY_REACHABLE_NODE" ]; then
				printf '        access-address %s\n        access-port %s\n' "$REACHABLE_HOSTNAME" "$port"
			else
				alternate_access "$REACHABLE_HOSTNAME" "$port"
				printf '\n'
			fi
			;;
		pass-through-names)
			hostnames+=("node-$index.$PASS_THROUGH_DOMAIN")
			alternate_access "node-$index.$PASS_THROUGH_DOMAIN" "$port"
			printf '\n'
			;;
		pass-through-ports)
			alternate_access "$PASS_THROUGH_HOSTNAME" "$port"
			printf '\n'
			;;
	esac
}

overlay_named_hosts() {
	local proxy_server_address proxy_client_address hostname index=0 container port
	proxy_server_address="$(container_address "$PROXY" "$AS_NETWORK")"
	proxy_client_address="$(container_address "$PROXY" "$CLIENT_NETWORK")"
	[ -n "$proxy_server_address" ] && [ -n "$proxy_client_address" ] || die "no Toxiproxy address"
	hostnames=()
	case "$PROFILE" in
		reachable)
			DISCOVERY_REACHABLE_NODE="${containers[$((NODES - 1))]}"
			hostnames=("$REACHABLE_HOSTNAME")
			;;
		pass-through-names)
			DISCOVERY_PASS_THROUGH_BRANCH=A
			;;
		pass-through-ports)
			DISCOVERY_PASS_THROUGH_BRANCH=B
			hostnames=("$PASS_THROUGH_HOSTNAME")
			;;
	esac
	for container in "${containers[@]}"; do
		index=$((index + 1))
		port="$(listener_port_for "$container")"
		render_conf "$container.conf" "$(node_access_stanza "$container" "$port" "$index")"
		echo "Node $container listener $port:"
		grep -E "access-(address|port)" "$WORK_DIR/conf/$container.conf"
	done
	for hostname in "${hostnames[@]}"; do
		[ "$UNRESOLVED" = "1" ] || node_hosts+=(--add-host "$hostname:$proxy_server_address")
		client_args+=(--add-host "$hostname:$proxy_client_address")
	done
	recreate_nodes_with_access
}

add_port_map_listeners() {
	local octet container
	DISCOVERY_PORT_MAP_RANGE="$SUBNET.$RANGE_FIRST-$SUBNET.$RANGE_LAST"
	DISCOVERY_PORT_MAP_PORT_BASE="$PORT_MAP_PORT_BASE"
	echo "Adding a Toxiproxy listener on port $PORT_MAP_PORT_BASE + last octet for $DISCOVERY_PORT_MAP_RANGE"
	for octet in $(seq "$RANGE_FIRST" "$RANGE_LAST"); do
		"$WORK_DIR/bin/curl" -sf -X POST "$TOXIPROXY_URL/proxies" -H "Content-Type: application/json" \
			-d "{\"name\":\"range-$SUBNET.$octet\",\"listen\":\"0.0.0.0:$((PORT_MAP_PORT_BASE + octet))\",\"upstream\":\"$SUBNET.$octet:3000\",\"enabled\":true}" \
			> /dev/null || die "failed to add the listener for $SUBNET.$octet"
	done
	for container in "${containers[@]}"; do
		echo "Node $container address $(container_address "$container" "$AS_NETWORK")"
	done
}

apply_profile() {
	case "$PROFILE" in
		reachable|pass-through-names|pass-through-ports) overlay_named_hosts ;;
		port-map-range) add_port_map_listeners ;;
		low-mtu)
			echo "Client network $CLIENT_NETWORK options: $(docker network inspect -f '{{json .Options}}' "$CLIENT_NETWORK")"
			;;
		port-range)
			echo "Toxiproxy net.ipv4.ip_local_port_range: $(docker run --rm --network "container:$PROXY" "$CURL_IMAGE" \
				cat /proc/sys/net/ipv4/ip_local_port_range)"
			;;
		unresolved-access)
			DISCOVERY_UNRESOLVED_HOSTNAME="$UNRESOLVED_HOSTNAME"
			DISCOVERY_VERBATIM_ACCESS_ADDRESS="$VERBATIM"
			echo "Server under test advertises access addresses verbatim: $VERBATIM ($IMAGE)"
			for container in "${containers[@]}"; do
				assert_unresolved "$container"
			done
			;;
	esac
}

start_node_control() {
	[ "$NODE_CONTROL" = "1" ] || return 0
	DISCOVERY_CONTROL_DIR="$CONTROL_DIR"
	control_loop &
	CONTROL_PID=$!
	echo "Serving node control requests in $CONTROL_DIR"
}

start_dns() {
	local proxy_client_address port
	[ "$ENABLE_DNS" = "1" ] || return 0
	proxy_client_address="$(container_address "$PROXY" "$CLIENT_NETWORK")"
	[ -n "$proxy_client_address" ] || die "no Toxiproxy address on the client network for DNS"
	mkdir -p "$WORK_DIR/dns"
	{
		echo "${DISCOVERY_ENDPOINT_HOSTNAME}.:53 {"
		echo "    file /zone"
		echo "}"
	} > "$WORK_DIR/dns/Corefile"
	{
		echo "\$ORIGIN ${DISCOVERY_ENDPOINT_HOSTNAME}."
		echo "@ 3600 IN SOA ns.${DISCOVERY_ENDPOINT_HOSTNAME}. admin.${DISCOVERY_ENDPOINT_HOSTNAME}. 1 60 60 60 1"
		echo "@ 1 IN A ${proxy_client_address}"
		IFS=',' read -r -a dns_ports <<< "$DISCOVERY_ENDPOINT_PORTS"
		for port in "${dns_ports[@]}"; do
			echo "_aerospike._tcp 1 IN SRV 10 10 ${port} ${DISCOVERY_ENDPOINT_HOSTNAME}."
		done
	} > "$WORK_DIR/dns/zone"
	echo "Starting DNS for ${DISCOVERY_ENDPOINT_HOSTNAME} -> ${proxy_client_address} ($DNS_IMAGE)"
	STARTED+=("$DNS")
	docker run -d --name "$DNS" --network "$CLIENT_NETWORK" \
		-v "$WORK_DIR/dns/Corefile:/Corefile:ro" -v "$WORK_DIR/dns/zone:/zone:ro" \
		"$DNS_IMAGE" -conf /Corefile >/dev/null || die "DNS server failed to start"
	DISCOVERY_DNS_SERVER="$(container_address "$DNS" "$CLIENT_NETWORK")"
	[ -n "$DISCOVERY_DNS_SERVER" ] || die "DNS server has no address on the client network"
	echo "DISCOVERY_DNS_SERVER=$DISCOVERY_DNS_SERVER"
}

disable_listeners() {
	local entry
	[ "$L2_DISABLE_LISTENERS" = "1" ] || return 0
	IFS=',' read -r -a proxies <<< "$DISCOVERY_PROXY_MAP"
	for entry in "${proxies[@]}"; do
		echo "Disabling Toxiproxy listener ${entry%:*}"
		"$WORK_DIR/bin/curl" -sf -X POST "$TOXIPROXY_URL/proxies/${entry%:*}" \
			-H "Content-Type: application/json" -d '{"enabled":false}' || die "failed to disable ${entry%:*}"
		echo
	done
}

init_run() {
	trap cleanup EXIT
	trap 'exit 130' INT TERM
	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR/conf" "$WORK_DIR/action" "$WORK_DIR/bin" "$CONTROL_DIR"
}

set_client_args() {
	client_args=(--rm --name "$CLIENT" --network "$CLIENT_NETWORK" --workdir "$ROOT_DIR"
		-v "$ROOT_DIR:$ROOT_DIR" -v "$WORK_DIR/l2.test:$ROOT_DIR/l2.test:ro"
		-e AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1)
}

run_suite() {
	local log="$WORK_DIR/l2.log" rc
	[ "$SUBSET" = "ee" ] && client_args+=(-v "$TLS_DIR:/aerospike-tls:ro" -e "AEROSPIKE_CA_CERT=/aerospike-tls/ca.crt")
	echo "Running the $PROFILE profile in $CLIENT on $CLIENT_NETWORK ($CLIENT_IMAGE)"
	set +e
	docker run "${client_args[@]}" \
		-e DISCOVERY_SUBSET \
		-e DISCOVERY_ENDPOINT_HOSTNAME \
		-e DISCOVERY_ENDPOINT_PORT_BASE \
		-e DISCOVERY_ENDPOINT_PORTS \
		-e DISCOVERY_TLS_ENDPOINT_PORTS \
		-e DISCOVERY_PROXY_MAP \
		-e TOXIPROXY_URL \
		-e TOXIPROXY_HOST \
		-e TOXIPROXY_CONTROL_PORT \
		-e AEROSPIKE_CONTAINER_NAMES \
		-e L2_DISABLE_LISTENERS \
		-e L2_PROFILE \
		-e DISCOVERY_CONTROL_DIR \
		-e DISCOVERY_REACHABLE_NODE \
		-e DISCOVERY_PASS_THROUGH_BRANCH \
		-e DISCOVERY_PORT_MAP_RANGE \
		-e DISCOVERY_PORT_MAP_PORT_BASE \
		-e DISCOVERY_TLS_CERT_SHAPE \
		-e DISCOVERY_CLIENT_NETWORK_MTU \
		-e DISCOVERY_IDLE_RESET \
		-e DISCOVERY_PROXY_PORT_RANGE \
		-e DISCOVERY_DNS_SERVER \
		-e DISCOVERY_UNRESOLVED_HOSTNAME \
		-e DISCOVERY_VERBATIM_ACCESS_ADDRESS \
		"$CLIENT_IMAGE" \
		bash .github/discovery/run-l2-suite.sh > "$log" 2>&1
	rc=$?
	set -e
	grep -E '^=== RUN|^--- |^FAIL|^ok ' "$log" || true
	echo "test log: $log"
	if [ $rc -ne 0 ]; then
		echo "RESULT FAIL"
		exit 1
	fi
	echo "RESULT PASS"
}

main() {
	configure_subset
	configure_profile
	require_prereqs
	init_run
	compile_l2_test
	fetch_topology_action
	write_curl_shim
	write_server_conf
	start_cluster
	setup_proxy_topology
	export_discovery_env
	set_client_args
	apply_profile
	start_node_control
	start_dns
	disable_listeners
	run_suite
}

main

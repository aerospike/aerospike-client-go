# Local L2 discovery runner

`run_l2.sh` builds the Toxiproxy split-network topology, compiles a linux
`l2.test`, and runs `.github/discovery/run-l2-suite.sh` in `debian:bookworm-slim`
on the client network. CI does not call this script. Not part of the SDK build.

Needs Docker, bash 4, and Go (unless `L2_SKIP_BUILD=1` and a linux `l2.test` is
in the repo root). Work directory: `test/discovery/.run`.

```
test/discovery/run_l2.sh
L2_PROFILE=reachable test/discovery/run_l2.sh
DISCOVERY_SUBSET=ee FEATURES_CONF_PATH=/path/to/features.conf test/discovery/run_l2.sh
```

Every `TestL2*` skips unless its profile's `DISCOVERY_*` variable is set. Default
rows also skip when another profile's variable is set.

| Variable | Meaning |
|---|---|
| `L2_PROFILE` | See profiles below. Default `default` |
| `DISCOVERY_SUBSET` | `ce` (default) or `ee` |
| `FEATURES_CONF_PATH` | Enterprise feature-key file. Required for `ee`. Never `FEATURES_CONF` env |
| `L2_DNS` | `1` starts DNS for N-4. Default `1` on `default`, `0` otherwise |
| `L2_NODE_CONTROL` | `1` lets N-11 start a node. Some profiles turn this on |
| `L2_SKIP_BUILD` | `1` uses `./l2.test` (must already be linux) |
| `AEROSPIKE_IMAGE` | Override the server image |
| `SHARED_WORKFLOWS_DIR` | Local `shared-workflows` clone instead of downloading the action |
| `L2_DISABLE_LISTENERS` | `1` disables every Toxiproxy listener (N-1 guard). Tests then fail |

## Profiles

| `L2_PROFILE` | What it builds | Tests |
|---|---|---|
| `default` | One hostname, DNS on, node control opt-in | N-1..N-14, F-4. N-6 on ee |
| `reachable` | Last node reachable as `as-reachable.test` | N-10 |
| `pass-through-names` | Each node `node-N.pass-through.test` | N-15 |
| `pass-through-ports` | One hostname, own ports, node control | N-16, N-18 |
| `port-map-range` | Fixed subnet, listener per IP, node control | N-19 |
| `low-mtu` | Client network MTU 1200 | F-2 |
| `port-range` | Toxiproxy source ports `40000 40009` | F-3 |
| `idle-reset` | Default topology plus `DISCOVERY_IDLE_RESET` | F-1 |
| `cn-only` | ee only. Cert is CN, no SAN | N-17 |
| `unresolved-access` | No access-address, node control | later tests, not CI |

On ee, every profile except `cn-only` uses a SAN-only certificate (`aerospike-tls` as dNSName, CN does not match).

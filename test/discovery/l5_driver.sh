#!/usr/bin/env bash
#
# L5 driver for aerospike-client-conformance l5-aws/DRIVER.md.
# L5_BUILD compiles l5.test. This script is L5_DRIVER.

set -euo pipefail

if [[ ! -x ./l5.test ]]; then
	echo "run L5_BUILD first" >&2
	exit 1
fi

export AEROSPIKE_TEST_SKIP_SUITE_CLIENT=1
export L5_DRIVER_RUN=1
exec ./l5.test -test.run '^TestL5Driver$' -- "$@"

//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package examples is the root of the SDK's example programs.
//
// It holds no code of its own. Each subdirectory is one example: a package
// exposing Run(*exrun.Env) error plus a cmd/ main that runs it standalone. The
// test in this directory drives every Run against a live cluster, which is what
// keeps the examples honest — an example no test executes rots silently as the
// API moves, while one the suite drives fails the build the moment a signature
// changes.
//
// Run one standalone:
//
//	AEROSPIKE_HOSTS=127.0.0.1:3000 go1.27rc2 run ./sdk/examples/batch/cmd
//
// Run all of them as tests:
//
//	go1.27rc2 test ./sdk/examples/ -args -h 127.0.0.1 -p 3000 -n test \
//	    -sc-namespace testsc -use-services-alternate
//
// See README.md for what each example shows.
package examples

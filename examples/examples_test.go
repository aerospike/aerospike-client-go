/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"flag"
	"os"
	"testing"
)

// testFacts is probed once in TestMain and shared by every subtest, matching
// how the CLI runner probes the server once for the whole run.
var testFacts serverFacts

// TestMain connects before any test runs. flag.Parse here is safe: the
// generated test binary calls testing.Init (registering every -test.* flag)
// before TestMain runs, so our own flags and the test flags coexist.
func TestMain(m *testing.M) {
	flag.Parse()
	testFacts = connectClient()
	defer client.Close()
	os.Exit(m.Run())
}

// TestExamples exposes the registry as one subtest per example, so standard
// Go test tooling (gotestsum, go test -json, IDE test runners, etc.) can run
// and report on the documentation examples the same way it does any other
// Go test.
func TestExamples(t *testing.T) {
	for _, ex := range examples {
		t.Run(ex.Name, func(t *testing.T) {
			res := execute(ex, testFacts)
			switch res.status {
			case statusSkip:
				t.Skip(res.detail)
			case statusFail:
				t.Fatal(res.detail)
			}
		})
	}
}

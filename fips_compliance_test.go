//go:build go1.24

// Copyright 2014-2022 Aerospike, Inc.
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

package aerospike_test

import (
	"crypto/fips140"
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FIPS Compliance", func() {
	Describe("FIPS mode", func() {
		BeforeEach(func() {
			// Check if FIPS environment is configured
			// This is only used when testing locally
			// and user might have not set right env variables
			godebug := os.Getenv("GODEBUG")
			if godebug == "" || (!strings.Contains(godebug, "fips140=on") && !strings.Contains(godebug, "fips140=only")) {
				Skip("FIPS test skipped: set GODEBUG=fips140=on or GODEBUG=fips140=only to run")
			}
		})

		It("should be enabled when GODEBUG is set correctly", func() {
			Expect(fips140.Enabled()).To(BeTrue(), "FIPS mode is not enabled (set GODEBUG=fips140=on or only)")
		})
	})
})

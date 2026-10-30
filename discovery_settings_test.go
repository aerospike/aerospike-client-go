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

package aerospike

import (
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Discovery setting tokens", func() {

	gg.It("parses every valid token and formats it back", func() {
		mergePolicy, err := parseSeedMergePolicy("discovery_only")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(mergePolicy).To(gm.Equal(SeedMergePolicyDiscoveryOnly))
		gm.Expect(mergePolicy.String()).To(gm.Equal("discovery_only"))

		missMode, err := parseTranslationMissMode("strict")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(missMode).To(gm.Equal(TranslationMissStrict))
		gm.Expect(missMode.String()).To(gm.Equal("strict"))

		executionMode, err := parseDiscoveryExecutionMode("tend")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(executionMode).To(gm.Equal(DiscoveryExecutionTend))
		gm.Expect(executionMode.String()).To(gm.Equal("tend"))
	})

	gg.It("defaults match the zero values", func() {
		gm.Expect(SeedMergePolicy(0).String()).To(gm.Equal("merge"))
		gm.Expect(TranslationMissMode(0).String()).To(gm.Equal("pass_through"))
		gm.Expect(DiscoveryExecutionMode(0).String()).To(gm.Equal("thread"))
	})

	gg.It("rejects unknown tokens with the valid values named", func() {
		_, err := parseSeedMergePolicy("pass-through")
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("merge, replace and discovery_only"))

		_, err = parseTranslationMissMode("advertised_address")
		gm.Expect(err).To(gm.HaveOccurred())

		_, err = parseDiscoveryExecutionMode("background")
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

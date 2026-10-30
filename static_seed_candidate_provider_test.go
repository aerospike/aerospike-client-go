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

var _ = gg.Describe("Static seed candidate provider", func() {

	gg.It("returns the configured seeds in order and never needs a refresh", func() {
		provider := NewStaticSeedCandidateProvider(
			NewHost("seed-1.internal", 3000),
			NewHost("seed-2.internal", 3100),
		)

		seeds, err := provider.RefreshSeedCandidates(&SeedCandidateContext{})

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(seeds).To(gm.HaveLen(2))
		gm.Expect(seeds[0].Name).To(gm.Equal("seed-1.internal"))
		gm.Expect(seeds[1].Name).To(gm.Equal("seed-2.internal"))
		gm.Expect(provider.NeedsPeriodicRefresh()).To(gm.BeFalse())
		gm.Expect(provider.SupportsDeadline()).To(gm.BeTrue())
	})

	gg.It("is unaffected by later changes to the caller's hosts", func() {
		callerSeed := NewHost("seed-1.internal", 3000)
		provider := NewStaticSeedCandidateProvider(callerSeed)

		callerSeed.Name = "changed.internal"
		callerSeed.Port = 9999

		seeds, err := provider.RefreshSeedCandidates(&SeedCandidateContext{})

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(seeds).To(gm.HaveLen(1))
		gm.Expect(seeds[0].Name).To(gm.Equal("seed-1.internal"))
		gm.Expect(seeds[0].Port).To(gm.Equal(3000))
	})
})

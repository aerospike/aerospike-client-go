/*
 * Copyright 2014-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package aerospike_test

import (
	internal "github.com/aerospike/aerospike-client-go/v8/internal/version"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Version", func() {
	Describe("internal.NewVersion", func() {
		Context("with valid version strings", func() {
			It("should parse full semantic versions correctly", func() {
				version, err := internal.NewVersion("8.0.1.0")
				Expect(err).ToNot(HaveOccurred())
				Expect(version.Major).To(Equal(8))
				Expect(version.Minor).To(Equal(0))
				Expect(version.Patch).To(Equal(1))
				Expect(version.Build).To(Equal(0))
			})

			It("should parse version with only major component", func() {
				version, err := internal.NewVersion("8")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}))
			})

			It("should parse version with major and minor components", func() {
				version, err := internal.NewVersion("8.1")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 8, Minor: 1, Patch: 0, Build: 0}))
			})

			It("should parse version with major, minor, and patch components", func() {
				version, err := internal.NewVersion("8.0.1")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}))
			})

			It("should handle version strings with 'v' prefix", func() {
				version, err := internal.NewVersion("v8.0.1.0")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}))
			})

			It("should parse large version numbers", func() {
				version, err := internal.NewVersion("100.200.300.400")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 100, Minor: 200, Patch: 300, Build: 400}))
			})

			It("should parse zero version", func() {
				version, err := internal.NewVersion("0.0.0.0")
				Expect(err).ToNot(HaveOccurred())
				Expect(*version).To(Equal(internal.Version{Major: 0, Minor: 0, Patch: 0, Build: 0}))
			})
		})

		Context("with invalid version strings", func() {
			DescribeTable("should return error for invalid formats",
				func(input string) {
					_, err := internal.NewVersion(input)
					Expect(err).To(HaveOccurred())
				},
				Entry("empty string", ""),
				Entry("non-numeric", "abc"),
				Entry("too many components", "1.2.3.4.5"),
				Entry("non-numeric major", "a.2.3.4"),
				Entry("non-numeric minor", "1.a.3.4"),
				Entry("non-numeric patch", "1.2.b.4"),
				Entry("non-numeric build", "1.2.3.c"),
				Entry("double dots", "1..3.4"),
				Entry("trailing dot", "1.2.3."),
				Entry("leading dot", ".1.2.3"),
				Entry("negative major", "-1.2.3.4"),
				Entry("negative minor", "1.-2.3.4"),
				Entry("negative patch", "1.2.-3.4"),
				Entry("negative build", "1.2.3.-4"),
			)
		})
	})

	Describe("String", func() {
		It("should return correct string representation", func() {
			version := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(version.String()).To(Equal("8.0.1.0"))
		})

		It("should handle zero version", func() {
			version := internal.Version{Major: 0, Minor: 0, Patch: 0, Build: 0}
			Expect(version.String()).To(Equal("0.0.0.0"))
		})

		It("should handle large numbers", func() {
			version := internal.Version{Major: 100, Minor: 200, Patch: 300, Build: 400}
			Expect(version.String()).To(Equal("100.200.300.400"))
		})
	})

	Describe("Compare", func() {
		Context("when versions are equal", func() {
			It("should return 0", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(0))
			})
		})

		Context("when first version is greater", func() {
			It("should return 1 for greater major version", func() {
				v1 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(1))
			})

			It("should return 1 for greater minor version", func() {
				v1 := internal.Version{Major: 8, Minor: 1, Patch: 0, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(1))
			})

			It("should return 1 for greater patch version", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 2, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(1))
			})

			It("should return 1 for greater build version", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 1}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(1))
			})
		})

		Context("when first version is smaller", func() {
			It("should return -1 for smaller major version", func() {
				v1 := internal.Version{Major: 7, Minor: 0, Patch: 0, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(-1))
			})

			It("should return -1 for smaller minor version", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 1, Patch: 0, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(-1))
			})

			It("should return -1 for smaller patch version", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 2, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(-1))
			})

			It("should return -1 for smaller build version", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 1}
				Expect(v1.Compare(&v2)).To(Equal(-1))
			})
		})

		Context("with complex version comparisons", func() {
			It("should prioritize major version over minor/patch/build", func() {
				v1 := internal.Version{Major: 8, Minor: 1, Patch: 0, Build: 0}
				v2 := internal.Version{Major: 8, Minor: 0, Patch: 10, Build: 10}
				Expect(v1.Compare(&v2)).To(Equal(1))
			})

			It("should prioritize minor version over patch/build", func() {
				v1 := internal.Version{Major: 8, Minor: 0, Patch: 10, Build: 10}
				v2 := internal.Version{Major: 8, Minor: 1, Patch: 0, Build: 0}
				Expect(v1.Compare(&v2)).To(Equal(-1))
			})
		})
	})

	Describe("IsEqual", func() {
		It("should return true for identical versions", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(v1.IsEqual(&v2)).To(BeTrue())
		})

		It("should return false for different versions", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 2, Build: 0}
			Expect(v1.IsEqual(&v2)).To(BeFalse())
		})
	})

	Describe("IsGreater", func() {
		It("should return true when version is greater", func() {
			v1 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsGreater(&v2)).To(BeTrue())
		})

		It("should return false when version is smaller", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsGreater(&v2)).To(BeFalse())
		})

		It("should return false when versions are equal", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(v1.IsGreater(&v2)).To(BeFalse())
		})
	})

	Describe("IsSmaller", func() {
		It("should return true when version is smaller", func() {
			v1 := internal.Version{Major: 7, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsSmaller(&v2)).To(BeTrue())
		})

		It("should return false when version is greater", func() {
			v1 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsSmaller(&v2)).To(BeFalse())
		})

		It("should return false when versions are equal", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(v1.IsSmaller(&v2)).To(BeFalse())
		})
	})

	Describe("IsGreaterOrEqual", func() {
		It("should return true when version is greater", func() {
			v1 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsGreaterOrEqual(&v2)).To(BeTrue())
		})

		It("should return true when versions are equal", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(v1.IsGreaterOrEqual(&v2)).To(BeTrue())
		})

		It("should return false when version is smaller", func() {
			v1 := internal.Version{Major: 7, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsGreaterOrEqual(&v2)).To(BeFalse())
		})
	})

	Describe("IsSmallerOrEqual", func() {
		It("should return true when version is smaller", func() {
			v1 := internal.Version{Major: 7, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsSmallerOrEqual(&v2)).To(BeTrue())
		})

		It("should return true when versions are equal", func() {
			v1 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 1, Build: 0}
			Expect(v1.IsSmallerOrEqual(&v2)).To(BeTrue())
		})

		It("should return false when version is greater", func() {
			v1 := internal.Version{Major: 9, Minor: 0, Patch: 0, Build: 0}
			v2 := internal.Version{Major: 8, Minor: 0, Patch: 0, Build: 0}
			Expect(v1.IsSmallerOrEqual(&v2)).To(BeFalse())
		})
	})

	Describe("Real-world scenarios", func() {
		Context("parsing and comparing version strings", func() {
			DescribeTable("should compare versions correctly",
				func(v1Str, v2Str, expectedResult string) {
					v1, err := internal.NewVersion(v1Str)
					Expect(err).ToNot(HaveOccurred())
					
					v2, err := internal.NewVersion(v2Str)
					Expect(err).ToNot(HaveOccurred())
					
					switch expectedResult {
					case "greater":
						Expect(v1.IsGreater(v2)).To(BeTrue())
					case "smaller":
						Expect(v1.IsSmaller(v2)).To(BeTrue())
					case "equal":
						Expect(v1.IsEqual(v2)).To(BeTrue())
					}
				},
				Entry("release vs patch", "8.1.0.0", "8.0.1.0", "greater"),
				Entry("major upgrade", "9.0.0.0", "8.9.9.9", "greater"),
				Entry("build difference", "8.0.1.1", "8.0.1.0", "greater"),
				Entry("same versions", "8.0.1.0", "8.0.1.0", "equal"),
				Entry("partial vs full", "8.1", "8.0.5.10", "greater"),
			)
		})
	})

	Describe("Edge cases", func() {
		It("should handle very large version numbers", func() {
			version, err := internal.NewVersion("999999.999999.999999.999999")
			Expect(err).ToNot(HaveOccurred())
			Expect(*version).To(Equal(internal.Version{Major: 999999, Minor: 999999, Patch: 999999, Build: 999999}))
		})

		It("should handle single digit zero version", func() {
			version, err := internal.NewVersion("0")
			Expect(err).ToNot(HaveOccurred())
			Expect(*version).To(Equal(internal.Version{Major: 0, Minor: 0, Patch: 0, Build: 0}))
		})
	})
})
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
	"math"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5040: map literals inside filter expressions must be packed in the
// server's canonical (key-ordered) msgpack form. These in-package tests pin
// the key ordering produced by compareCanonicalKeys and confirm the
// zero-allocation canonical marker actually drives the canonical pack path.
var _ = gg.Describe("Canonical map key ordering (CLIENT-5040)", func() {

	gg.DescribeTable("canonicalKeyOrder sorts keys in the server's msgpack order",
		func(in map[any]any, expected []any) {
			gm.Expect(canonicalKeyOrder(in)).To(gm.Equal(expected))
		},

		gg.Entry("string keys of mixed length sort lexicographically",
			map[any]any{"b": 1, "aa": 2, "ccc": 3, "a": 4, "bb": 5},
			[]any{"a", "aa", "b", "bb", "ccc"}),

		gg.Entry("negative and positive ints sort numerically",
			map[any]any{-7: 1, 42: 2, 1000: 3, 0: 4, -100: 5},
			[]any{-100, -7, 0, 42, 1000}),

		gg.Entry("ints always sort before strings (rank order)",
			map[any]any{"zzz": 1, 42: 2, "a": 3, -7: 4, 1000: 5},
			[]any{-7, 42, 1000, "a", "zzz"}),

		gg.Entry("unsigned values above MaxInt64 sort after every signed int",
			map[any]any{5: 1, uint64(math.MaxUint64): 2, 10: 3},
			[]any{5, 10, uint64(math.MaxUint64)}),

		gg.Entry("float keys sort numerically",
			map[any]any{3.5: 1, 1.2: 2, 2.0: 3},
			[]any{1.2, 2.0, 3.5}),
	)

	gg.It("extracts strings across raw string and StringValue keys of equal rank", func() {
		in := map[any]any{StringValue("m"): 1, "a": 2, "z": 3}
		gm.Expect(canonicalKeyOrder(in)).To(gm.Equal(
			[]any{"a", StringValue("m"), "z"}))
	})

	gg.It("orders GeoJSON keys after plain string keys", func() {
		in := map[any]any{GeoJSONValue("g2"): 1, "s": 2, GeoJSONValue("g1"): 3}
		gm.Expect(canonicalKeyOrder(in)).To(gm.Equal(
			[]any{"s", GeoJSONValue("g1"), GeoJSONValue("g2")}))
	})

	gg.Describe("canonical pack marker", func() {

		gg.It("is off by default and on once the buffer is marked", func() {
			buf := newBuffer(0)
			gm.Expect(isCanonicalPack(buf)).To(gm.BeFalse())
			buf.setCanonicalKeys(true)
			gm.Expect(isCanonicalPack(buf)).To(gm.BeTrue())
		})

		gg.It("is inherited by *baseCommand through its embedded buffer", func() {
			cmd := &baseCommand{}
			gm.Expect(isCanonicalPack(cmd)).To(gm.BeFalse())
			cmd.setCanonicalKeys(true)
			gm.Expect(isCanonicalPack(cmd)).To(gm.BeTrue())
		})

		gg.It("drives packIfcMap to emit keys in canonical order", func() {
			m := map[any]any{"b": 1, "aa": 2, "ccc": 3, "a": 4, "bb": 5}

			packCanonical := func(in map[any]any) []byte {
				sz, err := packIfcMap(nil, in)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				buf := newBuffer(sz)
				buf.setCanonicalKeys(true)
				_, err = packIfcMap(buf, in)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				return buf.Bytes()
			}

			// Manually pack the entries in canonical key order as the reference.
			expected := func(in map[any]any) []byte {
				keys := canonicalKeyOrder(in)
				sz := 0
				n, _ := packMapBegin(nil, len(in))
				sz += n
				for _, k := range keys {
					a, _ := packObject(nil, k, true)
					b, _ := packObject(nil, in[k], false)
					sz += a + b
				}
				buf := newBuffer(sz)
				packMapBegin(buf, len(in))
				for _, k := range keys {
					packObject(buf, k, true)
					packObject(buf, in[k], false)
				}
				return buf.Bytes()
			}

			// Deterministic across runs (Go map iteration is randomized) and
			// equal to the reference canonical ordering.
			first := packCanonical(m)
			for range 25 {
				gm.Expect(packCanonical(m)).To(gm.Equal(first))
			}
			gm.Expect(first).To(gm.Equal(expected(m)))
		})

		gg.It("clears the marker after an expression pack so sibling writes stay unordered", func() {
			// Base64 packs through fe.pack, which sets and (via defer) clears
			// the marker. The result must be byte-stable across many calls
			// regardless of map iteration order.
			exp := ExpListVal(NewMapValue(map[any]any{
				1402: 1802, 2003: 3946, 834: 1374, 3117: 1295,
			}))
			filter := ExpListAppendItems(DefaultListPolicy(),
				exp, ExpListBin("bin_li"))

			first, err := filter.Base64()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for range 25 {
				got, err := filter.Base64()
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(got).To(gm.Equal(first))
			}
		})
	})
})

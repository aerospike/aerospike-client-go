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

package aerospike_test

import (
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5045: servers with AER-6930 (8.1.2+) validate that list/map literals
// inside filter expressions are in canonical msgpack form, which packs
// non-negative integers unsigned. Positive integers > math.MaxUint32 must go
// on the wire as 0xcf (unsigned), not 0xd3 (signed), or the server rejects
// the expression with PARAMETER_ERROR.
var _ = gg.Describe("Expression big-integer literals (CLIENT-5045)", func() {

	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)

	// appendViaExp appends items as elements to a freshly seeded 2-element
	// list bin through a filter-expression literal and expects the resulting
	// list to have 2+len(items) elements.
	appendViaExp := func(key *as.Key, items ...as.Value) {
		client.Delete(wpolicy, key)
		err := client.PutBins(wpolicy, key, as.NewBin("bin_li", []any{0, 1}))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		appendItems := as.ExpListAppendItems(
			as.DefaultListPolicy(),
			as.ExpListVal(items...),
			as.ExpListBin("bin_li"),
		)
		rec, err := client.Operate(nil, key,
			as.ExpReadOp("res_bin", appendItems, as.ExpReadFlagDefault))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["res_bin"]).To(gm.HaveLen(2 + len(items)))
	}

	gg.It("should accept the smallest previously-rejected value, 2^32", func() {
		key, err := as.NewKey(ns, set, "client5045")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewLongValue(math.MaxUint32+1))
	})

	gg.It("should accept values across every integer encoding boundary", func() {
		key, err := as.NewKey(ns, set, "client5045-bounds")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key,
			as.NewLongValue(0),
			as.NewLongValue(127),
			as.NewLongValue(128),
			as.NewLongValue(math.MaxUint8),
			as.NewLongValue(math.MaxUint16),
			as.NewLongValue(math.MaxUint16+1),
			as.NewLongValue(math.MaxUint32),   // last 0xce value
			as.NewLongValue(math.MaxUint32+1), // first 0xcf value
			as.NewLongValue(math.MaxInt64),
		)
	})

	gg.It("should still accept large negative values (canonically signed)", func() {
		key, err := as.NewKey(ns, set, "client5045-neg")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key,
			as.NewLongValue(-1),
			as.NewLongValue(math.MinInt32),
			as.NewLongValue(math.MinInt32-1),
			as.NewLongValue(-(math.MaxUint32 + 1)),
			as.NewLongValue(math.MinInt64),
		)
	})

	gg.It("should accept big integers nested inside a map literal", func() {
		key, err := as.NewKey(ns, set, "client5045-map")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// big ints as unsorted map keys and values exercises CLIENT-5045
		// together with the CLIENT-5040 canonical key ordering
		appendViaExp(key, as.NewMapValue(map[any]any{
			int64(math.MaxUint32 + 1): int64(math.MaxInt64),
			42:                        int64(math.MaxUint32 + 2),
			int64(math.MaxInt64):      1,
		}))
	})

	gg.It("should accept big integers nested inside a list-in-list literal", func() {
		key, err := as.NewKey(ns, set, "client5045-nested")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewListValue([]any{
			int64(math.MaxUint32 + 1),
			[]any{int64(math.MaxInt64), int64(math.MinInt64)},
		}))
	})

	gg.It("should round-trip big integers unchanged through regular CDT writes", func() {
		key, err := as.NewKey(ns, set, "client5045-rt")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client.Delete(wpolicy, key)

		list := []any{int64(math.MaxUint32 + 1), int64(math.MaxInt64), int64(math.MinInt64)}
		err = client.PutBins(wpolicy, key, as.NewBin("bin_li", list))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		r, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins["bin_li"]).To(gm.Equal([]any{
			int(math.MaxUint32 + 1), int(math.MaxInt64), int(math.MinInt64),
		}))

		// the same big ints appended via the plain CDT operation path must
		// keep working
		_, err = client.Operate(nil, key, as.ListAppendOp("bin_li", int64(math.MaxUint32+3)))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})
})

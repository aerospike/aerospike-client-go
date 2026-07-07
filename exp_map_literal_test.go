// Copyright 2017-2022 Aerospike, Inc.
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
	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5040: servers with AER-6930 (8.1.2+) validate that map literals
// inside filter expressions are in canonical key order. Unordered maps must
// be packed key-ordered on the wire.
var _ = gg.Describe("Expression map literals (CLIENT-5040)", func() {

	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)

	unsortedMap := map[any]any{1402: 1802, 2003: 3946, 834: 1374, 3117: 1295}

	// appendViaExp appends mapVal as a single element to a freshly seeded
	// 2-element list bin through a filter-expression literal and expects the
	// resulting list to have 3 elements.
	appendViaExp := func(key *as.Key, mapVal as.Value) {
		client.Delete(wpolicy, key)
		err := client.PutBins(wpolicy, key, as.NewBin("bin_li", []any{0, 1}))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		appendItems := as.ExpListAppendItems(
			as.DefaultListPolicy(),
			as.ExpListVal(mapVal),
			as.ExpListBin("bin_li"),
		)
		rec, err := client.Operate(nil, key,
			as.ExpReadOp("res_bin", appendItems, as.ExpReadFlagDefault))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["res_bin"]).To(gm.HaveLen(3))
	}

	gg.It("ExpListAppendItems with an unsorted multi-key map literal should work", func() {
		key, err := as.NewKey(ns, set, "client5040")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewMapValue(unsortedMap))

		// same unsorted map appended via the plain CDT operation path must
		// keep working
		_, err = client.Operate(nil, key, as.ListAppendOp("bin_li", unsortedMap))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// regular Put/Get round-trip of the unordered map must be unchanged
		err = client.PutBins(wpolicy, key, as.NewBin("bin_map", unsortedMap))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		r, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(r.Bins["bin_map"]).To(gm.Equal(map[any]any{1402: 1802, 2003: 3946, 834: 1374, 3117: 1295}))
	})

	gg.It("should canonically order string map keys of mixed lengths", func() {
		key, err := as.NewKey(ns, set, "client5040-str")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewMapValue(map[any]any{
			"b": 1, "aa": 2, "ccc": 3, "a": 4, "bb": 5,
		}))
	})

	gg.It("should canonically order mixed-type map keys", func() {
		key, err := as.NewKey(ns, set, "client5040-mixed")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewMapValue(map[any]any{
			"zzz": 1, 42: 2, "a": 3, -7: 4, 1000: 5,
		}))
	})

	gg.It("should canonically order JSON map keys", func() {
		key, err := as.NewKey(ns, set, "client5040-json")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		appendViaExp(key, as.NewJsonValue(map[string]any{
			"b": 1, "aa": 2, "ccc": 3, "a": 4, "bb": 5,
		}))
	})
})

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

package sdk_test

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("SDK CDT range selections", func() {
	// seedMap writes a five-entry ordered map to a fresh key.
	seedMap := func() (*sdk.Session, *as.Key) {
		s, ds := newSession()
		key := ds.Key("m")
		gm.Expect(s.Put(key, as.BinMap{"m": map[any]any{
			"a": 1, "b": 2, "c": 3, "d": 4, "e": 5,
		}})).ToNot(gm.HaveOccurred())
		return s, key
	}

	gg.It("must read a map key range", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapKeyRange("b", "d").GetKeys().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// Half-open: b and c, not d.
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf("b", "c"))
	})

	gg.It("must count a map value range", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapValueRange(2, 4).Count().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.BeEquivalentTo(2))
	})

	gg.It("must read an explicit map key list", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapKeyList("a", "e").GetValues().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf(1, 5))
	})

	gg.It("must read a map index range", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapIndexRange(0, 2).Count().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.BeEquivalentTo(2))
	})

	gg.It("must remove a map key range and report the count", func() {
		s, key := seedMap()
		_, err := s.Update(key).Bin("m").OnMapKeyRange("b", "d").RemoveAnd().Count().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		remaining := asMap(rec.Bins["m"])
		gm.Expect(remaining).To(gm.HaveLen(3))
		gm.Expect(remaining).ToNot(gm.HaveKey("b"))
		gm.Expect(remaining).To(gm.HaveKey("a"))
	})

	gg.It("must read a list value range and a list value list", func() {
		s, ds := newSession()
		key := ds.Key("l")
		gm.Expect(s.Put(key, as.BinMap{"l": []any{10, 20, 30, 40, 50}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("l").OnListValueRange(20, 40).GetValues().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["l"]).To(gm.ConsistOf(20, 30))

		stream, err = s.Query(key).Bin("l").OnListValueList(10, 50).Count().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err = stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["l"]).To(gm.BeEquivalentTo(2))
	})
})

var _ = gg.Describe("SDK CDT inverted terminals", func() {
	seedMap := func() (*sdk.Session, *as.Key) {
		s, ds := newSession()
		key := ds.Key("m")
		gm.Expect(s.Put(key, as.BinMap{"m": map[any]any{
			"a": 1, "b": 2, "c": 3, "d": 4, "e": 5,
		}})).ToNot(gm.HaveOccurred())
		return s, key
	}

	gg.It("must read the entries a range did not match", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapKeyRange("b", "d").GetAllOtherKeys().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// The inverse of {b, c} over {a..e}.
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf("a", "d", "e"))
	})

	gg.It("must count the entries a range did not match", func() {
		s, key := seedMap()
		stream, err := s.Query(key).Bin("m").OnMapKeyRange("b", "d").CountAllOthers().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.BeEquivalentTo(3))
	})

	gg.It("must remove everything a range did not match", func() {
		s, key := seedMap()
		_, err := s.Update(key).Bin("m").OnMapKeyRange("b", "d").RemoveAllOthers().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		remaining := asMap(rec.Bins["m"])
		// Only the matched entries survive.
		gm.Expect(remaining).To(gm.HaveLen(2))
		gm.Expect(remaining).To(gm.HaveKey("b"))
		gm.Expect(remaining).To(gm.HaveKey("c"))
	})

	gg.It("must report what an inverted removal removed", func() {
		s, key := seedMap()
		stream, err := s.Update(key).
			Bin("m").OnMapKeyRange("b", "d").RemoveAllOthersAnd().Count().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.BeEquivalentTo(3))
	})

	gg.It("must read the list elements a value list did not match", func() {
		s, ds := newSession()
		key := ds.Key("l")
		gm.Expect(s.Put(key, as.BinMap{"l": []any{10, 20, 30}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("l").OnListValueList(20).GetAllOtherValues().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["l"]).To(gm.ConsistOf(10, 30))
	})

	gg.It("must reject a key-oriented terminal on a list selection", func() {
		s, ds := newSession()
		key := ds.Key("l")
		gm.Expect(s.Put(key, as.BinMap{"l": []any{1, 2}})).ToNot(gm.HaveOccurred())

		_, err := s.Query(key).Bin("l").OnListValueRange(1, 2).GetAllOtherKeys().Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK HyperLogLog", func() {
	gg.It("must add, count and describe", func() {
		s, ds := newSession()
		key := ds.Key("hll")
		cfg := sdk.HLLConfigOf(12)

		_, err := s.Upsert(key).
			Bin("h").HLLInit(as.DefaultHLLPolicy(), cfg).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = s.Upsert(key).
			Bin("h").HLLAdd(as.DefaultHLLPolicy(),
			as.NewStringValue("u1"), as.NewStringValue("u2"), as.NewStringValue("u3")).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("h").HLLGetCount().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["h"]).To(gm.BeEquivalentTo(3))

		stream, err = s.Query(key).Bin("h").HLLDescribe().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err = stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		described, err := row.GetHLLConfig("h")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(described).ToNot(gm.BeNil())
		gm.Expect(described.IndexBitCount).To(gm.BeEquivalentTo(12))
	})

	gg.It("must union two HyperLogLog bins and report the counts", func() {
		s, ds := newSession()
		key := ds.Key("hllunion")
		cfg := sdk.HLLConfigOf(12)
		p := as.DefaultHLLPolicy()

		// Two bins with an overlapping element.
		_, err := s.Upsert(key).
			Bin("a").HLLInit(p, cfg).
			Bin("b").HLLInit(p, cfg).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = s.Upsert(key).
			Bin("a").HLLAdd(p, as.NewStringValue("x"), as.NewStringValue("y")).
			Bin("b").HLLAdd(p, as.NewStringValue("y"), as.NewStringValue("z")).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		other, ok := rec.Bins["b"].(as.HLLValue)
		gm.Expect(ok).To(gm.BeTrue(), "bin b should hold an HLL value")

		// The union of {x,y} and {y,z} has three distinct elements.
		stream, err := s.Query(key).Bin("a").HLLGetUnionCount(other).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["a"]).To(gm.BeEquivalentTo(3))

		// Their intersection has one.
		stream, err = s.Query(key).Bin("a").HLLGetIntersectCount(other).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err = stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["a"]).To(gm.BeEquivalentTo(1))

		// Similarity is a ratio in [0, 1].
		stream, err = s.Query(key).Bin("a").HLLGetSimilarity(other).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err = stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["a"]).To(gm.BeNumerically(">=", 0.0))
		gm.Expect(row.Record.Bins["a"]).To(gm.BeNumerically("<=", 1.0))
	})

	gg.It("must fold to a smaller index bit count", func() {
		s, ds := newSession()
		key := ds.Key("hllfold")
		p := as.DefaultHLLPolicy()

		_, err := s.Upsert(key).Bin("h").HLLInit(p, sdk.HLLConfigOf(12)).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = s.Upsert(key).Bin("h").HLLAdd(p, as.NewStringValue("u1")).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = s.Upsert(key).Bin("h").HLLFold(8).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("h").HLLDescribe().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		cfg, err := row.GetHLLConfig("h")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cfg.IndexBitCount).To(gm.BeEquivalentTo(8))
	})
})

var _ = gg.Describe("SDK CDT relative range selections", func() {
	// seedOrderedMap writes a key-ordered map so relative offsets are
	// predictable: a=1 .. e=5.
	seedOrderedMap := func() (*sdk.Session, *as.Key) {
		s, ds := newSession()
		key := ds.Key("rel")
		policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
		_, err := s.Upsert(key).Bin("m").MapPutItems(policy,
			map[any]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
		).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return s, key
	}

	gg.It("must select map entries at an index offset from an anchor key", func() {
		s, key := seedOrderedMap()

		// Two entries starting one past where "b" sorts: c and d.
		stream, err := s.Query(key).
			Bin("m").OnMapKeyRelativeIndexRange("b", 1, 2).GetKeys().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf("c", "d"))
	})

	gg.It("must run to the end of the map with a negative count", func() {
		s, key := seedOrderedMap()
		stream, err := s.Query(key).
			Bin("m").OnMapKeyRelativeIndexRange("c", 1, -1).GetKeys().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf("d", "e"))
	})

	gg.It("must anchor on a key that is not present", func() {
		s, key := seedOrderedMap()
		// "bb" would sort between b and c, so offset 0 starts at c.
		stream, err := s.Query(key).
			Bin("m").OnMapKeyRelativeIndexRange("bb", 0, 2).GetKeys().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf("c", "d"))
	})

	gg.It("must select map entries at a rank offset from an anchor value", func() {
		s, key := seedOrderedMap()
		// Two entries starting one rank above value 2: 3 and 4.
		stream, err := s.Query(key).
			Bin("m").OnMapValueRelativeRankRange(2, 1, 2).GetValues().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf(3, 4))
	})

	gg.It("must select list elements at a rank offset from an anchor value", func() {
		s, ds := newSession()
		key := ds.Key("rell")
		gm.Expect(s.Put(key, as.BinMap{"l": []any{10, 20, 30, 40, 50}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).
			Bin("l").OnListValueRelativeRankRange(20, 1, 2).GetValues().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["l"]).To(gm.ConsistOf(30, 40))
	})

	gg.It("must remove a relative range and leave the rest", func() {
		s, key := seedOrderedMap()
		_, err := s.Update(key).
			Bin("m").OnMapKeyRelativeIndexRange("b", 1, 2).RemoveAnd().Count().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		remaining := asMap(rec.Bins["m"])
		gm.Expect(remaining).To(gm.HaveLen(3))
		gm.Expect(remaining).ToNot(gm.HaveKey("c"))
		gm.Expect(remaining).ToNot(gm.HaveKey("d"))
	})

	gg.It("must remove a relative list range, where the core takes its arguments in a different order", func() {
		s, ds := newSession()
		key := ds.Key("rellrm")
		gm.Expect(s.Put(key, as.BinMap{"l": []any{10, 20, 30, 40}})).ToNot(gm.HaveOccurred())

		_, err := s.Update(key).
			Bin("l").OnListValueRelativeRankRange(20, 1, 2).Remove().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["l"]).To(gm.ConsistOf(10, 20))
	})

	gg.It("must invert a relative selection", func() {
		s, key := seedOrderedMap()
		stream, err := s.Query(key).
			Bin("m").OnMapKeyRelativeIndexRange("b", 1, 2).CountAllOthers().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// Five entries, two matched, so three did not.
		gm.Expect(row.Record.Bins["m"]).To(gm.BeEquivalentTo(3))
	})

	gg.It("must report a range selection as a map", func() {
		s, key := seedOrderedMap()
		stream, err := s.Query(key).
			Bin("m").OnMapKeyRange("b", "d").GetAsOrderedMap().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		got := asMap(row.Record.Bins["m"])
		gm.Expect(got).To(gm.HaveLen(2))
		gm.Expect(got).To(gm.HaveKey("b"))
	})
})

var _ = gg.Describe("SDK CDT path entry from a fixed step", func() {
	gg.BeforeEach(func() {
		if !testCluster.SupportsCDTPathExpressions() {
			gg.Skip("cluster does not support CDT path expressions (requires 8.1.1+)")
		}
	})

	// A path chain need not start at the bin root: descend to a known place
	// with the ordinary selectors, then switch into path mode.
	gg.It("must iterate every child beneath a fixed map key", func() {
		s, ds := newSession()
		key := ds.Key("catalog")
		gm.Expect(s.Put(key, as.BinMap{"catalog": map[any]any{
			"book": map[any]any{
				"a": map[any]any{"title": "Moby Dick"},
				"b": map[any]any{"title": "Sword of Honour"},
			},
			"bicycle": map[any]any{"color": "red"},
		}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).
			Bin("catalog").OnMapKey("book", nil).
			OnEachChild().OnMapKey("title").NoFail().CollectValues().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// Only the titles under "book" — the bicycle has none.
		gm.Expect(row.Record.Bins["catalog"]).To(gm.ConsistOf("Moby Dick", "Sword of Honour"))
	})

	gg.It("must remove every match with RemoveMatches", func() {
		s, ds := newSession()
		key := ds.Key("prices")
		gm.Expect(s.Put(key, as.BinMap{"items": map[any]any{
			"a": map[any]any{"price": 5, "name": "cheap"},
			"b": map[any]any{"price": 50, "name": "dear"},
		}})).ToNot(gm.HaveOccurred())

		_, err := s.Update(key).
			Bin("items").OnEachChild().OnMapKey("price").NoFail().RemoveMatches().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		items := asMap(rec.Bins["items"])
		// The price leaves are gone; the names remain.
		for _, entry := range items {
			gm.Expect(asMap(entry)).ToNot(gm.HaveKey("price"))
			gm.Expect(asMap(entry)).To(gm.HaveKey("name"))
		}
	})

	gg.It("must collect a selection as an expression read", func() {
		s, ds := newSession()
		key := ds.Key("exread")
		gm.Expect(s.Put(key, as.BinMap{"m": map[any]any{
			"a": map[any]any{"v": 1},
			"b": map[any]any{"v": 2},
		}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).
			Bin("m").OnEachChild().OnMapKey("v").NoFail().
			CollectValuesAsExpressionRead(as.ExpTypeMAP, as.ExpTypeLIST, true).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["m"]).To(gm.ConsistOf(1, 2))
	})
})

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
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Customer is mapped by reflection; the key field never becomes a bin.
type Customer struct {
	ID   int64  `as:",key"`
	Name string `as:"name"`
	Age  int64  `as:"age"`
	Note string `as:"-"`
}

var _ = gg.Describe("SDK typed layer", func() {
	newTyped := func() (*sdk.Session, *sdk.TypedDataSet[Customer]) {
		s, err := testCluster.CreateSession(nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		ds, err := sdk.TypedDataSetOf[Customer](*namespace, randomSet())
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return s, ds
	}

	gg.It("must round-trip objects", func() {
		s, customers := newTyped()
		alice := &Customer{ID: 1, Name: "Ada", Age: 36, Note: "not stored"}
		bob := &Customer{ID: 2, Name: "Bob", Age: 41}

		stream, err := s.UpsertTyped(customers).Object(alice).Object(bob).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))

		typed, err := s.QueryTyped(customers).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		objs, err := typed.IntoObjects()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(objs).To(gm.HaveLen(2))

		byID := map[int64]*Customer{}
		for _, o := range objs {
			byID[o.ID] = o
		}
		gm.Expect(byID).To(gm.HaveKey(int64(1)))
		gm.Expect(byID[1].Name).To(gm.Equal("Ada"))
		// The key is recovered from the key, and `-` keeps a field out entirely.
		gm.Expect(byID[1].ID).To(gm.BeEquivalentTo(1))
		gm.Expect(byID[1].Note).To(gm.BeEmpty())
	})

	gg.It("must keep the key field out of the bins", func() {
		s, customers := newTyped()
		_, err := s.UpsertTyped(customers).Object(&Customer{ID: 9, Name: "Zoe"}).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		key := customers.Key(int64(9))
		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).ToNot(gm.HaveKey("ID"))
		gm.Expect(rec.Bins["name"]).To(gm.Equal("Zoe"))
	})

	gg.It("must defer a misplaced guard to the terminal", func() {
		s, customers := newTyped()
		_, err := s.UpsertTyped(customers).EnsureGenerationIs(1).Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK CDT navigation", func() {
	gg.It("must write and read a nested map value", func() {
		s, ds := newSession()
		key := ds.Key("prefs")
		keyOrdered := as.MapOrder.KEY_ORDERED

		_, err := s.Upsert(key).
			Bin("prefs").OnMapKey("theme", &keyOrdered).SetTo("dark").
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("prefs").OnMapKey("theme", nil).GetValues().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["prefs"]).To(gm.Equal("dark"))
	})

	gg.It("must honor the per-key write modes", func() {
		s, ds := newSession()
		key := ds.Key("modes")
		unordered := as.MapOrder.UNORDERED

		_, err := s.Upsert(key).Bin("m").OnMapKey("a", &unordered).Insert(1, false).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Create-only fails on an existing key unless noFail is set.
		_, err = s.Upsert(key).Bin("m").OnMapKey("a", &unordered).Insert(2, false).Execute()
		gm.Expect(err).To(gm.HaveOccurred())
		_, err = s.Upsert(key).Bin("m").OnMapKey("a", &unordered).Insert(2, true).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Update-only needs the key to exist.
		_, err = s.Upsert(key).Bin("m").OnMapKey("a", &unordered).Update(3, false).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = s.Upsert(key).Bin("m").OnMapKey("absent", &unordered).Update(1, false).Execute()
		gm.Expect(err).To(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(asMap(rec.Bins["m"])["a"]).To(gm.BeEquivalentTo(3))
	})

	gg.It("must navigate two levels deep, creating intermediates", func() {
		s, ds := newSession()
		key := ds.Key("deep")
		keyOrdered := as.MapOrder.KEY_ORDERED

		_, err := s.Upsert(key).
			Bin("doc").OnMapKey("mid", &keyOrdered).OnMapKey("leaf", &keyOrdered).SetTo(99).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(asMap(asMap(rec.Bins["doc"])["mid"])["leaf"]).To(gm.BeEquivalentTo(99))
	})

	gg.It("must reject a per-key write off a non-key selection", func() {
		s, ds := newSession()
		key := ds.Key("reject")
		_, err := s.Upsert(key).Bin("m").OnMapIndex(0).SetTo(1).Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must remove a list range and report the count", func() {
		s, ds := newSession()
		key := ds.Key("listrange")
		gm.Expect(s.Put(key, as.BinMap{"scores": []any{1, 2, 3, 4, 5}})).ToNot(gm.HaveOccurred())

		_, err := s.Update(key).Bin("scores").OnListIndexRange(0, 2).RemoveAnd().Count().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["scores"]).To(gm.HaveLen(3))
	})
})

var _ = gg.Describe("SDK CDT path expressions", func() {
	gg.BeforeEach(func() {
		if !testCluster.SupportsCDTPathExpressions() {
			gg.Skip("cluster does not support CDT path expressions (requires 8.1.1+)")
		}
	})

	gg.It("must collect values under every child", func() {
		s, ds := newSession()
		key := ds.Key("catalog")
		gm.Expect(s.Put(key, as.BinMap{"catalog": map[any]any{
			"a": map[any]any{"price": 5},
			"b": map[any]any{"price": 20},
		}})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).
			Bin("catalog").OnEachChild().OnMapKey("price").NoFail().CollectValues().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.GinkgoWriter.Printf("collected prices: %v\n", row.Record.Bins["catalog"])
	})
})

var _ = gg.Describe("SDK AEL filters", func() {
	gg.BeforeEach(func() {
		if !testCluster.SupportsServerCompiledAEL() {
			gg.Skip("cluster does not compile AEL (requires 8.1.3+)")
		}
	})

	gg.It("must filter with source text and with a typed expression alike", func() {
		s, ds := newSession()
		for i := range 10 {
			k := ds.Key(int64(i))
			gm.Expect(s.Put(k, as.BinMap{"age": i * 10})).ToNot(gm.HaveOccurred())
		}

		stream, err := s.Query(ds).Where("$.age >= 50").Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		aelRows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(aelRows).To(gm.HaveLen(5))

		stream, err = s.Query(ds).
			Where(as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(50))).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		typedRows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(typedRows).To(gm.HaveLen(len(aelRows)))
	})

	gg.It("must reject empty source text", func() {
		s, ds := newSession()
		_, err := s.Query(ds).Where("").Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK row writes", func() {
	gg.It("must write many records of one shape", func() {
		s, ds := newSession()
		stream, err := s.UpsertRows(ds).
			Bins("name", "age").
			Row(int64(1), "Alice", 34).
			Row(int64(2), "Bob", 41).
			DefaultExpireRecordAfterSeconds(3600).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))

		key := ds.Key(int64(1))
		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["name"]).To(gm.Equal("Alice"))
	})

	gg.It("must report a value-count mismatch from the terminal", func() {
		s, ds := newSession()
		_, err := s.UpsertRows(ds).Bins("a", "b").Row(int64(1), "only-one").Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must require Bins before Row", func() {
		s, ds := newSession()
		_, err := s.UpsertRows(ds).Row(int64(1), "x").Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK navigatable streams", func() {
	gg.It("must sort and page in memory", func() {
		s, ds := newSession()
		ages := []int{30, 10, 50, 20, 40}
		for i, a := range ages {
			k := ds.Key(int64(i))
			gm.Expect(s.Put(k, as.BinMap{"age": a})).ToNot(gm.HaveOccurred())
		}

		stream, err := s.Query(ds).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		nav, err := stream.IntoNavigatable()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(nav.Size()).To(gm.Equal(5))

		nav.SortBy(sdk.Asc("age"))
		var sorted []int
		for nav.HasNext() {
			row := nav.Next()
			sorted = append(sorted, int(row.Record.Bins["age"].(int)))
		}
		gm.Expect(sorted).To(gm.Equal([]int{10, 20, 30, 40, 50}))

		// Re-sorting descending needs no second query.
		nav.SortBy(sdk.Desc("age"))
		first := nav.Next()
		gm.Expect(first.Record.Bins["age"]).To(gm.BeEquivalentTo(50))

		// Pagination walks the whole set two at a time.
		nav.SortBy(sdk.Asc("age")).PageSize(2)
		gm.Expect(nav.MaxPages()).To(gm.Equal(3))
		total := 0
		for nav.HasMorePages() {
			for nav.HasNext() {
				nav.Next()
				total++
			}
		}
		gm.Expect(total).To(gm.Equal(5))
	})

	gg.It("must order values across types the way the server does", func() {
		// NIL < BOOLEAN < INTEGER < STRING, and no numeric promotion.
		gm.Expect(sdk.CompareValues(nil, true, false)).To(gm.BeNumerically("<", 0))
		gm.Expect(sdk.CompareValues(true, 1, false)).To(gm.BeNumerically("<", 0))
		gm.Expect(sdk.CompareValues(1, "a", false)).To(gm.BeNumerically("<", 0))
		gm.Expect(sdk.CompareValues(99, 1.0, false)).To(gm.BeNumerically("<", 0))
		gm.Expect(sdk.CompareValues(2, 10, false)).To(gm.BeNumerically("<", 0))
		// Case folding applies when asked.
		gm.Expect(sdk.CompareValues("ABC", "abc", true)).To(gm.Equal(0))
		gm.Expect(sdk.CompareValues("ABC", "abc", false)).ToNot(gm.Equal(0))
		// A shorter list sorts before its extension.
		gm.Expect(sdk.CompareValues([]any{1}, []any{1, 2}, false)).To(gm.BeNumerically("<", 0))
	})
})

var _ = gg.Describe("SDK transactions", func() {
	gg.BeforeEach(requireSC)

	gg.It("must commit a multi-record transaction", func() {
		s, ds := scSession()
		a := ds.Key("acct-a")
		b := ds.Key("acct-b")

		tx, err := s.Transaction()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = tx.Upsert(a).SetTo("balance", 100).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = tx.Upsert(b).SetTo("balance", 200).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = tx.Commit()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Finalizing twice is an error.
		_, err = tx.Commit()
		gm.Expect(err).To(gm.HaveOccurred())

		rec, err := s.Get(a, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["balance"]).To(gm.BeEquivalentTo(100))
	})

	gg.It("must run a function inside a transaction", func() {
		s, ds := scSession()
		k := ds.Key("x")
		err := s.DoInTransaction(func(tx *sdk.Session) error {
			_, e := tx.Upsert(k).SetTo("v", 42).Execute()
			return e
		}, 5, 0)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(k, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["v"]).To(gm.BeEquivalentTo(42))
	})

	gg.It("must resolve the phase policies from the bound behavior", func() {
		b := sdk.DefaultBehavior().DeriveWithChanges("txnpol_"+randomSet(),
			map[sdk.Scope]sdk.Settings{
				sdk.ScopeSystemTxnVerify: {MaxRetries: sdk.IntPtr(7)},
				sdk.ScopeSystemTxnRoll:   {MaxRetries: sdk.IntPtr(9)},
			})
		verify := b.SystemSettingsFor(sdk.ScopeSystemTxnVerify)
		gm.Expect(*verify.MaxRetries).To(gm.Equal(7))

		s, err := testCluster.CreateSession(b)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		ds, _ := sdk.DataSetOf(*scNamespace, randomSet())
		k := ds.Key("policy-driven")

		tx, err := s.Transaction()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = tx.Upsert(k).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = tx.Commit()
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.It("must resolve the implicit transaction's phase policies from the behavior", func() {
		// A behavior whose transaction-phase settings would fail to map if they
		// were ignored: an out-of-range reset percentage is rejected by the
		// policy mapper, so the batch can only succeed if the implicit path
		// really does resolve and validate these scopes.
		bad := sdk.DefaultBehavior().DeriveWithChanges("implicit_bad_"+randomSet(),
			map[sdk.Scope]sdk.Settings{
				sdk.ScopeSystemTxnVerify: {ReadTouchTTLPercent: sdk.Int32Ptr(150)},
			})
		s, err := testCluster.CreateSession(bad)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		ds, _ := sdk.DataSetOf(*scNamespace, randomSet())
		keys := ds.Keys([]int64{1, 2})

		_, err = s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).To(gm.HaveOccurred(),
			"an unmappable transaction-phase policy must surface, proving the scopes are consulted")

		// A valid behavior over the same scopes commits normally.
		good := sdk.DefaultBehavior().DeriveWithChanges("implicit_good_"+randomSet(),
			map[sdk.Scope]sdk.Settings{
				sdk.ScopeSystemTxnVerify: {MaxRetries: sdk.IntPtr(7)},
				sdk.ScopeSystemTxnRoll:   {MaxRetries: sdk.IntPtr(9)},
			})
		s2, err := testCluster.CreateSession(good)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		ds2, _ := sdk.DataSetOf(*scNamespace, randomSet())
		keys2 := ds2.Keys([]int64{1, 2})

		stream, err := s2.Upsert(keys2).SetTo("v", 2).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))
	})

	gg.It("must wrap a multi-key write batch in an implicit transaction", func() {
		s, ds := scSession()
		keys := ds.Keys([]int64{1, 2, 3})

		// No API change: the batch gains atomicity because the namespace is
		// strong-consistency and the setting is on by default.
		stream, err := s.Upsert(keys).SetTo("v", 7).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(3))

		rec, err := s.Get(keys[0], sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["v"]).To(gm.BeEquivalentTo(7))
	})
})

var _ = gg.Describe("SDK info", func() {
	gg.It("must report namespaces and a namespace detail", func() {
		s, _ := newSession()
		info := s.InfoCommands()

		names, err := info.Namespaces()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(names).To(gm.ContainElement(*namespace))

		detail, err := info.NamespaceDetail(*namespace)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(detail).ToNot(gm.BeNil())
		_, ok := detail.Objects()
		gm.Expect(ok).To(gm.BeTrue())
		// Anything without a named getter stays reachable.
		gm.Expect(detail.Stats().Len()).To(gm.BeNumerically(">", 0))
	})

	gg.It("must report absence for an unknown namespace", func() {
		s, _ := newSession()
		detail, err := s.InfoCommands().NamespaceDetail("no_such_namespace")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(detail).To(gm.BeNil())
	})

	gg.It("must report set details for a populated set", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{1, 2})
		_, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		details, err := s.InfoCommands().SetDetails(*namespace)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(details).ToNot(gm.BeEmpty())
	})

	gg.It("must parse and merge info statistics", func() {
		a := sdk.ParseInfoStats("objects=10;stop_writes=false;rate=1.5", ";")
		b := sdk.ParseInfoStats("objects=5;stop_writes=true;rate=2.5", ";")

		// Dash and underscore are interchangeable on lookup.
		v, ok := a.Get("stop-writes")
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(v).To(gm.Equal("false"))

		// Integers sum, floats average, booleans require agreement.
		merged, err := sdk.MergeInfoStats([]sdk.InfoStats{a, b}, nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		objects, _ := merged.GetInt("objects")
		gm.Expect(objects).To(gm.BeEquivalentTo(15))
		rate, _ := merged.GetFloat("rate")
		gm.Expect(rate).To(gm.BeNumerically("~", 2.0, 0.001))
		stop, _ := merged.GetBool("stop_writes")
		gm.Expect(stop).To(gm.BeFalse())

		// An override changes the strategy.
		merged, err = sdk.MergeInfoStats([]sdk.InfoStats{a, b},
			map[string]sdk.MergeStrategy{"stop-writes": sdk.MergeOr})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		stop, _ = merged.GetBool("stop_writes")
		gm.Expect(stop).To(gm.BeTrue())
	})
})

var _ = gg.Describe("SDK indexes", func() {
	gg.It("must create, list and drop an index", func() {
		s, ds := newSession()
		name := "idx_" + randomSet()

		task, err := s.Index(ds).OnBin("age").Named(name).Numeric().Create()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(task).ToNot(gm.BeNil())

		indexes, err := s.ListIndexes()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		found := false
		for _, i := range indexes {
			if i.Name == name {
				found = true
			}
		}
		gm.Expect(found).To(gm.BeTrue(), "the created index should appear in the listing")

		gm.Expect(s.Index(ds).Named(name).Drop()).ToNot(gm.HaveOccurred())
	})

	gg.It("must require a name and a value type", func() {
		s, ds := newSession()
		_, err := s.Index(ds).OnBin("age").Numeric().Create()
		gm.Expect(err).To(gm.HaveOccurred())
		_, err = s.Index(ds).OnBin("age").Named("x").Create()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK string operations", func() {
	gg.BeforeEach(func() {
		if !testCluster.SupportsStringOperations() {
			gg.Skip("cluster does not support the string operations (requires 8.1.3+)")
		}
	})

	gg.It("must read string properties server-side", func() {
		s, ds := newSession()
		key := ds.Key("str")
		gm.Expect(s.Put(key, as.BinMap{"s": "Hello World"})).ToNot(gm.HaveOccurred())

		stream, err := s.Query(key).Bin("s").StrLen().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(row.Record.Bins["s"]).To(gm.BeEquivalentTo(11))

		stream, err = s.Query(key).Bin("s").StrContains("World").Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err = stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.GinkgoWriter.Printf("contains = %v\n", row.Record.Bins["s"])
	})
})

var _ = gg.Describe("SDK background tasks", func() {
	gg.It("must reject a background task that targets keys", func() {
		s, ds := newSession()
		key := ds.Key("k")
		_, err := s.Query(key).
			WithWriteOperations(as.PutOp(as.NewBin("tier", "gold"))).
			ExecuteBackgroundTask()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must reject a background task with no write operations", func() {
		s, ds := newSession()
		_, err := s.Query(ds).ExecuteBackgroundTask()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must apply operations to every matching record", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{1, 2, 3})
		_, err := s.Upsert(keys).SetTo("score", 10).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		task, err := s.Query(ds).
			WithWriteOperations(as.PutOp(as.NewBin("tier", "gold"))).
			ExecuteBackgroundTask()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(task).ToNot(gm.BeNil())
		gm.Expect(<-task.OnComplete()).ToNot(gm.HaveOccurred())

		rec, err := s.Get(keys[0], sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["tier"]).To(gm.Equal("gold"))
	})

	gg.It("must delete every matching record in the background", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{1, 2})
		_, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		task, err := s.Query(ds).ExecuteBackgroundDelete()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-task.OnComplete()).ToNot(gm.HaveOccurred())

		_, err = s.Get(keys[0], sdk.AllBins)
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

// asMap normalizes a map bin: an unordered map arrives as map[any]any, while an
// ordered one arrives as []as.MapPair.
func asMap(v any) map[any]any {
	switch m := v.(type) {
	case map[any]any:
		return m
	case []as.MapPair:
		out := make(map[any]any, len(m))
		for _, p := range m {
			out[p.Key] = p.Value
		}
		return out
	}
	gm.Expect(v).To(gm.BeAssignableToTypeOf(map[any]any{}), "expected a map bin")
	return nil
}

var _ = gg.Describe("SDK typed mapping details", func() {
	// Optional models an absent value with a pointer, the shape Rust expresses
	// as Option<T>.
	type Optional struct {
		ID    int64    `as:",key"`
		Name  string   `as:"name"`
		Score *int64   `as:"score"`
		Ratio *float64 `as:"ratio"`
	}

	gg.It("must round-trip pointer fields, distinguishing absent from zero", func() {
		s, _ := newSession()
		ds, err := sdk.TypedDataSetOf[Optional](*namespace, randomSet())
		gm.Expect(err).ToNot(gm.HaveOccurred())

		score := int64(0) // present and zero, which must not read back as absent
		_, err = s.UpsertTyped(ds).
			Object(&Optional{ID: 1, Name: "has-zero", Score: &score}).
			Object(&Optional{ID: 2, Name: "has-none"}).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.QueryTyped(ds).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		objs, err := stream.IntoObjects()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(objs).To(gm.HaveLen(2))

		byID := map[int64]*Optional{}
		for _, o := range objs {
			byID[o.ID] = o
		}
		gm.Expect(byID[1].Score).ToNot(gm.BeNil(), "a present zero must not read back as absent")
		gm.Expect(*byID[1].Score).To(gm.BeEquivalentTo(0))
		gm.Expect(byID[2].Score).To(gm.BeNil(), "an unwritten bin must read back as absent")
		gm.Expect(byID[1].Ratio).To(gm.BeNil())
	})

	gg.It("must expose the mapping for a hand-built segment", func() {
		bins, err := sdk.BinsOf(&Customer{ID: 5, Name: "Ada", Age: 36})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		// The key field never becomes a bin, whichever path does the mapping.
		gm.Expect(bins).ToNot(gm.HaveKey("ID"))
		gm.Expect(bins).To(gm.HaveKeyWithValue("name", "Ada"))

		id, err := sdk.IDOf(&Customer{ID: 5, Name: "Ada"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(id).To(gm.BeEquivalentTo(5))
	})

	gg.It("must map one row at a time, for a heterogeneous batch", func() {
		s, _ := newSession()
		ds, err := sdk.TypedDataSetOf[Customer](*namespace, randomSet())
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = s.UpsertTyped(ds).Object(&Customer{ID: 7, Name: "Grace", Age: 45}).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		key := ds.Key(int64(7))
		stream, err := s.Query(key).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		obj, err := sdk.ObjectFromRecord[Customer](row)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(obj.Name).To(gm.Equal("Grace"))
		gm.Expect(obj.ID).To(gm.BeEquivalentTo(7))
	})

	gg.It("must render the enum settings by name in Explain", func() {
		text := sdk.DefaultBehavior().Explain()
		gm.Expect(text).To(gm.ContainSubstring("replica=SEQUENCE"))
		gm.Expect(text).To(gm.ContainSubstring("read_mode_sc=LINEARIZE"))
	})
})

var _ = gg.Describe("SDK batch correctness", func() {
	// An Exists segment is a header read. Building it as a write made the server
	// answer NO_RESPONSE for every row in the batch, not just that segment's, so
	// one existence check silently poisoned its neighbours.
	gg.It("must not let an Exists segment poison the rest of a batch", func() {
		s, ds := newSession()
		present := ds.Key("present")
		absent := ds.Key("absent")
		other := ds.Key("other")
		gm.Expect(s.Put(present, as.BinMap{"v": 1})).ToNot(gm.HaveOccurred())

		stream, err := s.Exists([]*as.Key{present, absent}).
			Upsert(other).SetTo("v", 2).
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for _, r := range rows {
			gm.Expect(r.ResultCode).ToNot(gm.BeEquivalentTo(types.NO_RESPONSE),
				"no row should come back NO_RESPONSE")
		}

		// The write in the same chain must have landed.
		rec, err := s.Get(other, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["v"]).To(gm.BeEquivalentTo(2))
	})

	gg.It("must report existence for a multi-key check", func() {
		s, ds := newSession()
		present := ds.Key("yes")
		absent := ds.Key("no")
		gm.Expect(s.Put(present, as.BinMap{"v": 1})).ToNot(gm.HaveOccurred())

		stream, err := s.Exists([]*as.Key{present, absent}).IncludeMissingKeys().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))

		found := map[string]bool{}
		for _, r := range rows {
			exists, err := r.AsBool()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			found[r.Key.Value().String()] = exists
		}
		gm.Expect(found["yes"]).To(gm.BeTrue())
		gm.Expect(found["no"]).To(gm.BeFalse())
	})

	gg.It("must honor WithNoBins on a set-wide query", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{1, 2})
		_, err := s.Upsert(keys).SetTo("v", 1).SetTo("w", 2).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(ds).WithNoBins().Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))
		for _, r := range rows {
			gm.Expect(r.Record.Bins).To(gm.BeEmpty(),
				"a header-only scan must return metadata without bins")
			gm.Expect(r.Record.Generation).To(gm.BeNumerically(">", 0))
		}
	})

	gg.It("must not crash on a nil bit policy", func() {
		s, ds := newSession()
		key := ds.Key("bits")
		gm.Expect(s.Put(key, as.BinMap{"b": []byte{0x00, 0x00}})).ToNot(gm.HaveOccurred())

		// The core client dereferences the policy unguarded, so a nil one used to
		// be a segfault rather than an error.
		_, err := s.Upsert(key).Bin("b").BitSet(nil, 0, 8, []byte{0xff}).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["b"]).To(gm.BeEquivalentTo([]byte{0xff, 0x00}))
	})

	gg.It("must report a nested struct as a mapping error, not a panic", func() {
		type Address struct {
			City string `as:"city"`
		}
		type Person struct {
			ID   int64   `as:",key"`
			Home Address `as:"home"`
		}
		s, _ := newSession()
		ds, err := sdk.TypedDataSetOf[Person](*namespace, randomSet())
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = s.UpsertTyped(ds).Object(&Person{ID: 1, Home: Address{City: "Zurich"}}).Execute()
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("nested struct"))
		gm.Expect(err.Error()).To(gm.ContainSubstring("RecordMapper"))
	})
})

// The UDF specs cover both routings of one verb: a one-key ExecuteUDF is a point
// call through the core's Execute, while a multi-key one is a batch UDF whose
// Lua value has to be lifted out of the server's SUCCESS bin. Asserting the same
// result shape from both is what keeps the two paths honest.
var _ = gg.Describe("SDK user-defined functions", func() {
	const module = "sdk_bench_udf.lua"
	const body = `
function echo(rec, value)
    return value
end

function bump(rec, delta)
    if not aerospike:exists(rec) then
        rec['n'] = delta
        aerospike:create(rec)
    else
        rec['n'] = (rec['n'] or 0) + delta
        aerospike:update(rec)
    end
    return rec['n']
end
`

	registerOnce := func(s *sdk.Session) {
		task, err := s.RegisterUDF([]byte(body), module, as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-task.OnComplete()).ToNot(gm.HaveOccurred())
	}

	gg.It("must route a one-key UDF through the point call and return its value", func() {
		s, ds := newSession()
		registerOnce(s)
		key := ds.Key("udf_single")

		stream, err := s.ExecuteUDF(key).Function("sdk_bench_udf", "echo").
			Passing(as.NewValue(42)).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()

		res, err := stream.FirstUDFResult()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(res).ToNot(gm.BeNil())
		gm.Expect(res.GetObject()).To(gm.BeEquivalentTo(42))
	})

	gg.It("must apply a one-key UDF's writes", func() {
		s, ds := newSession()
		registerOnce(s)
		key := ds.Key("udf_write")

		for range 3 {
			stream, err := s.ExecuteUDF(key).Function("sdk_bench_udf", "bump").
				Passing(as.NewValue(5)).Execute()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			stream.Close()
		}

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["n"]).To(gm.BeEquivalentTo(15))
	})

	gg.It("must return the same shape from the batch routing", func() {
		s, ds := newSession()
		registerOnce(s)
		keys := ds.Keys([]int64{1, 2, 3})

		stream, err := s.ExecuteUDF(keys).Function("sdk_bench_udf", "echo").
			Passing(as.NewValue(7)).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()

		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(3))
		for _, row := range rows {
			gm.Expect(row.UDFResult).ToNot(gm.BeNil())
			gm.Expect(row.UDFResult.GetObject()).To(gm.BeEquivalentTo(7))
		}
	})

	gg.It("must report a missing module as an error, not a panic", func() {
		s, ds := newSession()
		_, err := s.ExecuteUDF(ds.Key("udf_absent")).
			Function("no_such_module", "nope").Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

// TypedKey closes a gap against the Java and Rust SDKs, which both carry the
// entity type on a key. The point of these specs is that the type survives the
// key hop, so a read needs no dataset argument to produce objects.
var _ = gg.Describe("SDK typed keys", func() {
	newTyped := func() (*sdk.Session, *sdk.TypedDataSet[Customer]) {
		s, err := testCluster.CreateSession(nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		ds, err := sdk.TypedDataSetOf[Customer](*namespace, randomSet())
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return s, ds
	}

	gg.It("must read one entity through a typed key, with no dataset argument", func() {
		s, customers := newTyped()
		_, err := s.UpsertTyped(customers).
			Object(&Customer{ID: 1, Name: "Ada", Age: 36}).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		key := customers.TypedKey(int64(1))
		gm.Expect(key.IsZero()).To(gm.BeFalse())
		gm.Expect(key.Namespace()).To(gm.Equal(*namespace))
		gm.Expect(key.SetName()).To(gm.Equal(customers.SetName()))

		stream, err := s.QueryTypedKey(key).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()
		obj, err := stream.FirstObject()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(obj.Name).To(gm.Equal("Ada"))
		gm.Expect(obj.ID).To(gm.BeEquivalentTo(1))
	})

	gg.It("must read many entities through a typed key list", func() {
		s, customers := newTyped()
		_, err := s.UpsertTyped(customers).Objects([]*Customer{
			{ID: 1, Name: "Ada"}, {ID: 2, Name: "Bob"}, {ID: 3, Name: "Cy"},
		}).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		keys := customers.TypedKeys([]int64{1, 2, 3})
		gm.Expect(keys).To(gm.HaveLen(3))
		gm.Expect(keys.Keys()).To(gm.HaveLen(3))

		stream, err := s.QueryTypedKeyList(keys).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()
		objs, err := stream.IntoObjects()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(objs).To(gm.HaveLen(3))
	})

	gg.It("must mint a typed key from an entity instance", func() {
		s, customers := newTyped()
		alice := &Customer{ID: 7, Name: "Grace", Age: 45}
		_, err := s.UpsertTyped(customers).Object(alice).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		key, err := customers.TypedKeyForObject(alice)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(key.Key().Digest()).To(gm.Equal(customers.Key(int64(7)).Digest()))

		stream, err := s.QueryTypedKey(key).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()
		obj, err := stream.FirstObject()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(obj.Name).To(gm.Equal("Grace"))
	})

	gg.It("must unwrap into the untyped verbs", func() {
		s, customers := newTyped()
		key := customers.TypedKey(int64(9))

		// A write builder carries no entity type, so a typed key unwraps for it.
		_, err := s.Upsert(key.Key()).SetTo("name", "Zoe").Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key.Key(), sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["name"]).To(gm.Equal("Zoe"))
	})

	gg.It("must wrap and round-trip existing untyped keys", func() {
		_, customers := newTyped()
		raw := customers.Keys([]int64{4, 5})

		wrapped := sdk.TypedKeysOf[Customer](raw)
		gm.Expect(wrapped).To(gm.HaveLen(2))
		gm.Expect(wrapped.Keys()).To(gm.Equal(raw))

		one := sdk.TypedKeyOf[Customer](raw[0])
		gm.Expect(one.Key()).To(gm.Equal(raw[0]))
	})

	gg.It("must report a zero typed key without panicking", func() {
		var zero sdk.TypedKey[Customer]
		gm.Expect(zero.IsZero()).To(gm.BeTrue())
		gm.Expect(zero.Key()).To(gm.BeNil())
		gm.Expect(zero.Namespace()).To(gm.BeEmpty())
		gm.Expect(zero.SetName()).To(gm.BeEmpty())
		gm.Expect(zero.String()).To(gm.ContainSubstring("nil"))
	})
})

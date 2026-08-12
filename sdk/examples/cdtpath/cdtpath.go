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

// Package cdtpath iterates, collects and modifies nested CDTs with path
// expressions.
//
// Port of the Java SDK's CdtPathExpressionExample, by way of the Rust SDK's
// `cdt_path_expression`. Ordinary CDT navigation addresses one place in a
// collection; a path expression addresses *every* place that matches, so a
// single operation can bump every element of a list or collect a leaf out of
// every child of a document. The example walks five shapes: OnEachChild with
// ModifyBy, a filtered OnEachChildWhere removal, two filtered layers collecting
// leaf values, a nested ModifyBy price bump, and the same selection delivered as
// an expression read.
//
// Loop-variable expressions -- as.ExpIntLoopVar, as.ExpFloatLoopVar,
// as.ExpMapLoopVar, as.ExpStringLoopVar -- are what makes a filter or a modify
// expression refer to the node currently under consideration.
//
// Path expressions need server 8.1.1 or later; the example reports and stops
// otherwise.
package cdtpath

import (
	"fmt"
	"math"
	"sort"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// originalPrices are the prices of the four books, in catalog order.
var originalPrices = []float64{8.95, 12.99, 8.99, 22.99}

// Run executes the example.
func Run(env *exrun.Env) error {
	if !env.Cluster.SupportsCDTPathExpressions() {
		env.Printf("Skipping: CDT path expressions require server 8.1.1 or later.")
		return nil
	}

	set, err := env.DataSet("cdt_path_demo")
	if err != nil {
		return err
	}

	allOK := true

	// ------------------------------------------------------------------
	// 1) Every element of a list, incremented in place.
	// ------------------------------------------------------------------
	env.Printf("--- 1) Bin-root list: OnEachChild + ModifyBy (increment each element) ---")
	key1 := set.Key(int64(1))
	nums := []any{1, 2, 3}
	if err := putBin(env.Session, key1, "nums", nums); err != nil {
		return err
	}
	env.Printf("initial nums (before +10 to each): %s", exrun.Render(nums))

	// The modify expression runs once per selected node, with the loop variable
	// bound to that node's value.
	stream, err := env.Session.Upsert(key1).
		Bin("nums").OnEachChild().
		ModifyBy(as.ExpNumAdd(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(10))).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.FirstOrRaise(); err != nil {
		return err
	}

	after, err := readBin(env.Session, key1, "nums")
	if err != nil {
		return err
	}
	env.Printf("nums after +10 to each: %s", exrun.Render(after))
	allOK = report(env, 1, equalInts(after, []int64{11, 12, 13})) && allOK

	// ------------------------------------------------------------------
	// 2) Only the children a filter matches, removed.
	// ------------------------------------------------------------------
	env.Printf("--- 2) Bin-root list: OnEachChildWhere(filter) + remove ---")
	key2 := set.Key(int64(2))
	nums2 := []any{3, 7, 2, 9}
	if err := putBin(env.Session, key2, "nums", nums2); err != nil {
		return err
	}
	env.Printf("initial nums (before removing values > 5): %s", exrun.Render(nums2))

	// A removal is a modify whose expression yields the "remove this node"
	// result, so the filtered selection and the removal are one operation.
	stream, err = env.Session.Upsert(key2).
		Bin("nums").
		OnEachChildWhere(as.ExpGreater(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(5))).
		ModifyBy(as.ExpRemoveResult()).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.FirstOrRaise(); err != nil {
		return err
	}

	after, err = readBin(env.Session, key2, "nums")
	if err != nil {
		return err
	}
	env.Printf("nums after removing values > 5: %s", exrun.Render(after))
	allOK = report(env, 2, equalInts(after, []int64{3, 2})) && allOK

	// ------------------------------------------------------------------
	// 3) Two filtered each-child layers, collecting leaf values.
	// ------------------------------------------------------------------
	env.Printf("--- 3) Nested map/list: titles of books priced <= 10 (CollectValues) ---")
	key3 := set.Key(int64(3))
	catalog := bookCatalog()
	if err := putBin(env.Session, key3, "catalog", catalog); err != nil {
		return err
	}
	env.Printf("initial catalog (before collecting cheap-book titles): %s", exrun.Render(catalog))

	// Three layers: the root's "book" entry, then the book maps whose "price" is
	// at most 10.0, then each survivor's "title" entry. CollectValues reports the
	// values of whatever the last layer selected.
	qstream, err := env.Session.Query(key3).
		Bin("catalog").
		OnEachChildWhere(keyIs("book")).
		OnEachChildWhere(as.ExpLessEq(priceOfChild(), as.ExpFloatVal(10.0))).
		OnEachChildWhere(keyIs("title")).
		CollectValues().
		Execute()
	if err != nil {
		return err
	}
	titles, err := projection(qstream, "catalog")
	if err != nil {
		return err
	}
	env.Printf("collected titles (price <= 10) in projection bin 'catalog': %s", exrun.Render(titles))
	allOK = report(env, 3, equalStrings(sortedStrings(titles),
		[]string{"Moby Dick", "Sayings of the Century"})) && allOK

	// ------------------------------------------------------------------
	// 4) A modify expression applied to one key of every child map.
	// ------------------------------------------------------------------
	env.Printf("--- 4) Nested map/list: multiply every book price by 1.10 (ModifyBy) ---")
	key4 := set.Key(int64(4))
	catalog = bookCatalog()
	if err := putBin(env.Session, key4, "catalog", catalog); err != nil {
		return err
	}
	env.Printf("initial catalog (before the 1.10x price bump): %s", exrun.Render(catalog))

	// One operation reaches four leaves: the "price" of every book under "book".
	stream, err = env.Session.Upsert(key4).
		Bin("catalog").
		OnEachChildWhere(keyIs("book")).
		OnEachChild().
		OnMapKey("price").
		ModifyBy(as.ExpNumMul(as.ExpFloatLoopVar(as.VALUE), as.ExpFloatVal(1.10))).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.FirstOrRaise(); err != nil {
		return err
	}

	bumped, err := readBin(env.Session, key4, "catalog")
	if err != nil {
		return err
	}
	env.Printf("catalog after the 10%% price bump: %s", exrun.Render(bumped))
	allOK = report(env, 4, pricesBumped(bumped, 1.10, 0.02)) && allOK

	// ------------------------------------------------------------------
	// 5) The same selection delivered as an expression read.
	// ------------------------------------------------------------------
	env.Printf("--- 5) Expression read: the same selection as a computed projection bin ---")
	key5 := set.Key(int64(5))
	catalog = bookCatalog()
	if err := putBin(env.Session, key5, "catalog", catalog); err != nil {
		return err
	}
	env.Printf("initial catalog (before the expression read of all titles): %s", exrun.Render(catalog))

	// A path selection is also available as a plain expression, which any read
	// that evaluates an expression can carry. That is the form to reach for when
	// the selection has to compose with other expression machinery -- or, as
	// here, when the path starts at a fixed map key rather than at each child.
	selectTitles := as.ExpSelectByPath(
		as.ExpTypeLIST,
		as.EXP_PATH_SELECT_VALUE,
		as.ExpMapBin("catalog"),
		as.CtxMapKey(as.NewValue("book")),
		as.CtxAllChildren(),
		as.CtxMapKey(as.NewValue("title")),
	)
	qstream, err = env.Session.Query(key5).
		Bin("catalog").SelectFrom(selectTitles, false).
		Execute()
	if err != nil {
		return err
	}
	allTitles, err := projection(qstream, "catalog")
	if err != nil {
		return err
	}
	env.Printf("expression read (all titles) in bin 'catalog': %s", exrun.Render(allTitles))
	allOK = report(env, 5, equalStrings(sortedStrings(allTitles), []string{
		"Moby Dick",
		"Sayings of the Century",
		"Sword of Honour",
		"The Lord of the Rings",
	})) && allOK

	if !allOK {
		return fmt.Errorf("one or more CDT path expression checks failed")
	}
	env.Printf("Overall: SUCCESS")
	return nil
}

// report prints and returns one step's verdict.
func report(env *exrun.Env, step int, ok bool) bool {
	verdict := "*** FAILURE ***"
	if ok {
		verdict = "SUCCESS"
	}
	env.Printf("Step %d: %s", step, verdict)
	return ok
}

// priceOfChild is the "price" of the child map currently bound to the loop
// variable, as a float.
func priceOfChild() *as.Expression {
	return as.ExpMapGetByKey(
		as.MapReturnType.VALUE,
		as.ExpTypeFLOAT,
		as.ExpStringVal("price"),
		as.ExpMapLoopVar(as.VALUE),
	)
}

// keyIs matches the map entry currently being iterated by its key.
func keyIs(name string) *as.Expression {
	return as.ExpEq(as.ExpStringLoopVar(as.MAP_KEY), as.ExpStringVal(name))
}

// bookCatalog is the Java fixture: a root map whose "book" key holds a list of
// book maps.
func bookCatalog() map[any]any {
	titles := []string{
		"Sayings of the Century",
		"Sword of Honour",
		"Moby Dick",
		"The Lord of the Rings",
	}
	books := make([]any, 0, len(titles))
	for i, title := range titles {
		books = append(books, map[any]any{"title": title, "price": originalPrices[i]})
	}
	return map[any]any{"book": books}
}

// putBin writes one bin.
func putBin(session *sdk.Session, key *as.Key, bin string, value any) error {
	return session.Put(key, as.BinMap{bin: value})
}

// readBin reads one bin back.
func readBin(session *sdk.Session, key *as.Key, bin string) (any, error) {
	rec, err := session.Get(key, sdk.AllBins)
	if err != nil {
		return nil, err
	}
	value, ok := rec.Bins[bin]
	if !ok {
		return nil, fmt.Errorf("bin %q is absent", bin)
	}
	return value, nil
}

// projection takes the computed bin off the single row a path read produces.
func projection(stream *sdk.RecordStream, bin string) (any, error) {
	row, err := stream.FirstOrRaise()
	if err != nil {
		return nil, err
	}
	rec, err := row.RecordOrRaise()
	if err != nil {
		return nil, err
	}
	value, ok := rec.Bins[bin]
	if !ok {
		return nil, fmt.Errorf("projection bin %q is absent", bin)
	}
	return value, nil
}

// mapGet fetches a key from either map shape the server may return.
func mapGet(value any, key string) any {
	switch m := value.(type) {
	case map[any]any:
		return m[key]
	case []as.MapPair:
		for _, p := range m {
			if fmt.Sprintf("%v", p.Key) == key {
				return p.Value
			}
		}
	}
	return nil
}

// asFloat widens whatever numeric shape a price came back as.
func asFloat(value any) float64 {
	switch n := value.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return math.NaN()
}

// pricesBumped reports whether every book price is original * factor, within
// epsilon.
func pricesBumped(catalog any, factor, epsilon float64) bool {
	books, ok := mapGet(catalog, "book").([]any)
	if !ok || len(books) != len(originalPrices) {
		return false
	}
	for i, book := range books {
		if math.Abs(asFloat(mapGet(book, "price"))-originalPrices[i]*factor) > epsilon {
			return false
		}
	}
	return true
}

// equalInts compares a returned list against the integers expected in it.
func equalInts(value any, want []int64) bool {
	items, ok := value.([]any)
	if !ok || len(items) != len(want) {
		return false
	}
	for i, item := range items {
		if int64(asFloat(item)) != want[i] {
			return false
		}
	}
	return true
}

// sortedStrings reports the strings in a list result, sorted -- collection order
// is not defined.
func sortedStrings(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	sort.Strings(out)
	return out
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

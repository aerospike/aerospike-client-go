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

// Package queryexamples is the broad tour of the SDK's query, batch, CDT,
// expression and object-mapping surface.
//
// Port of the Java SDK's QueryExamples (plus its
// query/{Customer,CustomerMapper,Address,AddressMapper} classes, which collapse
// into two Go types here), by way of the Rust SDK's `query_examples`. Each
// logical area lives in its own method sharing one session and one set of
// datasets: cluster info, writes and expected errors, seeding, secondary
// indexes, conditional and background updates, batch reads, filtered updates,
// point/header/projection reads, object lists and chunked iteration,
// client-side sorting and pagination, TTLs, read/write expressions,
// multi-operation batches, object mapping including a nested value struct,
// generation checks, complex CDT and bitwise operations, a heterogeneous batch,
// and the typed stream.
//
// Two Java sections are intentionally not ported:
//
//   - demonstratePreparedAel — client-side AEL with `?0` placeholder binding.
//     This SDK does not parse or rewrite AEL (the server compiles it), so there
//     is no PreparedAel; build the filter string or use a typed Expression.
//   - demonstrateQueryHints — index-selection hints (forIndex / forBin), which
//     existed to drive client-side index selection. Pick an index explicitly
//     with Filter instead, as the secondary-index section here does.
package queryexamples

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// ===== Entities =============================================================

// Address is the value object nested inside a [Customer]. It has no user key of
// its own: it is written as a map, either as one bin of a customer or as the
// bins of a record in the `address` set, which is the same shape either way.
type Address struct {
	Line1   string
	City    string
	State   string
	Country string
	ZipCode string
}

// AsMap renders the address the way the server stores it, with `zip` as the bin
// name the Java mapper used.
func (a *Address) AsMap() map[any]any {
	return map[any]any{
		"line1":   a.Line1,
		"city":    a.City,
		"state":   a.State,
		"country": a.Country,
		"zip":     a.ZipCode,
	}
}

func (a *Address) String() string {
	if a == nil {
		return "<none>"
	}
	return fmt.Sprintf("Address{%s, %s, %s, %s, %s}", a.Line1, a.City, a.State, a.Country, a.ZipCode)
}

// Customer is the entity type for the `person` set.
//
// It takes control of its own mapping by implementing [sdk.RecordMapper] rather
// than relying on the struct tags, because reflection cannot write the nested
// Address: a Go struct is not a value the wire protocol knows. ToBins turns it
// into a map, which is exactly what the Java AddressMapper did by hand and what
// the Rust derive calls its value mode.
type Customer struct {
	CustomerID int64
	Name       string
	Age        int64
	DOB        int64
	Address    *Address
}

// newCustomer is the Java constructor: an id, a name and an age, stamped now.
func newCustomer(id int64, name string, age int64) *Customer {
	return &Customer{CustomerID: id, Name: name, Age: age, DOB: nowMillis()}
}

// ID reports the user key.
func (c *Customer) ID() any { return c.CustomerID }

// ToBins reports the bins to write. An absent address is omitted rather than
// written as nil, which would be a delete-bin operation.
func (c *Customer) ToBins() (as.BinMap, error) {
	bins := as.BinMap{"name": c.Name, "age": c.Age, "dob": c.DOB}
	if c.Address != nil {
		bins["address"] = c.Address.AsMap()
	}
	return bins, nil
}

// SetFromRecord rebuilds a customer from a record. The set holds many partial
// records, so every bin is treated as optional.
func (c *Customer) SetFromRecord(bins as.BinMap, key *as.Key, generation uint32) error {
	if key != nil && key.Value() != nil {
		if id, ok := asInt64(key.Value().GetObject()); ok {
			c.CustomerID = id
		}
	}
	c.Name, _ = bins["name"].(string)
	c.Age, _ = asInt64(bins["age"])
	c.DOB, _ = asInt64(bins["dob"])
	c.Address = addressFrom(bins["address"])
	return nil
}

func (c *Customer) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Customer{id=%d name=%q age=%d dob=%d address=%s}",
		c.CustomerID, c.Name, c.Age, c.DOB, c.Address)
}

// ===== Filters ==============================================================

// predicate carries one filter in both forms the SDK accepts: AEL source, which
// only a server from 8.1.3 can compile, and the equivalent typed expression,
// which the client compiles for any server. The tour picks a form once, so the
// output is the same either way.
type predicate struct {
	ael  string
	expr *as.Expression
}

var (
	nameIsTim = predicate{
		ael:  "$.name == 'Tim'",
		expr: as.ExpEq(as.ExpStringBin("name"), as.ExpStringVal("Tim")),
	}
	timOverThirty = predicate{
		ael: "$.name == 'Tim' and $.age > 30",
		expr: as.ExpAnd(
			as.ExpEq(as.ExpStringBin("name"), as.ExpStringVal("Tim")),
			as.ExpGreater(as.ExpIntBin("age"), as.ExpIntVal(30))),
	}
	ageUnder35 = predicate{
		ael:  "$.age < 35",
		expr: as.ExpLess(as.ExpIntBin("age"), as.ExpIntVal(35)),
	}
	ageUnder21 = predicate{
		ael:  "$.age < 21",
		expr: as.ExpLess(as.ExpIntBin("age"), as.ExpIntVal(21)),
	}
	notYetUpdated = predicate{
		ael:  "$.updated == false",
		expr: as.ExpEq(as.ExpBoolBin("updated"), as.ExpBoolVal(false)),
	}
	stateIsNSW = predicate{
		ael:  "$.state == 'nsw'",
		expr: as.ExpEq(as.ExpStringBin("state"), as.ExpStringVal("nsw")),
	}
	activeAdults = predicate{
		ael: "$.status == 'active' and $.age >= 21",
		expr: as.ExpAnd(
			as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("active")),
			as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(21))),
	}
)

// ===== The tour =============================================================

// tour is the state every section shares: the tuned session and the datasets.
type tour struct {
	env       *exrun.Env
	session   *sdk.Session
	person    *sdk.DataSet
	customers *sdk.TypedDataSet[Customer]
	addresses *sdk.DataSet
	users     *sdk.DataSet

	// ael records whether filters travel as AEL source or as typed
	// expressions.
	ael bool
}

// Run executes the example.
func Run(env *exrun.Env) error {
	person, err := env.DataSet("person")
	if err != nil {
		return err
	}
	addresses, err := env.DataSet("address")
	if err != nil {
		return err
	}
	users, err := env.DataSet("users")
	if err != nil {
		return err
	}

	// Java derives a tuned policy bundle before opening the session; the same
	// scope-keyed patches apply here.
	behavior := sdk.DefaultBehavior().DeriveWithChanges("queryExamples", map[sdk.Scope]sdk.Settings{
		sdk.ScopeAll: {
			SocketTimeout: sdk.DurationPtr(3 * time.Second),
			SendKey:       sdk.BoolPtr(true),
		},
		sdk.ScopeReadsQuery: {
			SocketTimeout: sdk.DurationPtr(2 * time.Second),
			TotalTimeout:  sdk.DurationPtr(30 * time.Second),
		},
		sdk.ScopeReadsBatch: {
			MaxRetries:  sdk.IntPtr(6),
			AllowInline: sdk.BoolPtr(true),
		},
	})
	session, err := env.Session.SessionFor(behavior)
	if err != nil {
		return err
	}

	t := &tour{
		env:       env,
		session:   session,
		person:    person,
		customers: sdk.TypedDataSetFrom[Customer](person),
		addresses: addresses,
		users:     users,
		ael:       env.Cluster.SupportsServerCompiledAEL(),
	}
	if t.ael {
		env.Printf("Filters travel as AEL source; the server compiles them.")
	} else {
		env.Printf("This cluster is below 8.1.3, so filters travel as client-compiled expressions.")
	}

	sample := sampleCustomers()

	if err := t.printClusterInfo(); err != nil {
		return err
	}
	if err := t.basicWritesAndErrors(); err != nil {
		return err
	}
	if err := t.seedData(sample); err != nil {
		return err
	}
	if err := t.secondaryIndexQuery(); err != nil {
		return err
	}
	if err := t.conditionalUpdates(sample); err != nil {
		return err
	}
	if err := t.batchReads(); err != nil {
		return err
	}
	if err := t.filteredUpdates(); err != nil {
		return err
	}
	if err := t.pointAndHeaderReads(); err != nil {
		return err
	}
	if err := t.objectListAndChunking(); err != nil {
		return err
	}
	if err := t.sortingAndPagination(); err != nil {
		return err
	}
	if err := t.ttl(); err != nil {
		return err
	}
	if err := t.readWriteExpressions(); err != nil {
		return err
	}
	if err := t.backgroundQuery(); err != nil {
		return err
	}
	if err := t.multiOperationBatches(); err != nil {
		return err
	}
	readBack, err := t.objectMapping()
	if err != nil {
		return err
	}
	if err := t.generationCheck(readBack); err != nil {
		return err
	}
	if err := t.complexCDT(); err != nil {
		return err
	}
	if err := t.bitOperations(); err != nil {
		return err
	}
	if err := t.heterogeneousBatch(); err != nil {
		return err
	}
	return t.typedStream()
}

// ===== Cluster info =========================================================

// printClusterInfo reports the namespaces, one namespace's stats, and the
// secondary indexes the cluster already carries.
func (t *tour) printClusterInfo() error {
	t.env.Printf("--- Cluster info ---")
	info := t.session.InfoCommands()

	namespaces, err := info.Namespaces()
	if err != nil {
		return err
	}
	t.env.Printf("  Namespaces: %v", namespaces)
	for _, namespace := range namespaces {
		detail, err := info.NamespaceDetail(namespace)
		if err != nil || detail == nil {
			continue
		}
		objects, _ := detail.Objects()
		replication, _ := detail.EffectiveReplicationFactor()
		sc, _ := detail.StrongConsistency()
		t.env.Printf("    %s: objects=%d replication-factor=%d strong-consistency=%t",
			namespace, objects, replication, sc)
	}

	sindexes, err := info.SindexList("")
	if err != nil {
		return err
	}
	t.env.Printf("  Secondary indexes: %d defined (showing up to 5)", len(sindexes))
	for i, sindex := range sindexes {
		if i == 5 {
			break
		}
		t.env.Printf("    %s.%s on bin %q (%s, %s)",
			sindex.Namespace, sindex.IndexName, sindex.Bin, sindex.IndexType, sindex.State)
	}
	return nil
}

// ===== Basic writes and expected errors =====================================

// basicWritesAndErrors runs the write verbs and the errors they raise: an
// operation-less update, an update of a record that does not exist — as an
// error and then as an in-stream row — and finally a plain insert and upsert.
func (t *tour) basicWritesAndErrors() error {
	t.env.Printf("--- Basic writes and expected errors ---")
	key := t.person.Key(int64(1))

	if err := drain(t.session.Update(key).Execute()); err != nil {
		t.env.Printf("  operation-less update rejected: %s", errSummary(err))
	} else {
		t.env.Printf("  operation-less update succeeded (server default)")
	}

	// Update needs an existing record, and the set was just truncated.
	if err := drain(t.session.Update(key).Bin("bob").SetTo(5).Execute()); err != nil {
		t.env.Printf("  update of a missing record failed as expected: %s", errSummary(err))
	} else {
		t.env.Printf("  update of a missing record succeeded — unexpected")
	}

	// The same failure as data rather than as an error.
	stream, err := t.session.Update(key).Bin("bob").SetTo(5).ExecuteOnError(sdk.InStream())
	if err != nil {
		return err
	}
	defer stream.Close()
	row, err := stream.Next()
	if err != nil {
		return err
	}
	if row != nil {
		t.env.Printf("  in-stream disposition: result code %s", row.ResultCode)
	}

	if err := drain(t.session.Insert(key).
		Bin("Name").SetTo("test1").
		Bin("i1").SetTo(1).
		Bin("i2").SetTo(2).
		Bin("f1").SetTo(1.1).
		Bin("f2").SetTo(2.2).
		Bin("s1").SetTo("hello ").
		Bin("s2").SetTo("world").
		Execute()); err != nil {
		return err
	}

	// A string user key in a set of integer-keyed customers is fine for an
	// untyped read, so read it back through a projection and then remove it
	// again: the mapper below expects integer keys.
	bob := t.person.Key("bob")
	if err := drain(t.session.Upsert(bob).Bin("A").SetTo(2).Bin("B").SetTo(2.2).Execute()); err != nil {
		return err
	}
	projection, err := t.session.Query(bob).Bins("name").Execute()
	if err != nil {
		return err
	}
	defer projection.Close()
	if row, err := projection.Next(); err != nil {
		return err
	} else if row != nil {
		t.env.Printf("  projection of an absent bin: %s", describe(row))
	}
	return drain(t.session.Delete(bob).Execute())
}

// ===== Seeding ==============================================================

// sampleCustomers are the seed customers, inserted as mapped objects and reused
// later for the conditional update.
func sampleCustomers() []*Customer {
	rows := []struct {
		id   int64
		name string
		age  int64
	}{
		{20, "Jordan", 36}, {21, "Alex", 27}, {22, "Betty", 27}, {23, "Bob", 33},
		{24, "Fred", 6}, {25, "Alex", 28}, {26, "Alex", 26}, {27, "Jordan", 19},
		{28, "Gruper", 28}, {29, "Bree", 24}, {30, "Perry", 44}, {31, "Alex", 27},
		{32, "Betty", 27}, {33, "Wilma", 18}, {34, "Joran", 82}, {35, "Alex", 27},
		{36, "Fred", 99}, {37, "Sydney", 22}, {38, "Ita", 99}, {39, "Rupert", 83},
		{40, "Dominic", 53}, {41, "Tim", 27}, {42, "Tim", 29}, {43, "Tim", 31},
		{44, "Tim", 30}, {45, "Tim", 33}, {46, "Tim", 35},
	}
	out := make([]*Customer, 0, len(rows))
	for _, r := range rows {
		out = append(out, newCustomer(r.id, r.name, r.age))
	}
	return out
}

// seedData writes every shape the rest of the tour reads: batch bin updates,
// row writes, existence checks, deletes, the TTL variants, a deep CDT map, and
// the mapped-object insert.
func (t *tour) seedData(sample []*Customer) error {
	t.env.Printf("--- Seeding ---")

	// One operation applied to five keys.
	first5 := t.person.Keys([]int64{1, 2, 3, 4, 5})
	if err := drain(t.session.Upsert(first5).Bin("holdings").Add(1).Execute()); err != nil {
		return err
	}

	// Row-oriented writes: declare the bins once, then one row per record.
	if err := drain(t.session.UpsertRows(t.person).Bins("name", "age").
		Row(int64(1), "Tim", 312).
		Row(int64(2), "Bob", 25).
		Row(int64(3), "Jane", 46).
		Execute()); err != nil {
		return err
	}

	key2 := t.person.Key(int64(2))
	existsStream, err := t.session.Exists([]*as.Key{key2}).Execute()
	if err != nil {
		return err
	}
	existsRow, err := existsStream.FirstOrRaise()
	if err != nil {
		return err
	}
	exists, err := existsRow.AsBool()
	if err != nil {
		return err
	}
	t.env.Printf("  id 2 exists: %t", exists)
	if err := drain(t.session.Delete([]*as.Key{key2}).WithoutDurableDelete().Execute()); err != nil {
		return err
	}

	key80 := t.person.Key(int64(80))
	stream, err := t.session.Upsert(key80).Bin("name").SetTo("Tim").Bin("age").SetTo(342).Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  upsert id 80 -> %s", describe(row))

	keys8182 := t.person.Keys([]int64{81, 82})
	if err := drain(t.session.Upsert(keys8182).
		Bin("name").SetTo("Tim").Bin("age").SetTo(343).Execute()); err != nil {
		return err
	}

	if err := drain(t.session.UpsertRows(t.person).Bins("name", "age").
		Row(int64(83), "Tim", 342).
		Row(int64(84), "Tim", 342).
		Row(int64(85), "Fred", 37).
		Execute()); err != nil {
		return err
	}

	// An absolute expiration time, matching Java's expireRecordAt(2030-01-01).
	key100 := t.person.Key(int64(100))
	if err := drain(t.session.Upsert(key100).
		Bin("name").SetTo("Tim").
		Bin("age").SetTo(312).
		Bin("dob").SetTo(nowMillis()).
		Bin("id2").SetTo(100).
		ExpireRecordAt(time.Unix(1_893_456_000, 0)).
		Execute()); err != nil {
		return err
	}

	ttlKeys := t.person.Keys([]int64{900, 901, 902, 903, 904, 905})
	if err := drain(t.session.Delete(ttlKeys).Execute()); err != nil {
		return err
	}

	// Builders are lazy: this one is never executed, so nothing is sent.
	_ = t.session.InsertRows(t.person).Bins("name", "age", "hair", "dob").
		Row(int64(899), "Tim", 312, "brown", nowMillis())

	// A per-row TTL on one row, a chain-wide default for the others.
	rows, err := t.session.InsertRows(t.person).Bins("name", "age", "hair", "dob").
		Row(int64(900), "Tim", 312, "brown", nowMillis()).
		Row(int64(901), "Jane", 28, "blonde", nowMillis()).
		Row(int64(902), "Bob", 54, "brown", nowMillis()).
		ExpireRecordAfter(5*24*time.Hour).
		Row(int64(903), "Jordan", 45, "red", nowMillis()).
		Row(int64(904), "Alex", 67, "blonde", nowMillis()).
		Row(int64(905), "Sam", 24, "brown", nowMillis()).
		DefaultExpireRecordAfter(30 * 24 * time.Hour).
		Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  rows with mixed TTLs:", rows); err != nil {
		return err
	}

	for i := int64(0); i < 15; i++ {
		key := t.person.Key(i)
		if err := drain(t.session.Upsert(key).
			Bin("name").SetTo(fmt.Sprintf("Tim-%d", i)).
			Bin("age").SetTo(312 + i).
			Bin("hair").SetTo("brown").
			Bin("dob").SetTo(nowMillis()).
			Execute()); err != nil {
			return err
		}
		if err := drain(t.session.UpsertRows(t.person).Bins("name", "age", "hair", "dob").
			Row(1000+i, fmt.Sprintf("Tim-%d", i), 312+i, "brown", nowMillis()).
			ExpireRecordAfter(30 * 24 * time.Hour).
			Execute()); err != nil {
			return err
		}
	}

	gone := t.person.Keys([]int64{1, 2, 3, 5, 7, 11, 13, 17})
	if err := drain(t.session.Delete(gone).Execute()); err != nil {
		return err
	}

	if err := t.recordLifeCycle(); err != nil {
		return err
	}

	// Mapped objects: one row per object, keys from ID().
	if err := drain(t.session.InsertTyped(t.customers).Objects(sample).Execute()); err != nil {
		return err
	}
	t.env.Printf("  inserted %d mapped customers", len(sample))
	return nil
}

// recordLifeCycle walks one record through insert, update and delete, then
// re-creates it with a deep CDT map and reworks that map in a single call.
func (t *tour) recordLifeCycle() error {
	key := t.person.Key(int64(102))
	if err := drain(t.session.Delete(key).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Insert(key).
		Bin("name").SetTo("Sue").
		Bin("age").SetTo(27).
		Bin("id").SetTo(102).
		Bin("dob").SetTo(nowMillis()).
		Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Update(key).Bin("age").SetTo(26).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Delete(key).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Upsert(key).
		Bin("name").SetTo("Sue").
		Bin("age").SetTo(27).
		Bin("id").SetTo(102).
		Bin("dob").SetTo(nowMillis()).
		Bin("rooms").SetTo(roomsMap()).
		Bin("rooms2").SetTo(map[any]any{"test": true}).
		Execute()); err != nil {
		return err
	}

	// One call mixing scalar writes, map reads, nested map writes, a map clear,
	// a range removal and a create-on-missing path. Several operations land on
	// the same bin, so the results have to be read positionally.
	keyOrdered := as.MapOrder.KEY_ORDERED
	stream, err := t.session.Upsert(key).
		Bin("name").SetTo("Bob").
		Bin("age").SetTo(30).
		Bin("id").Get().
		Bin("dob").SetTo(nowMillis()).
		Bin("rooms").OnMapIndex(2).GetValues().
		Bin("rooms").OnMapKeyRange("room1", "room2").CountAllOthers().
		Bin("rooms").OnMapKey("room1", nil).GetValues().
		Bin("rooms").OnMapKeyRange("room1", "room3").Count().
		Bin("rooms").OnMapKey("room1", nil).OnMapKey("rates", nil).OnMapKey(1, nil).SetTo(110).
		Bin("rooms").OnMapKey("room2", nil).MapClear().
		Bin("rooms").OnMapKeyRange("room4", "room9").Remove().
		Bin("rooms").OnMapKey("room1", nil).OnMapKey("rates", nil).OnMapKey(1, nil).Add(5).
		Bin("rooms").OnMapKeyRelativeIndexRange("bob", -1, 1).GetValues().
		Bin("rooms2").MapClear().
		Bin("rooms2").OnMapKey("child", &keyOrdered).OnMapKey("subChild", nil).SetTo(5).
		Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  multi-op results: %s", operationResults(row))
	if err := t.printRecord(key, "  record 102 after the multi-op"); err != nil {
		return err
	}

	if err := drain(t.session.Update(key).
		Bin("name").Append("-test").
		Bin("age").Add(1).
		Execute()); err != nil {
		return err
	}
	if err := t.printRecord(key, "  record 102 after append/add"); err != nil {
		return err
	}

	return drain(t.session.Upsert(key).
		Bin("name").SetTo("Sue").
		Bin("age").SetTo(26).
		Bin("dob").SetTo(nowMillis()).
		Execute())
}

// ===== Secondary indexes ====================================================

// secondaryIndexQuery builds an index, queries through it, and drops it again.
//
// A secondary-index filter is not a record filter: it selects which index the
// server walks, so it narrows what the query *visits* rather than what it
// returns. That is what Java's dropped query hints existed to express.
func (t *tour) secondaryIndexQuery() error {
	t.env.Printf("--- Secondary index ---")
	const indexName = "queryexamples_person_age_idx"

	// A leftover index from an interrupted run would collide with this one.
	_ = t.session.Index(t.person).Named(indexName).Drop()

	task, err := t.session.Index(t.person).OnBin("age").Named(indexName).Numeric().Create()
	if err != nil {
		return err
	}
	if err := <-task.OnComplete(); err != nil {
		return err
	}
	defer func() { _ = t.session.Index(t.person).Named(indexName).Drop() }()
	t.env.Printf("  created %s on person.age", indexName)

	stream, err := t.session.Query(t.person).
		Filter(as.NewRangeFilter("age", 25, 30)).
		Execute()
	if err != nil {
		return err
	}
	return t.printRows("  ages 25..30 through the index:", stream)
}

// ===== Conditional and background updates ===================================

// conditionalUpdates writes one filtered segment per object — all in one batch —
// and then runs a background query that touches every record in the set.
func (t *tour) conditionalUpdates(sample []*Customer) error {
	t.env.Printf("--- Conditional updates ---")
	t.env.Printf("  Updating all customers called Tim")

	// The typed object writer carries no filter, so the filter goes on untyped
	// segments instead: one segment per object, chained into a single batch.
	var chain *sdk.WriteSegmentBuilder
	for _, customer := range sample {
		key := t.person.Key(customer.CustomerID)
		bins, err := customer.ToBins()
		if err != nil {
			return err
		}
		target := []*as.Key{key}
		if chain == nil {
			chain = t.session.Update(target)
		} else {
			chain = chain.Update(target)
		}
		for _, name := range sortedNames(bins) {
			chain = chain.SetTo(name, bins[name])
		}
		chain = t.whereWrite(chain, nameIsTim)
	}
	stream, err := chain.Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  filtered update results:", stream); err != nil {
		return err
	}

	before, err := t.customerAge(46)
	if err != nil {
		return err
	}
	t.env.Printf("  customer 46 age before the background scan: %s", before)

	task, err := t.session.Query(t.person).
		WithWriteOperations(as.AddOp(as.NewBin("age", 1))).
		ExecuteBackgroundTask()
	if err != nil {
		return err
	}
	t.env.Printf("  background task id = %d", task.TaskId())
	if err := <-task.OnComplete(); err != nil {
		return err
	}

	after, err := t.customerAge(46)
	if err != nil {
		return err
	}
	t.env.Printf("  customer 46 age after the background scan: %s", after)
	return nil
}

// ===== Batch reads ==========================================================

// batchReads runs a batch behind a partition filter, a record filter,
// missing-key inclusion and FailOnFilteredOut, then the same read set-wide.
func (t *tour) batchReads() error {
	t.env.Printf("--- Batch reads ---")
	keys := idRange(t.person, 20, 48)

	stream, err := t.session.Query(keys).OnPartitionRange(0, 2048).Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  keys 20..48, but only partitions 0..2048:", stream); err != nil {
		return err
	}

	if stream, err = t.session.Query(keys).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  full batch read:", stream); err != nil {
		return err
	}

	if stream, err = t.whereQuery(t.session.Query(keys), nameIsTim).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  batch read where name == 'Tim':", stream); err != nil {
		return err
	}

	if stream, err = t.whereQuery(t.session.Query(keys).IncludeMissingKeys(), nameIsTim).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  same filter, including keys that were not found:", stream); err != nil {
		return err
	}

	if stream, err = t.whereQuery(t.session.Query(keys), nameIsTim).
		IncludeMissingKeys().FailOnFilteredOut().Execute(); err != nil {
		return err
	}
	if err := t.printRows("  same filter, reporting filtered-out rows as errors:", stream); err != nil {
		return err
	}

	if stream, err = t.session.Query(t.person).Limit(6).Execute(); err != nil {
		return err
	}
	return t.printRows("  set-wide read, limit 6:", stream)
}

// ===== Filtered updates =====================================================

// filteredUpdates contrasts the buffered and lazy terminals, then puts the same
// write behind a filter and behind FailOnFilteredOut.
func (t *tour) filteredUpdates() error {
	t.env.Printf("--- Filtered batch updates ---")
	keys := idRange(t.person, 20, 27)

	// Stream is lazy: rows arrive as nodes answer, and the writes are not
	// guaranteed complete when it returns.
	lazy, err := t.session.Update(keys).Bin("age").Add(1).IntoQueryBuilder().Stream()
	if err != nil {
		return err
	}
	t.env.Printf("  Reading before the lazy stream is drained:")
	ages, err := t.session.Query(keys).Bins("age").Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  ages now:", ages); err != nil {
		return err
	}
	if err := t.printRows("  lazy write results:", lazy); err != nil {
		return err
	}
	if ages, err = t.session.Query(keys).Bins("age").Execute(); err != nil {
		return err
	}
	if err := t.printRows("  ages after the writes completed:", ages); err != nil {
		return err
	}

	t.env.Printf("  Update the same keys, but only where age < 35:")
	stream, err := t.whereWrite(t.session.Update(keys).Bin("age").Add(1), ageUnder35).Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  filtered write results:", stream); err != nil {
		return err
	}

	if stream, err = t.whereWrite(t.session.Update(keys).Bin("age").Add(1), ageUnder35).
		FailOnFilteredOut().Execute(); err != nil {
		return err
	}
	if err := t.printRows("  filtered write, filtered-out rows reported as errors:", stream); err != nil {
		return err
	}

	if ages, err = t.session.Query(keys).Bins("age").Execute(); err != nil {
		return err
	}
	return t.printRows("  final ages:", ages)
}

// ===== Point, header and projection reads ===================================

// pointAndHeaderReads runs the three read shapes — point, batch and set-wide —
// as full reads, header-only reads and bin projections.
func (t *tour) pointAndHeaderReads() error {
	t.env.Printf("--- Point, header and projection reads ---")

	// Batch results come back in the order the keys were given. Java reads 1,
	// 3, 5 and 7 here; those ids were deleted during seeding, so this port
	// picks keys that still exist. Limit caps *query* results, so a batch read
	// still answers for every key.
	batch := t.person.Keys([]int64{6, 8, 10, 12})
	stream, err := t.session.Query(batch).Limit(3).Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  batch read in key order, limit 3:", stream); err != nil {
		return err
	}

	key6 := t.person.Key(int64(6))
	if stream, err = t.session.Query(key6).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  single point record:", stream); err != nil {
		return err
	}

	t.env.Printf("  set read, limit 5, mapped to a name per row:")
	if stream, err = t.session.Query(t.person).Limit(5).Execute(); err != nil {
		return err
	}
	defer stream.Close()
	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		name := "<none>"
		if row.Record != nil {
			if v, ok := row.Record.Bins["name"]; ok {
				name = fmt.Sprintf("%v", v)
			}
		}
		t.env.Printf("    Name: %s", name)
	}

	headers := t.person.Keys([]int64{6, 7, 8})
	for _, shape := range []struct {
		label string
		build func() (*sdk.RecordStream, error)
	}{
		{"  header-only point read:", func() (*sdk.RecordStream, error) {
			return t.session.Query(key6).WithNoBins().Execute()
		}},
		{"  header-only batch read:", func() (*sdk.RecordStream, error) {
			return t.session.Query(headers).WithNoBins().Execute()
		}},
		// A set-wide query has no header-only form: the dataset path builds a
		// statement, which carries bin names but no "headers only" flag, so the
		// records arrive whole.
		{"  header-only set read, limit 4 (the set-wide path returns bins anyway):", func() (*sdk.RecordStream, error) {
			return t.session.Query(t.person).WithNoBins().Limit(4).Execute()
		}},
		{"  projection, point read:", func() (*sdk.RecordStream, error) {
			return t.session.Query(key6).Bins("name", "age").Execute()
		}},
		{"  projection, batch read:", func() (*sdk.RecordStream, error) {
			return t.session.Query(headers).Bins("name", "age").Execute()
		}},
		{"  projection, set read, limit 4:", func() (*sdk.RecordStream, error) {
			return t.session.Query(t.person).Bins("name", "age").Limit(4).Execute()
		}},

		// Java's last read here combines the two and catches the resulting
		// exception. This SDK does not refuse the combination: WithNoBins wins,
		// because a header read is the narrower request.
		{"  projection plus WithNoBins (the header read wins):", func() (*sdk.RecordStream, error) {
			return t.session.Query(key6).Bins("name", "age").WithNoBins().Execute()
		}},
	} {
		s, err := shape.build()
		if err != nil {
			return err
		}
		if err := t.printRows(shape.label, s); err != nil {
			return err
		}
	}
	return nil
}

// ===== Object lists, throttling and chunking ================================

// objectListAndChunking reads typed object lists, throttles a scan server-side,
// walks both chunked shapes, and finishes with a client-side aggregate.
func (t *tour) objectListAndChunking() error {
	t.env.Printf("--- Object lists, throttling and chunking ---")

	keys := t.person.Keys([]int64{20, 21})
	typed, err := t.session.QueryTypedKeys(t.customers, keys).Execute()
	if err != nil {
		return err
	}
	two, err := typed.IntoObjects()
	if err != nil {
		return err
	}
	t.env.Printf("  two customers by key:")
	for _, customer := range two {
		t.env.Printf("    %s", customer)
	}

	// Server-side rate limiting, kept to a few records so the tour stays quick.
	stream, err := t.session.Query(t.person).RecordsPerSecond(1).Limit(3).Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  throttled scan (1 record/second, limit 3):", stream); err != nil {
		return err
	}

	// A chunked typed read: one chunk per drain.
	chunked, err := t.session.QueryTyped(t.customers).ChunkSize(20).Execute()
	if err != nil {
		return err
	}
	chunk, err := chunked.IntoObjects()
	if err != nil {
		return err
	}
	t.env.Printf("  first chunk of 20: %d customer(s)", len(chunk))

	// A chunked raw read, where the cursor advances through HasMoreChunks.
	if stream, err = t.session.Query(t.person).ChunkSize(10).Execute(); err != nil {
		return err
	}
	defer stream.Close()
	chunks := 0
	for {
		more, err := stream.HasMoreChunks()
		if err != nil {
			return err
		}
		if !more {
			break
		}
		rows := 0
		for {
			row, err := stream.Next()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			rows++
		}
		if rows > 0 {
			chunks++
			t.env.Printf("    chunk %d: %d record(s)", chunks, rows)
		}
	}

	// A whole-set aggregate, computed client-side.
	scan, err := t.session.Query(t.person).Bins("age").Execute()
	if err != nil {
		return err
	}
	defer scan.Close()
	total := int64(0)
	for {
		row, err := scan.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		if row.Record == nil {
			continue
		}
		if age, ok := asInt64(row.Record.Bins["age"]); ok {
			total += age
		}
	}
	t.env.Printf("  sum of every age in the set: %d", total)
	return nil
}

// ===== Client-side sorting and pagination ===================================

// sortingAndPagination loads a result set once and then sorts, pages and
// re-sorts it without touching the server again.
func (t *tour) sortingAndPagination() error {
	t.env.Printf("--- Sorting and pagination ---")

	t.env.Printf("  Customers named Tim over 30, sorted by name:")
	typed, err := t.whereTyped(t.session.QueryTyped(t.customers), timOverThirty).Limit(1000).Execute()
	if err != nil {
		return err
	}
	nav, err := typed.IntoNavigatable()
	if err != nil {
		return err
	}
	if err := t.printObjects(nav.SortBy(sdk.AscIgnoreCase("name"))); err != nil {
		return err
	}

	// The same filter as a typed expression, whatever the cluster supports:
	// the client compiles it, so it works on a server without AEL.
	t.env.Printf("  The same query with a client-compiled expression filter:")
	if typed, err = t.session.QueryTyped(t.customers).
		Where(timOverThirty.expr).Limit(1000).Execute(); err != nil {
		return err
	}
	if nav, err = typed.IntoNavigatable(); err != nil {
		return err
	}
	if err := t.printObjects(nav.SortBy(sdk.AscIgnoreCase("name"))); err != nil {
		return err
	}

	if typed, err = t.session.QueryTyped(t.customers).Limit(13).Execute(); err != nil {
		return err
	}
	if nav, err = typed.IntoNavigatable(); err != nil {
		return err
	}
	nav.SortBy(sdk.Desc("age"), sdk.AscIgnoreCase("name")).PageSize(5)
	t.env.Printf("  %d records by age (desc) then name (asc, case-insensitive), 5 per page:", nav.Size())

	// HasMorePages is deliberately mutating, which is what makes this loop
	// shape work.
	page := 0
	for nav.HasMorePages() {
		page++
		t.env.Printf("    ---- page %d (%d total) ----", page, nav.MaxPages())
		if err := t.printObjects(nav); err != nil {
			return err
		}
	}

	// Jump back to a page, then re-sort the same in-memory result set.
	t.env.Printf("    ---- back to page 2 ----")
	if err := nav.SetPageTo(2); err != nil {
		return err
	}
	if err := t.printObjects(nav); err != nil {
		return err
	}

	t.env.Printf("    ---- re-sorted by name only ----")
	nav.SortBy(sdk.Asc("name"))
	page = 0
	for nav.HasMorePages() {
		page++
		t.env.Printf("    ---- page %d ----", page)
		if err := t.printObjects(nav); err != nil {
			return err
		}
	}
	return nil
}

// ===== TTL ==================================================================

// ttl writes a five-second expiration and reads the record either side of it.
func (t *tour) ttl() error {
	t.env.Printf("--- TTL ---")
	key := t.person.Key(int64(1))
	if err := drain(t.session.Delete(key).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.UpsertRows(t.person).Bins("binA").
		Row(int64(1), 5).
		ExpireRecordAfterSeconds(5).
		Execute()); err != nil {
		return err
	}

	if err := t.printRecord(key, "  initial read (should be present)"); err != nil {
		return err
	}
	time.Sleep(6 * time.Second)
	return t.printRecord(key, "  read after the TTL expired")
}

// ===== Read and write expressions ===========================================

// readWriteExpressions computes bins server-side: a read expression projects a
// virtual bin, a write expression stores one.
func (t *tour) readWriteExpressions() error {
	t.env.Printf("--- Read and write expressions ---")
	key := t.person.Key(int64(223))
	if err := drain(t.session.Replace(key).
		Bin("age").SetTo(500).
		Bin("value").SetTo(123).
		Execute()); err != nil {
		return err
	}
	if err := t.printRecord(key, "  base record"); err != nil {
		return err
	}

	stream, err := t.session.Query(key).Bin("age").Get().Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  reading one bin back:", stream); err != nil {
		return err
	}

	// Java passes AEL text to selectFrom; the per-bin expression builders take
	// a typed Expression, because only the record *filter* accepts AEL.
	sum := as.ExpNumAdd(as.ExpIntBin("age"), as.ExpIntBin("value"))
	if stream, err = t.session.Query([]*as.Key{key}).
		Bin("bob").SelectFrom(sum, true).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  read expression as a virtual bin (age + value):", stream); err != nil {
		return err
	}

	t.env.Printf("  write expressions:")
	weighted := as.ExpNumAdd(as.ExpIntBin("age"), as.ExpNumMul(as.ExpIntVal(2), as.ExpIntBin("value")))
	key1 := t.person.Key(int64(1))
	if err := drain(t.session.Upsert(key1).
		Bin("age").SetTo(50).
		Bin("value").SetTo(10).
		Bin("c2").WriteFrom(weighted, as.ExpWriteFlagDefault).
		Execute()); err != nil {
		return err
	}

	// EvalNoFail lets the write survive a record the expression cannot be
	// evaluated against, instead of failing the whole operation.
	if err := drain(t.session.Update(key).
		Bin("bob").WriteFrom(weighted, as.ExpWriteFlagEvalNoFail).
		Execute()); err != nil {
		return err
	}
	return t.printRecord(key, "  modified record")
}

// ===== Background query =====================================================

// backgroundQuery applies a numeric increment and an expression-computed bin to
// every record a filtered dataset query matches.
func (t *tour) backgroundQuery() error {
	t.env.Printf("--- Background query ---")
	derived := as.ExpNumAdd(as.ExpIntBin("age"), as.ExpIntBin("value"))
	task, err := t.whereQuery(t.session.Query(t.person), stateIsNSW).
		WithWriteOperations(
			as.AddOp(as.NewBin("age", 1)),
			as.ExpWriteOp("bob", derived, as.ExpWriteFlagEvalNoFail)).
		ExecuteBackgroundTask()
	if err != nil {
		return err
	}
	if err := <-task.OnComplete(); err != nil {
		return err
	}
	t.env.Printf("  background update complete (no record has a 'state' bin, so nothing matched)")

	// The other two background terminals inject their own operation, so they
	// must not carry one. A touch resets every match's expiration.
	touch, err := t.session.Query(t.users).ExecuteBackgroundTouch()
	if err != nil {
		return err
	}
	if err := <-touch.OnComplete(); err != nil {
		return err
	}
	t.env.Printf("  background touch over the (still empty) users set complete")
	return nil
}

// ===== Multi-operation batches ==============================================

// multiOperationBatches chains several verbs: they leave the client as a single
// batch.
func (t *tour) multiOperationBatches() error {
	t.env.Printf("--- Multi-operation batches ---")
	rowKeys := t.person.Keys([]int64{1000, 1001})
	readKeys := t.person.Keys([]int64{10, 12})
	key1003 := t.person.Key(int64(1003))
	// WithTxn(nil) opts this segment out of any ambient transaction, and the
	// chain-wide default expiration covers the segments that set none.
	//
	// Java's version of this chain also holds an existence check. This SDK
	// answers a batched Exists with NO_RESPONSE for every row in the batch, so
	// the check stays a standalone read (seeding above runs one).
	stream, err := t.session.Update(rowKeys).
		Bin("age").Add(1).
		Bin("dob").SetTo(nowMillis()).
		ExpireRecordAfter(300 * time.Second).
		Query(readKeys).
		Delete([]*as.Key{key1003}).
		WithTxn(nil).
		IntoQueryBuilder().
		DefaultExpireRecordAfter(1200 * time.Second).
		Execute()
	if err != nil {
		return err
	}
	if err := t.printRows("  update + read + delete:", stream); err != nil {
		return err
	}

	first3 := t.person.Keys([]int64{1, 2, 3})
	key1 := t.person.Key(int64(1))
	if stream, err = t.session.Query(first3).
		Bin("name").Get().
		Bin("map").OnMapKeyRange(5, 10).GetKeysAndValues().
		Update([]*as.Key{key1}).
		Bin("age").Add(1).
		Execute(); err != nil {
		return err
	}
	if err := t.printRows("  bin reads followed by a write:", stream); err != nil {
		return err
	}

	// A per-segment filter and a chain-wide default filter, together.
	doomed := idRange(t.person, 11, 15)
	winners := t.person.Keys([]int64{5, 6, 7})
	segment := t.whereWrite(t.session.Update(first3).
		Bin("age").Add(1).
		Bin("updated").SetTo(true), ageUnder21)
	segment = segment.Delete(doomed).
		Update(winners).
		Bin("luckyWinner").SetTo("true")
	if stream, err = t.defaultWhere(segment.IntoQueryBuilder(), notYetUpdated).Execute(); err != nil {
		return err
	}
	if err := t.printRows("  per-segment filter plus a chain default:", stream); err != nil {
		return err
	}

	renamed := t.person.Keys([]int64{4, 5, 6})
	key7 := t.person.Key(int64(7))
	if stream, err = t.session.Query(first3).
		Limit(2).
		Update(renamed).
		Bin("name").SetTo("bob").
		ExpireRecordAfterSeconds(500).
		Query([]*as.Key{key7}).
		Execute(); err != nil {
		return err
	}
	if err := t.printRows("  read, write and read again:", stream); err != nil {
		return err
	}

	// A plain filtered query over a different set, with string user keys.
	if err := drain(t.session.UpsertRows(t.users).Bins("status", "age").
		Row("u1", "active", 34).
		Row("u2", "active", 19).
		Row("u3", "suspended", 41).
		Execute()); err != nil {
		return err
	}
	if stream, err = t.whereQuery(t.session.Query(t.users), activeAdults).Execute(); err != nil {
		return err
	}
	return t.printRows("  active users of 21 or older:", stream)
}

// ===== Object mapping =======================================================

// objectMapping writes and reads a whole object including its nested value
// struct, then writes one nested address into a map bin. It returns the object
// it read back, which the generation check reuses.
func (t *tour) objectMapping() (*Customer, error) {
	t.env.Printf("--- Object mapping ---")
	sample := &Customer{
		CustomerID: 999,
		Name:       "sample",
		Age:        456,
		DOB:        nowMillis(),
		Address: &Address{
			Line1: "123 Main St", City: "Denver", State: "CO", Country: "USA", ZipCode: "80112",
		},
	}
	t.env.Printf("  reference customer: %s", sample)

	key999 := t.person.Key(int64(999))
	if err := drain(t.session.Delete(key999).Execute()); err != nil {
		return nil, err
	}
	if err := drain(t.session.InsertTyped(t.customers).Object(sample).Execute()); err != nil {
		return nil, err
	}

	typed, err := t.session.QueryTypedKeys(t.customers, []*as.Key{key999}).Execute()
	if err != nil {
		return nil, err
	}
	readBack, err := typed.FirstObject()
	if err != nil {
		return nil, err
	}
	t.env.Printf("  customer read back: %s", readBack)

	// Java expects bin operations on a set-wide query to be rejected; this SDK
	// sends them as an operation projection, which servers from 8.1.2 accept
	// for reads. No record holds a `fred` bin, and the server drops a record
	// whose projection produced nothing, so the answer is empty rather than a
	// row per match.
	stream, err := t.whereQuery(t.session.Query(t.person), nameIsTim).
		Bin("fred").Get().Limit(2).Execute()
	if err != nil {
		t.env.Printf("  bin projection on a set-wide query rejected: %s", errSummary(err))
	} else {
		rows, err := stream.Collect()
		if err != nil {
			return nil, err
		}
		t.env.Printf("  bin projection on a set-wide query returned %d row(s)", len(rows))
	}

	// Nested mapping: write a customer with no address, then add one address
	// into a map bin under the key "home".
	if err := drain(t.session.UpsertTyped(t.customers).Object(newCustomer(1, "Bob", 37)).Execute()); err != nil {
		return nil, err
	}
	home := &Address{
		Line1: "123 Main St", City: "Denver", State: "CO", Country: "USA", ZipCode: "80000",
	}
	keyOrdered := as.MapOrder.KEY_ORDERED
	key1 := t.person.Key(int64(1))
	if err := drain(t.session.Upsert(key1).
		Bin("addrs").OnMapKey("home", &keyOrdered).SetTo(home.AsMap()).
		Execute()); err != nil {
		return nil, err
	}
	if err := t.printRecord(key1, "  customer 1 with addrs"); err != nil {
		return nil, err
	}
	return readBack, nil
}

// ===== Generation check =====================================================

// generationCheck is optimistic locking: the first write at the generation that
// was read succeeds, and the second one, at a generation that has moved on,
// must fail.
func (t *tour) generationCheck(readBack *Customer) error {
	t.env.Printf("--- Generation check ---")
	key := t.person.Key(int64(999))
	stream, err := t.session.Query(key).Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	record, err := row.RecordOrRaise()
	if err != nil {
		return err
	}
	generation := record.Generation
	t.env.Printf("  read record with generation %d", generation)

	if err := drain(t.session.Update(key).
		Bin("gen").SetTo(int64(generation)).
		EnsureGenerationIs(generation).
		Execute()); err != nil {
		return err
	}
	t.env.Printf("  first update succeeded")

	// A typed write is a batch of one, so its failure rides on the row rather
	// than coming back from the terminal: FirstOrRaise is what lifts it out.
	second, err := t.session.UpdateTyped(t.customers).
		Object(readBack).
		EnsureGenerationIs(generation).
		Execute()
	if err == nil {
		defer second.Close()
		_, err = second.FirstOrRaise()
	}
	if err == nil {
		t.env.Printf("  second update succeeded — this is an error")
		return nil
	}
	t.env.Printf("  second update failed as expected: %s", errSummary(err))
	return nil
}

// ===== Complex CDT ==========================================================

// complexCDT reads and writes lists and maps at the top level and through
// nested CDT paths, ending with one call that touches three bins.
func (t *tour) complexCDT() error {
	t.env.Printf("--- Complex CDT operations ---")
	key := t.person.Key(int64(500))
	if err := drain(t.session.Delete(key).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Upsert(key).
		Bin("name").SetTo("CDT-Test").
		Bin("scores").SetTo([]any{95, 82, 73, 88, 91}).
		Bin("tags").SetTo([]any{"java", "python", "rust"}).
		Bin("inventory").SetTo(map[any]any{"apples": 10, "bananas": 5, "cherries": 20}).
		Bin("nested").SetTo(nestedTeams()).
		Execute()); err != nil {
		return err
	}

	// Read-only operations at the top level.
	for _, probe := range []struct {
		label string
		build func(*sdk.QueryBuilder) *sdk.QueryBuilder
	}{
		{"list size of 'scores'", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("scores").ListSize()
		}},
		{"map size of 'inventory'", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("inventory").MapSize()
		}},
		{"first score", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("scores").ListGet(0)
		}},
		{"scores [1..4)", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("scores").ListGetRange(1, 3)
		}},

		// The builders take an explicit count, so an open-ended range reaches
		// for the core operation directly.
		{"scores from index 3 on", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.AddOperation(as.ListGetRangeFromOp("scores", 3))
		}},

		// Read-only operations through a CDT path.
		{"team1 member count", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListSize()
		}},
		{"team1 second member", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListGetRange(1, 1)
		}},
		{"team2 members [0..2)", func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team2", nil).OnMapKey("members", nil).ListGetRange(0, 2)
		}},
	} {
		if err := t.printOp(key, probe.label, probe.build); err != nil {
			return err
		}
	}

	// List mutations.
	for _, mutation := range []struct {
		bin, label string
		build      func(*sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder
	}{
		{"scores", "ListAppendItems([77, 65, 99])", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListAppendItems([]any{77, 65, 99})
		}},
		{"tags", "ListInsert(1, 'go')", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("tags").ListInsert(1, "go")
		}},
		{"tags", "ListSet(0, 'kotlin')", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("tags").ListSet(0, "kotlin")
		}},
		{"scores", "ListInsertItems(2, [100, 200])", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListInsertItems(2, []any{100, 200})
		}},
		{"scores", "ListIncrement(0, 5)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListIncrement(0, 5)
		}},
		{"scores", "ListSort()", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListSort(as.ListSortFlagsDefault)
		}},
		{"scores", "ListRemove(0)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListRemove(0)
		}},
		{"scores", "ListRemoveRange(4, 2)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListRemoveRange(4, 2)
		}},
	} {
		if err := t.writeAndShow(key, mutation.bin, mutation.label, mutation.build); err != nil {
			return err
		}
	}

	stream, err := t.session.Update(key).Bin("scores").ListPop(0).Execute()
	if err != nil {
		return err
	}
	popped, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  popped element: %s", operationResults(popped))

	for _, mutation := range []struct {
		bin, label string
		build      func(*sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder
	}{
		{"scores", "ListTrim(0, 3)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("scores").ListTrim(0, 3)
		}},
		{"tags", "ListClear()", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("tags").ListClear()
		}},

		// Map mutations.
		{"inventory", "MapPutItems(...)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("inventory").MapPutItems(as.DefaultMapPolicy(),
				map[any]any{"dates": 15, "elderberries": 8})
		}},
		{"inventory", "MapSetPolicy(KEY_ORDERED)", func(w *sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder {
			return w.Bin("inventory").MapSetPolicy(
				as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE))
		}},
	} {
		if err := t.writeAndShow(key, mutation.bin, mutation.label, mutation.build); err != nil {
			return err
		}
	}

	if err := t.nestedCDTWrites(key); err != nil {
		return err
	}

	// One call, three bins, mixed reads and writes.
	if stream, err = t.session.Update(key).
		Bin("scores").ListAppendItems([]any{50, 60, 70}).
		Bin("inventory").MapPutItems(as.DefaultMapPolicy(), map[any]any{"figs": 12}).
		Bin("nested").OnMapKey("team2", nil).OnMapKey("members", nil).ListAppend("Quinn").
		Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListSize().
		Execute(); err != nil {
		return err
	}
	combined, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  combined CDT result: %s", operationResults(combined))
	if err := t.printRecord(key, "  final state"); err != nil {
		return err
	}

	// A key range open at the top end, where infinity is the sentinel.
	if err := drain(t.session.Update(key).
		Bin("test").SetTo(map[any]any{1: "a", 5: "b", 9: "c"}).
		Execute()); err != nil {
		return err
	}
	if stream, err = t.session.Query(key).
		Bin("test").OnMapKeyRange(5, as.NewInfinityValue()).GetKeys().
		Execute(); err != nil {
		return err
	}
	unbounded, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  map keys from 5 to +infinity: %s", operationResults(unbounded))
	return nil
}

// nestedCDTWrites writes through CDT paths, creating the intermediate map
// entries on the way.
func (t *tour) nestedCDTWrites(key *as.Key) error {
	if err := drain(t.session.Update(key).
		Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListAppend("Diana").
		Execute()); err != nil {
		return err
	}
	if err := t.printOp(key, "team1 size after appending Diana",
		func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListSize()
		}); err != nil {
		return err
	}

	// A nested list has no Insert on the navigated builder, so the core
	// operation carries the path as a context instead.
	teamCtx := func(team string) []*as.CDTContext {
		return []*as.CDTContext{
			as.CtxMapKey(as.NewStringValue(team)),
			as.CtxMapKey(as.NewStringValue("members")),
		}
	}
	if err := drain(t.session.Update(key).
		AddOperation(as.ListInsertWithPolicyContextOp(
			as.DefaultListPolicy(), "nested", 0, teamCtx("team2"), "Zara")).
		Execute()); err != nil {
		return err
	}
	if err := t.printOp(key, "team2 after inserting Zara",
		func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team2", nil).OnMapKey("members", nil).ListGetRange(0, 100)
		}); err != nil {
		return err
	}

	if err := drain(t.session.Update(key).
		AddOperation(as.ListSortOp("nested", as.ListSortFlagsDefault, teamCtx("team1")...)).
		Execute()); err != nil {
		return err
	}
	if err := t.printOp(key, "team1 sorted",
		func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team1", nil).OnMapKey("members", nil).ListGetRange(0, 100)
		}); err != nil {
		return err
	}

	// An ordered list at a path that does not exist yet: the create-order
	// contexts build the intermediate maps, and ListCreate fixes the ordering
	// before anything is appended.
	createCtx := []*as.CDTContext{
		as.CtxMapKeyCreate(as.NewStringValue("team3"), as.MapOrder.KEY_ORDERED),
		as.CtxMapKeyCreate(as.NewStringValue("members"), as.MapOrder.KEY_ORDERED),
	}
	if err := drain(t.session.Update(key).
		AddOperation(as.ListCreateOp("nested", as.ListOrderOrdered, false, createCtx...)).
		Execute()); err != nil {
		return err
	}
	appendCtx := teamCtx("team3")
	if err := drain(t.session.Update(key).
		AddOperation(as.ListAppendWithPolicyContextOp(
			as.DefaultListPolicy(), "nested", appendCtx, "Ivy", "Frank", "Grace")).
		Execute()); err != nil {
		return err
	}
	return t.printOp(key, "team3 members (ordered)",
		func(q *sdk.QueryBuilder) *sdk.QueryBuilder {
			return q.Bin("nested").OnMapKey("team3", nil).OnMapKey("members", nil).ListGetRange(0, 100)
		})
}

// ===== Bitwise operations ===================================================

// bitOperations exercises the bit (blob) family on one small bin.
func (t *tour) bitOperations() error {
	t.env.Printf("--- Bit (BLOB) operations ---")

	// The bit operations take a policy rather than defaulting one, and the
	// operation encoder reads it unconditionally, so it has to be supplied.
	bitPolicy := as.DefaultBitPolicy()

	key := t.person.Key(int64(501))
	if err := drain(t.session.Delete(key).Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Upsert(key).
		Bin("flags").SetTo([]byte{0x01, 0x42}).
		Execute()); err != nil {
		return err
	}

	if err := drain(t.session.Update(key).
		Bin("flags").BitResize(bitPolicy, 4, as.BitResizeFlagsDefault).
		Bin("flags").BitSet(bitPolicy, 8, 8, []byte{0xFF}).
		Bin("flags").BitOr(bitPolicy, 0, 16, []byte{0x0F, 0xF0}).
		Execute()); err != nil {
		return err
	}

	stream, err := t.session.Query(key).
		Bin("flags").BitGet(0, 8).
		Bin("flags").BitCount(0, 32).
		Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  first byte + set-bit count: %s", operationResults(row))

	if stream, err = t.session.Query(key).Bin("flags").BitGetInt(0, 16, false).Execute(); err != nil {
		return err
	}
	if row, err = stream.FirstOrRaise(); err != nil {
		return err
	}
	t.env.Printf("  uint16 at bit 0: %s", operationResults(row))

	if stream, err = t.session.Query(key).
		Bin("flags").BitLScan(0, 32, true).
		Bin("flags").BitRScan(0, 32, true).
		Execute(); err != nil {
		return err
	}
	if row, err = stream.FirstOrRaise(); err != nil {
		return err
	}
	t.env.Printf("  first/last set bit: %s", operationResults(row))

	// Wrap lets the addition roll over instead of failing the operation.
	if err := drain(t.session.Update(key).
		Bin("flags").BitSetInt(bitPolicy, 16, 16, 100).
		Bin("flags").BitAdd(bitPolicy, 16, 16, 1, false, as.BitOverflowActionWrap).
		Execute()); err != nil {
		return err
	}
	if stream, err = t.session.Query(key).Bin("flags").BitGetInt(16, 16, false).Execute(); err != nil {
		return err
	}
	if row, err = stream.FirstOrRaise(); err != nil {
		return err
	}
	t.env.Printf("  after BitSetInt/BitAdd: %s", operationResults(row))

	if err := drain(t.session.Update(key).
		Bin("flags").BitLShift(bitPolicy, 0, 8, 1).
		Bin("flags").BitNot(bitPolicy, 8, 8).
		Execute()); err != nil {
		return err
	}
	if err := drain(t.session.Update(key).
		Bin("flags").BitInsert(bitPolicy, 1, []byte{0x11, 0x22}).
		Bin("flags").BitRemove(bitPolicy, 3, 1).
		Execute()); err != nil {
		return err
	}
	return t.printRecord(key, "  final flags blob")
}

// ===== Heterogeneous batch ==================================================

// heterogeneousBatch reads one batch spanning two sets and two entity types,
// and maps each row by the set it came from.
func (t *tour) heterogeneousBatch() error {
	t.env.Printf("--- Heterogeneous batch ---")
	addressKey := t.addresses.Key(int64(1))
	if err := drain(t.session.Upsert(addressKey).
		Bin("line1").SetTo("123 Main St").
		Bin("city").SetTo("Denver").
		Bin("state").SetTo("CO").
		Bin("country").SetTo("USA").
		Bin("zip").SetTo("80000").
		Execute()); err != nil {
		return err
	}

	people := t.person.Keys([]int64{21, 22, 23})
	stream, err := t.session.Query(people).Query([]*as.Key{addressKey}).Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		if !row.IsOK() {
			t.env.Printf("  %s -> %s", exrun.KeyString(row.Key), row.ResultCode)
			continue
		}
		record, err := row.RecordOrRaise()
		if err != nil {
			return err
		}
		if row.Key.SetName() == t.person.SetName() {
			customer := &Customer{}
			if err := customer.SetFromRecord(record.Bins, row.Key, record.Generation); err != nil {
				return err
			}
			t.env.Printf("  customer: %s", customer)
			continue
		}
		// The address set holds value-shaped records, so its bins *are* the
		// address map.
		t.env.Printf("  address:  %s", addressFrom(map[any]any(binsAsMap(record.Bins))))
	}
	return nil
}

// ===== Typed stream =========================================================

// typedStream is Java's CompletableFuture mapping demo. Go's concurrency lives
// in goroutines rather than in the call shape, so the ordinary typed path is
// what that example was reaching for: rows arrive as data, and a batch of
// partly-missing keys maps row by row.
func (t *tour) typedStream() error {
	t.env.Printf("--- Typed stream ---")
	keys := t.person.Keys([]int64{1, 2, 3})
	stream, err := t.session.QueryTypedKeys(t.customers, keys).IncludeMissingKeys().Execute()
	if err != nil {
		return err
	}
	defer stream.Close()
	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		if !row.IsOK() {
			t.env.Printf("  key %s -> %s", exrun.KeyString(row.Key), row.ResultCode)
			continue
		}
		customer := &Customer{}
		if err := customer.SetFromRecord(row.Record.Bins, row.Key, row.Record.Generation); err != nil {
			return err
		}
		t.env.Printf("  %s", customer)
	}

	key1 := t.person.Key(int64(1))
	point, err := t.session.QueryTypedKeys(t.customers, []*as.Key{key1}).Execute()
	if err != nil {
		return err
	}
	customer, err := point.FirstObject()
	if err != nil {
		return err
	}
	if customer == nil {
		t.env.Printf("  customer id=1 not found")
		return nil
	}
	t.env.Printf("  single customer: %s", customer)
	return nil
}

// ===== Filter plumbing ======================================================

// whereQuery applies a predicate to a read in whichever form the cluster takes.
func (t *tour) whereQuery(q *sdk.QueryBuilder, p predicate) *sdk.QueryBuilder {
	if t.ael {
		return q.Where(p.ael)
	}
	return q.Where(p.expr)
}

// whereWrite is [tour.whereQuery] for a write segment.
func (t *tour) whereWrite(b *sdk.WriteSegmentBuilder, p predicate) *sdk.WriteSegmentBuilder {
	if t.ael {
		return b.Where(p.ael)
	}
	return b.Where(p.expr)
}

// whereTyped is [tour.whereQuery] for a typed read.
func (t *tour) whereTyped(q *sdk.TypedQueryBuilder[Customer], p predicate) *sdk.TypedQueryBuilder[Customer] {
	if t.ael {
		return q.Where(p.ael)
	}
	return q.Where(p.expr)
}

// defaultWhere applies a predicate as a chain-wide default, which every segment
// without its own filter inherits.
func (t *tour) defaultWhere(q *sdk.QueryBuilder, p predicate) *sdk.QueryBuilder {
	if t.ael {
		return q.DefaultWhere(p.ael)
	}
	return q.DefaultWhere(p.expr)
}

// ===== Output helpers =======================================================

// printRows prints every row of a stream, numbered like the Java print helper.
func (t *tour) printRows(label string, stream *sdk.RecordStream) error {
	defer stream.Close()
	t.env.Printf("%s", label)
	count := 0
	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		count++
		t.env.Printf("    %4d - key %s: %s", count, exrun.KeyString(row.Key), describe(row))
	}
	if count == 0 {
		t.env.Printf("    (no rows)")
	}
	return nil
}

// printRecord reads one record and prints it, or reports that it is gone.
func (t *tour) printRecord(key *as.Key, label string) error {
	stream, err := t.session.Query(key).Execute()
	if err != nil {
		return err
	}
	defer stream.Close()
	row, err := stream.Next()
	if err != nil {
		return err
	}
	if row == nil {
		t.env.Printf("%s: <not found>", label)
		return nil
	}
	t.env.Printf("%s: %s", label, describe(row))
	return nil
}

// printOp runs one read operation on a single key and prints what it produced.
func (t *tour) printOp(key *as.Key, label string, build func(*sdk.QueryBuilder) *sdk.QueryBuilder) error {
	stream, err := build(t.session.Query(key)).Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  %s: %s", label, operationResults(row))
	return nil
}

// writeAndShow applies one CDT write to a single key, then prints the bin it
// touched.
func (t *tour) writeAndShow(key *as.Key, bin, label string, build func(*sdk.WriteSegmentBuilder) *sdk.WriteSegmentBuilder) error {
	if err := drain(build(t.session.Update(key)).Execute()); err != nil {
		return err
	}
	stream, err := t.session.Query(key).Bins(bin).Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	t.env.Printf("  after %s: %s", label, describe(row))
	return nil
}

// printObjects prints the navigatable stream's current page.
func (t *tour) printObjects(nav *sdk.TypedNavigatableRecordStream[Customer]) error {
	objects, err := nav.Objects()
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		t.env.Printf("      (none)")
		return nil
	}
	for _, customer := range objects {
		t.env.Printf("      %s", customer)
	}
	return nil
}

// customerAge reports one customer's age bin, rendered for the console.
func (t *tour) customerAge(id int64) (string, error) {
	key := t.person.Key(id)
	stream, err := t.session.Query(key).Bins("age").Execute()
	if err != nil {
		return "", err
	}
	defer stream.Close()
	row, err := stream.Next()
	if err != nil {
		return "", err
	}
	if row == nil || row.Record == nil {
		return "<not found>", nil
	}
	age, ok := asInt64(row.Record.Bins["age"])
	if !ok {
		return "<no age>", nil
	}
	return fmt.Sprintf("%d", age), nil
}

// errSummary names an error by kind and result code.
//
// The raw message is not what a caller wants to read: the core error chains
// every attempt it made, including the connection-pool notices from cluster
// warm-up, so one KEY_NOT_FOUND can render as a hundred lines.
func errSummary(err error) string {
	var e *sdk.Error
	if !errors.As(err, &e) {
		return firstLine(err.Error())
	}
	if rc, ok := e.ResultCode(); ok {
		return fmt.Sprintf("%s / %s", e.Kind(), rc)
	}
	return fmt.Sprintf("%s: %s", e.Kind(), firstLine(e.Message()))
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// describe renders one row: bins sorted for stable output, with header-only and
// failed rows called out.
func describe(row *sdk.RecordResult) string {
	if row.Record == nil {
		return fmt.Sprintf("<%s>", row.ResultCode)
	}
	if len(row.Record.Bins) == 0 {
		return fmt.Sprintf("<no bins, generation %d>", row.Record.Generation)
	}
	return exrun.BinsString(row.Record.Bins)
}

// operationResults renders a row positionally, which is the only way to read a
// chain that put several operations on one bin: the bin map keeps just the last
// value.
func operationResults(row *sdk.RecordResult) string {
	values := row.OperationResults()
	if len(values) == 0 {
		return describe(row)
	}
	parts := make([]string, 0, len(values))
	for i, v := range values {
		parts = append(parts, fmt.Sprintf("[%d]=%s", i, exrun.Render(v)))
	}
	return strings.Join(parts, " ")
}

// ===== Value helpers ========================================================

// drain executes a chain's outcome and discards its rows, so a write that only
// needs to have happened reads as one line.
func drain(stream *sdk.RecordStream, err error) error {
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.Collect()
	return err
}

// idRange builds the keys for an inclusive range of integer ids.
func idRange(ds *sdk.DataSet, from, to int64) []*as.Key {
	ids := make([]int64, 0, to-from+1)
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}
	return ds.Keys(ids)
}

// sortedNames orders a bin map's names, so a generated chain sends its
// operations in the same order every run.
func sortedNames(bins as.BinMap) []string {
	names := make([]string, 0, len(bins))
	for name := range bins {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func nowMillis() int64 { return time.Now().UnixMilli() }

// asInt64 widens whichever integer width the server sent.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	default:
		return 0, false
	}
}

// binsAsMap re-keys a bin map as a value map, which is the shape a nested map
// bin arrives in.
func binsAsMap(bins as.BinMap) map[any]any {
	out := make(map[any]any, len(bins))
	for name, value := range bins {
		out[name] = value
	}
	return out
}

// addressFrom maps a map bin back to an [Address], tolerating both shapes the
// server may return a map in.
func addressFrom(v any) *Address {
	var entries map[any]any
	switch m := v.(type) {
	case map[any]any:
		entries = m
	case []as.MapPair:
		entries = make(map[any]any, len(m))
		for _, pair := range m {
			entries[pair.Key] = pair.Value
		}
	default:
		return nil
	}
	str := func(key string) string {
		s, _ := entries[key].(string)
		return s
	}
	return &Address{
		Line1:   str("line1"),
		City:    str("city"),
		State:   str("state"),
		Country: str("country"),
		ZipCode: str("zip"),
	}
}

// roomsMap is `{ "roomN": { "occupied": bool, "rates": { 1: .., 2: .., 3: .. } } }`.
func roomsMap() map[any]any {
	rooms := []struct {
		name     string
		occupied bool
		rates    [3]int
	}{
		{"room1", false, [3]int{100, 150, -1}},
		{"room2", true, [3]int{90, -1, -1}},
		{"room3", false, [3]int{67, 200, 99}},
		{"room4", true, [3]int{98, -1, -1}},
		{"room5", false, [3]int{98, -1, -1}},
		{"room6", true, [3]int{98, -1, -1}},
	}
	out := make(map[any]any, len(rooms))
	for _, room := range rooms {
		rates := make(map[any]any, len(room.rates))
		for i, rate := range room.rates {
			rates[i+1] = rate
		}
		out[room.name] = map[any]any{"occupied": room.occupied, "rates": rates}
	}
	return out
}

// nestedTeams is `{ "team1": { "members": [...] }, "team2": { "members": [...] } }`.
func nestedTeams() map[any]any {
	return map[any]any{
		"team1": map[any]any{"members": []any{"Alice", "Bob", "Charlie"}},
		"team2": map[any]any{"members": []any{"Dave", "Eve"}},
	}
}

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

// This example walks the SDK's main surfaces against a live cluster: the fast
// path, the builder chain, batches, row writes, a dataset query with
// client-side sorting, the typed layer, and info.
//
//	go1.27rc2 run ./sdk/examples/basic -h 127.0.0.1 -p 3000 -n test
package main

import (
	"flag"
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Customer is mapped by reflection. The key field never becomes a bin.
type Customer struct {
	ID   int64  `as:",key"`
	Name string `as:"name"`
	Age  int64  `as:"age"`
}

func main() {
	host := flag.String("h", "127.0.0.1", "seed host")
	port := flag.Int("p", 3000, "seed port")
	namespace := flag.String("n", "test", "namespace")
	alternate := flag.Bool("use-services-alternate", false, "use alternate addresses")
	flag.Parse()

	def := sdk.NewClusterDefinition(*host, *port)
	if *alternate {
		def = def.UsingServicesAlternate()
	}
	cluster, err := def.Connect()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer cluster.Close()

	session, err := cluster.CreateSession(nil)
	if err != nil {
		log.Fatalf("create session: %v", err)
	}

	fmt.Printf("cluster supports: transactions=%v cdt-paths=%v string-ops=%v ael=%v\n",
		cluster.SupportsMRT(), cluster.SupportsCDTPathExpressions(),
		cluster.SupportsStringOperations(), cluster.SupportsServerCompiledAEL())

	users, err := sdk.DataSetOf(*namespace, "sdk_example_users")
	if err != nil {
		log.Fatalf("dataset: %v", err)
	}

	// --- Fast path ---
	key := users.Key("user-1")
	if err := session.Put(key, as.BinMap{"name": "Ada", "age": 36}); err != nil {
		log.Fatalf("put: %v", err)
	}
	rec, err := session.Get(key, sdk.AllBins)
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("fast path: name=%v age=%v generation=%d\n",
		rec.Bins["name"], rec.Bins["age"], rec.Generation)

	// --- Builder chain: several operations on one bin ---
	counter := users.Key("counter")
	stream, err := session.Upsert(counter).
		Bin("hits").SetTo(100).
		Bin("hits").Add(11).
		Bin("hits").Get().
		Execute()
	if err != nil {
		log.Fatalf("operate: %v", err)
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		log.Fatalf("first: %v", err)
	}
	// Only the Get produces a value, so there is one positional result.
	if v, ok := row.OperationResult(0); ok {
		fmt.Printf("builder chain: hits=%v\n", v)
	}

	// --- Batch: the same verb takes one key or many ---
	keys := users.Keys([]int64{1, 2, 3, 4, 5})
	stream, err = session.Upsert(keys).SetTo("age", 20).Execute()
	if err != nil {
		log.Fatalf("batch upsert: %v", err)
	}
	rows, err := stream.Collect()
	if err != nil {
		log.Fatalf("collect: %v", err)
	}
	fmt.Printf("batch: wrote %d records\n", len(rows))

	// --- Row-oriented writes ---
	stream, err = session.UpsertRows(users).
		Bins("name", "age").
		Row(int64(100), "Alice", 34).
		Row(int64(101), "Bob", 41).
		Execute()
	if err != nil {
		log.Fatalf("row writes: %v", err)
	}
	rows, _ = stream.Collect()
	fmt.Printf("row writes: wrote %d records\n", len(rows))

	// --- Dataset query with client-side sorting and pagination ---
	stream, err = session.Query(users).Execute()
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	nav, err := stream.IntoNavigatable()
	if err != nil {
		log.Fatalf("navigatable: %v", err)
	}
	nav.SortBy(sdk.Desc("age")).PageSize(3)
	fmt.Printf("query: %d records over %d pages\n", nav.Size(), nav.MaxPages())
	page := 0
	for nav.HasMorePages() {
		page++
		count := 0
		for nav.HasNext() {
			nav.Next()
			count++
		}
		fmt.Printf("  page %d: %d records\n", page, count)
	}

	// --- Typed layer ---
	customers, err := sdk.TypedDataSetOf[Customer](*namespace, "sdk_example_customers")
	if err != nil {
		log.Fatalf("typed dataset: %v", err)
	}
	if _, err := session.UpsertTyped(customers).
		Object(&Customer{ID: 1, Name: "Ada", Age: 36}).
		Object(&Customer{ID: 2, Name: "Grace", Age: 45}).
		Execute(); err != nil {
		log.Fatalf("typed write: %v", err)
	}
	typed, err := session.QueryTyped(customers).Execute()
	if err != nil {
		log.Fatalf("typed query: %v", err)
	}
	objs, err := typed.IntoObjects()
	if err != nil {
		log.Fatalf("typed read: %v", err)
	}
	for _, c := range objs {
		fmt.Printf("typed: id=%d name=%s age=%d\n", c.ID, c.Name, c.Age)
	}

	// --- Info ---
	detail, err := session.InfoCommands().NamespaceDetail(*namespace)
	if err != nil {
		log.Fatalf("info: %v", err)
	}
	if detail != nil {
		objects, _ := detail.Objects()
		sc, _ := detail.StrongConsistency()
		fmt.Printf("info: namespace %s holds %d objects, strong-consistency=%v\n",
			*namespace, objects, sc)
	}

	// Clean up the sets this example created.
	if err := session.Truncate(users, 0); err != nil {
		log.Printf("truncate users: %v", err)
	}
	if err := session.Truncate(customers.DataSet(), 0); err != nil {
		log.Printf("truncate customers: %v", err)
	}
	fmt.Println("done")
}

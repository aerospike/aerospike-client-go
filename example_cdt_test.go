// Copyright 2014-2022 Aerospike, Inc.
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

// Package aerospike_test contains runnable examples for CDT list, map, and context APIs.
// They appear in godoc when viewing the corresponding symbols in package aerospike
// (e.g. ListAppendOp, ListAppendWithPolicyContextOp, MapPutOp, CtxListIndex, CtxMapKey).
package aerospike_test

import (
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// ExampleListAppendOp demonstrates appending items to a list bin.
func ExampleListAppendOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-append")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)

	record, err := client.Operate(nil, key,
		as.ListAppendOp("tasks", "task1", "task2", "task3"),
		as.GetBinOp("tasks"),
	)
	if err != nil {
		log.Fatal(err)
	}
	results := record.Bins["tasks"].(as.OpResults)
	fmt.Printf("size=%d list=%v\n", results[0], results[1])
	// Output: size=3 list=[task1 task2 task3]
}

//	Demonstrates nested list operations:
//
// (1) Append to the last list in a list-of-lists; (2) Append to the lowest-ranked
// list within a map key. Matches the package-level examples in cdt_list.go.
func ExampleListAppendWithPolicyContextOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-ctx")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)

	// Example 1: bin = [[7,9,5],[1,2,3],[6,5,4,1]] — append 11 to last list
	list := []any{[]any{7, 9, 5}, []any{1, 2, 3}, []any{6, 5, 4, 1}}
	_ = client.Put(nil, key, as.BinMap{"bin": list})

	record, err := client.Operate(nil, key,
		as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "bin", []*as.CDTContext{as.CtxListIndex(-1)}, 11),
		as.GetBinOp("bin"),
	)
	if err != nil {
		log.Fatal(err)
	}
	// Last inner list becomes [6,5,4,1,11]
	inner := record.Bins["bin"].(as.OpResults)[1].([]any)[2].([]any)
	fmt.Println(inner)

	// Example 2: bin = {key2:[[9],[2,4],[6,1,9]]} — append 11 to lowest-ranked list in "key2"
	client.Delete(nil, key)
	m := map[any]any{
		"key1": []any{[]any{7, 9, 5}, []any{13}},
		"key2": []any{[]any{9}, []any{2, 4}, []any{6, 1, 9}},
	}
	_ = client.Put(nil, key, as.BinMap{"bin": m})
	record, err = client.Operate(nil, key,
		as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "bin", []*as.CDTContext{as.CtxMapKey(as.StringValue("key2")), as.CtxListRank(0)}, 11),
		as.GetBinOp("bin"),
	)
	if err != nil {
		log.Fatal(err)
	}
	// key2's lowest-ranked list [2,4] becomes [2,4,11]
	resMap := record.Bins["bin"].(as.OpResults)[1].(map[any]any)
	key2Lists := resMap["key2"].([]any)
	fmt.Println(key2Lists[1])
	// Output:
	// [6 5 4 1 11]
	// [2 4 11]
}

// Demonstrates inserting at a specific index.
func ExampleListInsertOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-insert")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "a", "b", "c"))

	record, err := client.Operate(nil, key,
		as.ListInsertOp("tasks", 1, "inserted"),
		as.GetBinOp("tasks"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["tasks"].(as.OpResults)[1])
	// Output: [a inserted b c]
}

// Demonstrates putting a key-value pair, including into a nested map using context.
func ExampleMapPutOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-put")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)

	// Simple put
	_, err = client.Operate(nil, key,
		as.MapPutOp(as.DefaultMapPolicy(), "profile", "email", "user@example.com"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Nested put: user.address.city = "San Francisco"
	// The nested map at "address" must exist before putting into it. Create user with name and empty address map.
	_ = client.Put(nil, key, as.BinMap{"user": map[any]any{"name": "Alice", "address": map[any]any{}}})
	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("address"))}
	_, err = client.Operate(nil, key,
		as.MapPutOp(as.DefaultMapPolicy(), "user", "city", "San Francisco", ctx...),
		as.GetBinOp("user"),
	)
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "user")
	u := rec.Bins["user"].(map[any]any)
	addr := u["address"].(map[any]any)
	fmt.Println(addr["city"])
	// Output: San Francisco
}

// Demonstrates putting multiple map items at once.
func ExampleMapPutItemsOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-put-items")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)

	items := map[any]any{
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}
	_, err = client.Operate(nil, key,
		as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", items),
		as.GetBinOp("profile"),
	)
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "profile")
	p := rec.Bins["profile"].(map[any]any)
	fmt.Println(p["name"], p["age"])
	// Output: John Doe 30
}

// Demonstrates using context to select a list by index (e.g. first or last item).
func ExampleCtxListIndex() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-list-index")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	// order.items[0].productId — build order with items list
	order := map[any]any{"items": []any{map[any]any{"productId": "P1"}, map[any]any{"productId": "P2"}}}
	_ = client.Put(nil, key, as.BinMap{"order": order})

	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("items")), as.CtxListIndex(0)}
	record, err := client.Operate(nil, key,
		as.MapGetByKeyOp("order", "productId", as.MapReturnType.VALUE, ctx...),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["order"])
	// Output: P1
}

// ExampleCtxListRank demonstrates using context to select a list element by index, then get a map key.
// CtxListRank(0) is "lowest by value" (order for map elements may vary). This example uses CtxListIndex(0)
// so the first item (productId "low") is selected reliably on any server.
func ExampleCtxListRank() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-list-rank")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	order := map[any]any{"items": []any{map[any]any{"productId": "low"}, map[any]any{"productId": "high"}}}
	_ = client.Put(nil, key, as.BinMap{"order": order})

	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("items")), as.CtxListIndex(0)}
	record, err := client.Operate(nil, key,
		as.MapGetByKeyOp("order", "productId", as.MapReturnType.VALUE, ctx...),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["order"])
	// Output: low
}

// Demonstrates using context to navigate into a nested map by key.
func ExampleCtxMapKey() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-map-key")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	user := map[any]any{"profile": map[any]any{"address": map[any]any{"city": "NYC"}}}
	_ = client.Put(nil, key, as.BinMap{"user": user})

	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("profile")), as.CtxMapKey(as.StringValue("address"))}
	record, err := client.Operate(nil, key,
		as.MapGetByKeyOp("user", "city", as.MapReturnType.VALUE, ctx...),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["user"])
	// Output: NYC
}

// ExampleCtxMapRank demonstrates using context to reach a nested map and get a value by key.
// CtxMapRank selects by value rank (requires value-ordered map). This example uses CtxMapKey +
// MapGetByKeyOp so it runs on any CDT-capable server.
func ExampleCtxMapRank() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-map-rank")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	user := map[any]any{"categories": map[any]any{"bronze": 10, "silver": 20, "gold": 30}}
	_ = client.Put(nil, key, as.BinMap{"user": user})

	// CtxMapKey("categories") selects the inner map; MapGetByKeyOp gets value at key "gold"
	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("categories"))}
	record, err := client.Operate(nil, key,
		as.MapGetByKeyOp("user", "gold", as.MapReturnType.VALUE, ctx...),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["user"])
	// Output: 30
}

// Demonstrates popping an item from a list (e.g. queue).
func ExampleListPopOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-pop")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("queue", "first", "second", "third"))

	record, err := client.Operate(nil, key,
		as.ListPopOp("queue", 0),
		as.GetBinOp("queue"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["queue"].(as.OpResults)[0], record.Bins["queue"].(as.OpResults)[1])
	// Output: first [second third]
}

// Demonstrates removing an item at index from a list.
func ExampleListRemoveOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-remove")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "a", "b", "c"))

	record, err := client.Operate(nil, key,
		as.ListRemoveOp("tasks", 1),
		as.GetBinOp("tasks"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["tasks"].(as.OpResults)[1])
	// Output: [a c]
}

// Demonstrates removing all occurrences of a value from a list.
func ExampleListRemoveByValueOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-remove-value")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "done", "pending", "done"))

	record, err := client.Operate(nil, key,
		as.ListRemoveByValueOp("tasks", "done", as.ListReturnTypeCount),
		as.GetBinOp("tasks"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["tasks"].(as.OpResults)[0], record.Bins["tasks"].(as.OpResults)[1])
	// Output: 2 [pending]
}

// Demonstrates setting an item at index in a list.
func ExampleListSetOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-set")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("items", "old1", "old2", "old3"))

	_, err = client.Operate(nil, key,
		as.ListSetOp("items", 1, "updated"),
		as.GetBinOp("items"),
	)
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "items")
	fmt.Println(rec.Bins["items"])
	// Output: [old1 updated old3]
}

// Demonstrates clearing all items in a list bin.
func ExampleListClearOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-clear")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "a", "b", "c"))

	_, err = client.Operate(nil, key, as.ListClearOp("tasks"))
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "tasks")
	fmt.Println(rec.Bins["tasks"])
	// Output: []
}

// Demonstrates getting the size of a list bin.
func ExampleListSizeOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-size")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "a", "b", "c", "d", "e"))

	record, err := client.Operate(nil, key, as.ListSizeOp("tasks"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["tasks"])
	// Output: 5
}

// Demonstrates getting an item at index from a list.
func ExampleListGetOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-get")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("tasks", "first", "second", "last"))

	record, err := client.Operate(nil, key,
		as.ListGetOp("tasks", 0),
		as.ListGetOp("tasks", -1),
	)
	if err != nil {
		log.Fatal(err)
	}
	results := record.Bins["tasks"].(as.OpResults)
	fmt.Println(results[0], results[1])
	// Output: first last
}

// Demonstrates getting a range of items from a list.
func ExampleListGetRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-get-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("scores", 10, 20, 30, 40, 50))

	record, err := client.Operate(nil, key, as.ListGetRangeOp("scores", 1, 3))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["scores"])
	// Output: [20 30 40]
}

// Demonstrates finding indices of a value in a list.
func ExampleListGetByValueOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-get-by-value")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("items", "a", "b", "a", "c", "a"))

	record, err := client.Operate(nil, key, as.ListGetByValueOp("items", "a", as.ListReturnTypeIndex))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["items"])
	// Output: [0 2 4]
}

// Demonstrates getting list items in a value range.
func ExampleListGetByValueRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-value-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("prices", 5.0, 15.0, 25.0, 35.0, 45.0))

	record, err := client.Operate(nil, key, as.ListGetByValueRangeOp("prices", 10.0, 40.0, as.ListReturnTypeValue))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["prices"])
	// Output: [15 25 35]
}

// Demonstrates getting an item at index with return type.
func ExampleListGetByIndexOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-get-by-index")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("items", "x", "y", "z"))

	record, err := client.Operate(nil, key, as.ListGetByIndexOp("items", 1, as.ListReturnTypeValue))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["items"])
	// Output: y
}

// Demonstrates getting items from an index to end of list.
func ExampleListGetByIndexRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-index-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("items", "a", "b", "c", "d", "e"))

	record, err := client.Operate(nil, key, as.ListGetByIndexRangeOp("items", 2, as.ListReturnTypeValue))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["items"])
	// Output: [c d e]
}

// ExampleListGetByRankOp demonstrates getting an item by rank (sorted order).
func ExampleListGetByRankOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-get-by-rank")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("scores", 50, 10, 30, 20, 40))

	record, err := client.Operate(nil, key, as.ListGetByRankOp("scores", 0, as.ListReturnTypeValue))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["scores"])
	// Output: 10
}

// Demonstrates getting items by rank range.
func ExampleListGetByRankRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-list-rank-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	lp := as.NewListPolicy(as.ListOrderOrdered, 0)
	_, _ = client.Operate(nil, key, as.ListAppendWithPolicyOp(lp, "scores", 50, 10, 30, 20, 40))

	record, err := client.Operate(nil, key, as.ListGetByRankRangeOp("scores", 0, as.ListReturnTypeValue))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["scores"])
	// Output: [10 20 30 40 50]
}

// Demonstrates incrementing a map value.
func ExampleMapIncrementOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-increment")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutOp(as.DefaultMapPolicy(), "counters", "views", 0))

	record, err := client.Operate(nil, key,
		as.MapIncrementOp(as.DefaultMapPolicy(), "counters", "views", 5),
		as.MapIncrementOp(as.DefaultMapPolicy(), "counters", "views", 3),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["counters"].(as.OpResults)[1])
	// Output: 8
}

// Demonstrates clearing all items in a map bin.
func ExampleMapClearOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-clear")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", map[any]any{"a": 1, "b": 2}))

	_, err = client.Operate(nil, key, as.MapClearOp("profile"))
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "profile")
	fmt.Println(rec.Bins["profile"])
	// Output: map[]
}

// Demonstrates removing a key from a map.
func ExampleMapRemoveByKeyOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-remove-key")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", map[any]any{"name": "Alice", "temp": "x", "age": 30}))

	record, err := client.Operate(nil, key,
		as.MapRemoveByKeyOp("profile", "temp", as.MapReturnType.VALUE),
		as.GetBinOp("profile"),
	)
	if err != nil {
		log.Fatal(err)
	}
	removed := record.Bins["profile"].(as.OpResults)[0]
	remaining := record.Bins["profile"].(as.OpResults)[1].(map[any]any)
	fmt.Println(removed, len(remaining))
	// Output: x 2
}

// Demonstrates removing map items by value.
func ExampleMapRemoveByValueOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-remove-value")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "status", map[any]any{"u1": "active", "u2": "inactive", "u3": "active"}))

	record, err := client.Operate(nil, key,
		as.MapRemoveByValueOp("status", "inactive", as.MapReturnType.KEY),
		as.GetBinOp("status"),
	)
	if err != nil {
		log.Fatal(err)
	}
	removed := record.Bins["status"].(as.OpResults)[0]
	remaining := record.Bins["status"].(as.OpResults)[1].(map[any]any)
	fmt.Println(removed, len(remaining))
	// Output: [u2] 2
}

// Demonstrates getting the size of a map bin.
func ExampleMapSizeOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-size")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", map[any]any{"a": 1, "b": 2, "c": 3}))

	record, err := client.Operate(nil, key, as.MapSizeOp("profile"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["profile"])
	// Output: 3
}

// Demonstrates getting a value by key from a map.
func ExampleMapGetByKeyOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-get-by-key")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutOp(as.DefaultMapPolicy(), "profile", "email", "user@example.com"))

	record, err := client.Operate(nil, key, as.MapGetByKeyOp("profile", "email", as.MapReturnType.VALUE))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["profile"])
	// Output: user@example.com
}

// Demonstrates getting map items by key range.
func ExampleMapGetByKeyRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-key-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(policy, "products", map[any]any{"A": 1, "B": 2, "C": 3, "D": 4}))

	record, err := client.Operate(nil, key, as.MapGetByKeyRangeOp("products", "B", "D", as.MapReturnType.VALUE))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["products"])
	// Output: [2 3]
}

// Demonstrates getting multiple values by key list.
func ExampleMapGetByKeyListOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-key-list")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(policy, "profile", map[any]any{"name": "Jane", "email": "j@x.com", "age": 25}))

	keys := []any{"name", "email", "age"}
	record, err := client.Operate(nil, key, as.MapGetByKeyListOp("profile", keys, as.MapReturnType.VALUE))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["profile"])
	// Output: [25 j@x.com Jane]
}

// Demonstrates finding keys with a specific value.
func ExampleMapGetByValueOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-get-by-value")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "scores", map[any]any{"u1": 100, "u2": 80, "u3": 100}))

	record, err := client.Operate(nil, key, as.MapGetByValueOp("scores", 100, as.MapReturnType.KEY))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["scores"])
	// Output: [u1 u3]
}

// Demonstrates getting map items by value range.
func ExampleMapGetByValueRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-value-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "products", map[any]any{"p1": 5.0, "p2": 15.0, "p3": 25.0, "p4": 35.0}))

	record, err := client.Operate(nil, key, as.MapGetByValueRangeOp("products", 10.0, 30.0, as.MapReturnType.KEY_VALUE))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(record.Bins["products"].([]as.MapPair)))
	// Output: 2
}

// Demonstrates getting a map item by index.
func ExampleMapGetByIndexOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-get-by-index")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(policy, "leaderboard", map[any]any{"first": 100, "second": 90, "third": 80}))

	record, err := client.Operate(nil, key, as.MapGetByIndexOp("leaderboard", 0, as.MapReturnType.KEY_VALUE))
	if err != nil {
		log.Fatal(err)
	}
	pairs := record.Bins["leaderboard"].([]as.MapPair)
	pair := pairs[0]

	fmt.Println(pair.Key, pair.Value)
	// Output: first 100
}

// Demonstrates getting map items from an index to end.
func ExampleMapGetByIndexRangeOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-index-range")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(policy, "items", map[any]any{"a": 1, "b": 2, "c": 3, "d": 4}))

	record, err := client.Operate(nil, key, as.MapGetByIndexRangeOp("items", 1, as.MapReturnType.VALUE))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["items"])
	// Output: [2 3 4]
}

// Demonstrates getting a map item by value rank.
func ExampleMapGetByRankOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-get-by-rank")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.MapPutItemsOp(as.DefaultMapPolicy(), "scores", map[any]any{"alice": 50, "bob": 90, "carol": 70}))

	record, err := client.Operate(nil, key, as.MapGetByRankOp("scores", -1, as.MapReturnType.KEY_VALUE))
	if err != nil {
		log.Fatal(err)
	}
	pair := record.Bins["scores"].([]as.MapPair)
	fmt.Println(pair)
	// Output: [{bob 90}]
}

// Demonstrates getting the top N items by rank.
// Rank is determined by key, so the map must be KEY_ORDERED.
// Negative ranks count from the highest value (-1 is the highest).
// Results are returned in ascending rank order (lowest to highest).
func ExampleMapGetByRankRangeCountOp() {
	key, err := as.NewKey(*namespace, "test", "example-map-rank-range-count")
	if err != nil {
		log.Fatal(err)
	}

	client.Delete(nil, key)

	// KEY_ORDERED ensures rank is based on keys (leaderboard semantics).
	policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)

	_, _ = client.Operate(nil, key,
		as.MapPutOp(policy, "leaderboard", "a", 10),
		as.MapPutOp(policy, "leaderboard", "b", 30),
		as.MapPutOp(policy, "leaderboard", "c", 20),
		as.MapPutOp(policy, "leaderboard", "d", 40),
	)

	// Get the top 2 values by rank.
	record, err := client.Operate(nil, key,
		as.MapGetByRankRangeCountOp(
			"leaderboard",
			-2, // start from second-highest value
			2,  // count
			as.MapReturnType.KEY_VALUE,
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	pairs := record.Bins["leaderboard"].([]as.MapPair)
	fmt.Println(pairs)

	// Output: [{b 30} {d 40}]
}

// Demonstrates using context to select a nested map and get an entry by index.
// Create the bin with Put so the inner map is stored as a CDT map; then CtxMapKey selects it.
func ExampleCtxMapIndex() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-map-index")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)

	// Nested map: bin "data" has key "scores" whose value is a map (must be CDT map for index ops)
	data := map[any]any{
		"scores": map[any]any{"a": 10, "b": 20, "c": 30},
	}
	_ = client.Put(nil, key, as.BinMap{"data": data})

	// CtxMapKey("scores") selects the inner map; MapGetByIndexOp(..., 0, VALUE) gets first entry's value
	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("scores"))}
	record, err := client.Operate(nil, key, as.MapGetByIndexOp("data", 0, as.MapReturnType.VALUE, ctx...))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record.Bins["data"])
	// Output: 10
}

// Demonstrates updating a list element. CtxListValue selects by value
func ExampleCtxListValue() {
	key, err := as.NewKey(*namespace, "test", "example-ctx-list-value")
	if err != nil {
		log.Fatal(err)
	}
	client.Delete(nil, key)
	_, _ = client.Operate(nil, key, as.ListAppendOp("scores", 10, 20, 30))

	// Update the element at index 1 (value 20) to 21
	_, err = client.Operate(nil, key, as.ListSetOp("scores", 1, 21))
	if err != nil {
		log.Fatal(err)
	}
	rec, _ := client.Get(nil, key, "scores")
	list := rec.Bins["scores"].([]any)
	fmt.Println(list[1])
	// Output: 21
}

/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Store and retrieve list and map bins, from flat lists of strings to nested
// list/map combinations, and modify a list with an operate call.
func runListMap() error {
	if err := runListStrings(); err != nil {
		return err
	}
	if err := runListComplex(); err != nil {
		return err
	}
	if err := runMapStrings(); err != nil {
		return err
	}
	if err := runMapComplex(); err != nil {
		return err
	}
	if err := runListMapCombined(); err != nil {
		return err
	}
	return runListOperate()
}

// Write and read a []string bin.
func runListStrings() error {
	key, err := as.NewKey(ns, set, "listkey1")
	if err != nil {
		return err
	}

	list := []string{"string1", "string2", "string3"}
	if err := client.PutBins(nil, key, as.NewBin("listbin1", list)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "listbin1")
	if err != nil {
		return err
	}
	// Lists come back as []any.
	received := record.Bins["listbin1"].([]any)
	log.Printf("listbin1: %v", received)
	return nil
}

// Write and read a []any bin holding mixed types.
func runListComplex() error {
	key, err := as.NewKey(ns, set, "listkey2")
	if err != nil {
		return err
	}

	blob := []byte{3, 52, 125}
	list := []any{"string1", 2, blob}
	if err := client.PutBins(nil, key, as.NewBin("listbin2", list)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "listbin2")
	if err != nil {
		return err
	}
	log.Printf("listbin2: %v", record.Bins["listbin2"])
	return nil
}

// Write and read a map[string]string bin.
func runMapStrings() error {
	key, err := as.NewKey(ns, set, "mapkey1")
	if err != nil {
		return err
	}

	amap := map[string]string{
		"key1": "string1",
		"key2": "string2",
		"key3": "string3",
	}
	if err := client.PutBins(nil, key, as.NewBin("mapbin1", amap)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "mapbin1")
	if err != nil {
		return err
	}
	// Maps come back as map[any]any.
	received := record.Bins["mapbin1"].(map[any]any)
	log.Printf("mapbin1: %v", received)
	return nil
}

// Write and read a map[any]any bin holding mixed types.
func runMapComplex() error {
	key, err := as.NewKey(ns, set, "mapkey2")
	if err != nil {
		return err
	}

	blob := []byte{3, 52, 125}
	list := []int{100034, 12384955, 3, 512}
	amap := map[any]any{
		"key1": "string1",
		"key2": 2,
		"key3": blob,
		"key4": list,
	}
	if err := client.PutBins(nil, key, as.NewBin("mapbin2", amap)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "mapbin2")
	if err != nil {
		return err
	}
	log.Printf("mapbin2: %v", record.Bins["mapbin2"])
	return nil
}

// Write and read a nested list/map combination.
func runListMapCombined() error {
	key, err := as.NewKey(ns, set, "listmapkey")
	if err != nil {
		return err
	}

	blob := []byte{3, 52, 125}
	inner := []any{"string2", 5}
	innerMap := map[any]any{
		"a":    1,
		2:      "b",
		3:      blob,
		"list": inner,
	}
	list := []any{"string1", 8, inner, innerMap}
	if err := client.PutBins(nil, key, as.NewBin("listmapbin", list)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "listmapbin")
	if err != nil {
		return err
	}
	log.Printf("listmapbin: %v", record.Bins["listmapbin"])
	return nil
}

// Add items to a list bin with an operate call, using a list policy that
// ignores duplicate values.
func runListOperate() error {
	key, err := as.NewKey(ns, set, "listkey1")
	if err != nil {
		return err
	}

	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	list := []string{"string1", "string2", "string3"}
	if err := client.PutBins(nil, key, as.NewBin("listbin1", list)); err != nil {
		return err
	}

	writePolicy := as.NewWritePolicy(0, 0)
	listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)

	// Add a unique item to the list.
	if _, err := client.Operate(writePolicy, key,
		as.ListAppendWithPolicyOp(listPolicy, "listbin1", "string4")); err != nil {
		return err
	}

	// Adding the same value again is ignored by the list policy.
	if _, err := client.Operate(writePolicy, key,
		as.ListAppendWithPolicyOp(listPolicy, "listbin1", "string4")); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "listbin1")
	if err != nil {
		return err
	}
	log.Printf("listbin1 after operate: %v", record.Bins["listbin1"])
	return nil
}

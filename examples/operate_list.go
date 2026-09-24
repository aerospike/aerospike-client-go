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

// Perform server-side operations on list bins: append, pop, insert, remove,
// range reads, and navigating into a nested list with a context.
func runOperateList() error {
	if err := runListBasics(); err != nil {
		return err
	}
	return runListNested()
}

// Append, pop, insert, remove and read ranges on a simple list bin.
func runListBasics() error {
	key, err := as.NewKey(ns, set, "listkey")
	if err != nil {
		return err
	}

	// Append items and read the resulting size and list in one call.
	record, err := client.Operate(nil, key,
		as.ListAppendOp("tasks", "task1", "task2", "task3"),
		as.GetBinOp("tasks"))
	if err != nil {
		return err
	}
	results := record.Bins["tasks"].(as.OpResults)
	log.Printf("Appended: size=%v list=%v", results[0], results[1])

	// Insert an item at a specific index.
	record, err = client.Operate(nil, key,
		as.ListInsertOp("tasks", 1, "inserted"),
		as.GetBinOp("tasks"))
	if err != nil {
		return err
	}
	log.Printf("After insert: %v", record.Bins["tasks"].(as.OpResults)[1])

	// Pop the first item off and report the remaining size.
	record, err = client.Operate(nil, key,
		as.ListPopOp("tasks", 0),
		as.ListSizeOp("tasks"))
	if err != nil {
		return err
	}
	results = record.Bins["tasks"].(as.OpResults)
	log.Printf("Popped %v, %v items remain", results[0], results[1])

	// Remove all occurrences of a value.
	record, err = client.Operate(nil, key,
		as.ListRemoveByValueOp("tasks", "task2", as.ListReturnTypeCount),
		as.GetBinOp("tasks"))
	if err != nil {
		return err
	}
	results = record.Bins["tasks"].(as.OpResults)
	log.Printf("Removed %v matching items, remaining: %v", results[0], results[1])

	// Read a range of items without modifying the list.
	record, err = client.Operate(nil, key, as.ListGetRangeOp("tasks", 0, 2))
	if err != nil {
		return err
	}
	log.Printf("Range read: %v", record.Bins["tasks"])

	return nil
}

// Navigate into a list of lists using a context, without reading the whole
// bin back and forth.
func runListNested() error {
	key, err := as.NewKey(ns, set, "listkey2")
	if err != nil {
		return err
	}

	list := []any{[]any{7, 9, 5}, []any{1, 2, 3}, []any{6, 5, 4, 1}}
	if err := client.Put(nil, key, as.BinMap{"bin": list}); err != nil {
		return err
	}

	// Append to the last inner list and read the whole bin back.
	record, err := client.Operate(nil, key,
		as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "bin", []*as.CDTContext{as.CtxListIndex(-1)}, 11),
		as.GetBinOp("bin"))
	if err != nil {
		return err
	}
	log.Printf("After nested append: %v", record.Bins["bin"].(as.OpResults)[1])

	return nil
}

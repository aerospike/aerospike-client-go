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

// Perform server-side operations on map bins: put, increment, remove, a
// leaderboard-style rank query, and navigating into a nested map with a
// context.
func runOperateMap() error {
	if err := runMapBasics(); err != nil {
		return err
	}
	return runMapNested()
}

// Put, increment, remove and rank-query a simple map bin.
func runMapBasics() error {
	key, err := as.NewKey(ns, set, "mapkey")
	if err != nil {
		return err
	}

	// Put several entries at once and read the resulting size.
	record, err := client.Operate(nil, key,
		as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", map[any]any{"name": "Alice", "temp": "x", "age": 30}),
		as.MapSizeOp("profile"))
	if err != nil {
		return err
	}
	log.Printf("Profile size after put: %v", record.Bins["profile"].(as.OpResults)[1])

	// Remove a key, returning its value, and report the new size.
	record, err = client.Operate(nil, key,
		as.MapRemoveByKeyOp("profile", "temp", as.MapReturnType.VALUE),
		as.MapSizeOp("profile"))
	if err != nil {
		return err
	}
	results := record.Bins["profile"].(as.OpResults)
	log.Printf("Removed %v, %v entries remain", results[0], results[1])

	// A key-ordered map is required for rank-based operations to reflect
	// value order (leaderboard semantics).
	scorePolicy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
	scoreKey, err := as.NewKey(ns, set, "mapkey_scores")
	if err != nil {
		return err
	}

	if _, err := client.Operate(nil, scoreKey,
		as.MapPutOp(scorePolicy, "scores", "alice", 55),
		as.MapPutOp(scorePolicy, "scores", "bob", 98),
		as.MapPutOp(scorePolicy, "scores", "carol", 76),
		as.MapPutOp(scorePolicy, "scores", "dave", 82)); err != nil {
		return err
	}

	// Increment two scores in one call.
	record, err = client.Operate(nil, scoreKey,
		as.MapIncrementOp(as.DefaultMapPolicy(), "scores", "carol", 5),
		as.MapIncrementOp(as.DefaultMapPolicy(), "scores", "bob", -4))
	if err != nil {
		return err
	}
	log.Printf("Scores after increment: %v", record.Bins["scores"])

	// Read the top two scores by rank.
	record, err = client.Operate(nil, scoreKey,
		as.MapGetByRankRangeCountOp("scores", -2, 2, as.MapReturnType.KEY_VALUE))
	if err != nil {
		return err
	}
	log.Printf("Top two scores: %v", record.Bins["scores"])

	return nil
}

// Navigate into a map of maps using a context, updating one entry without
// reading and rewriting the whole bin.
func runMapNested() error {
	key, err := as.NewKey(ns, set, "mapkey_nested")
	if err != nil {
		return err
	}

	inputMap := map[any]any{
		"key1": map[any]any{"key11": 9, "key12": 4},
		"key2": map[any]any{"key21": 3, "key22": 5},
	}
	if err := client.Put(nil, key, as.BinMap{"bin": inputMap}); err != nil {
		return err
	}

	// Update "key21" inside the map at "key2" and read the whole bin back.
	// The context is a trailing argument on the regular map operation.
	ctx := []*as.CDTContext{as.CtxMapKey(as.StringValue("key2"))}
	record, err := client.Operate(nil, key,
		as.MapPutOp(as.DefaultMapPolicy(), "bin", "key21", 11, ctx...),
		as.GetBinOp("bin"))
	if err != nil {
		return err
	}
	log.Printf("After nested update: %v", record.Bins["bin"].(as.OpResults)[1])

	return nil
}

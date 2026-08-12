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

// Package batch shows multi-record writes through one session chain.
//
// Port of the Java SDK's BatchExample, by way of the Rust SDK's `batch`.
// One Insert writes five keys at once; a second chain mixes Insert, Update and
// Delete across different keys in a single round trip, and a set-wide query
// prints the result after each step.
package batch

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Run executes the example.
func Run(env *exrun.Env) error {
	set, err := env.DataSet("batch_example")
	if err != nil {
		return err
	}

	env.Printf("*************")
	env.Printf("* Batch tests")
	env.Printf("*************")

	env.Printf("Batch Insert:")
	keys := set.Keys([]int64{1, 2, 3, 4, 5})
	stream, err := env.Session.Insert(keys).
		Bin("name").SetTo("Fred").
		Bin("age").SetTo(30).
		Bin("value").SetTo(10).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.Collect(); err != nil {
		return err
	}
	if err := env.Dump(set); err != nil {
		return err
	}

	// One chain, three verbs, three different key sets: insert 6/7/8, bump
	// `value` on 2, and delete 1. The client sends this as a single batch.
	env.Printf("Batch Modify:")
	newKeys := set.Keys([]int64{6, 7, 8})
	bump := set.Key(int64(2))
	gone := set.Key(int64(1))
	stream, err = env.Session.Insert(newKeys).
		Bin("name").SetTo("Wilma").
		Bin("age").SetTo(33).
		Bin("value").SetTo(20).
		Update([]*as.Key{bump}).
		Bin("value").Add(5).
		Delete([]*as.Key{gone}).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.Collect(); err != nil {
		return err
	}
	return env.Dump(set)
}

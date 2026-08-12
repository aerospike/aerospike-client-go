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

// Package stringops shows the server-side string read and modify operations.
//
// Port of the Java SDK's StringOperationsExample, by way of the Rust SDK's
// `string_operations`. Three surfaces reach the same operations: the fluent bin
// builder, raw [as.Operation] values appended to a write chain, and string
// expressions read into projection bins by a query. The whole example needs
// server 8.1.3 or later.
package stringops

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Run executes the example.
func Run(env *exrun.Env) error {
	if !env.Cluster.SupportsStringOperations() {
		env.Printf("skipping string operations: they require server 8.1.3 or later")
		return nil
	}

	set, err := env.DataSet("string_ops_demo")
	if err != nil {
		return err
	}
	key := set.Key("row1")

	env.Printf("--- 1) Fluent bin builder: strlen, substr [1,4), substr from 3, find, upper ---")
	if err := reset(env, key); err != nil {
		return err
	}

	// Six operations, one atomic call. The uppercase rewrites the bin, so the
	// trailing Get observes the uppercased value while the reads before it still
	// saw "hello". The bin map holds only the last value, hence the positional
	// results.
	stream, err := env.Session.Upsert(key).
		Bin("message").StrLen().
		Bin("message").StrSubstr(1, 4).
		Bin("message").StrSubstrFrom(3).
		Bin("message").StrFind("ll").
		AddOperation(as.StrUpperOp(as.NewStringPolicy(as.StringWriteDefault), "message")).
		Bin("message").Get().
		Execute()
	if err != nil {
		return err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return err
	}
	env.Printf("strlen -> %s", show(row.OperationResult(0)))
	env.Printf("substr(1,4) [1,4) -> %s", show(row.OperationResult(1)))
	env.Printf("substr(3) suffix -> %s", show(row.OperationResult(2)))
	env.Printf("find ll -> %s", show(row.OperationResult(3)))
	env.Printf("upper result -> %s", show(row.OperationResult(4)))
	env.Printf("get bin after upper -> %s", show(row.OperationResult(5)))

	env.Printf("--- 2) Raw string Operations appended to a chain: same reads on a fresh value ---")
	if err := reset(env, key); err != nil {
		return err
	}

	// Any core string operation can be handed to the chain directly, which is
	// how you reach the ones the bin builder does not name.
	stream, err = env.Session.Upsert(key).
		AddOperation(as.StrLenOp("message")).
		AddOperation(as.StrSubstrOp("message", 1, 4)).
		AddOperation(as.StrFindOp("message", "ll")).
		Execute()
	if err != nil {
		return err
	}
	row, err = stream.FirstOrRaise()
	if err != nil {
		return err
	}
	env.Printf("strlen / substr / find via Operation list -> %s, %s, %s",
		show(row.OperationResult(0)),
		show(row.OperationResult(1)),
		show(row.OperationResult(2)))

	env.Printf("--- 3) Query: string expressions read into projection bins ---")
	if err := reset(env, key); err != nil {
		return err
	}

	// The same computations as expressions, evaluated server-side and delivered
	// as bins that exist only in the reply.
	stream, err = env.Session.Query(key).
		Bin("slen").SelectFrom(as.ExpStringLen(as.ExpStringBin("message")), false).
		Bin("stail").SelectFrom(as.ExpStringSubstrFrom(as.ExpIntVal(3), as.ExpStringBin("message")), false).
		Bin("atLl").SelectFrom(as.ExpStringFind(as.ExpStringVal("ll"), as.ExpStringBin("message")), false).
		Execute()
	if err != nil {
		return err
	}
	row, err = stream.FirstOrRaise()
	if err != nil {
		return err
	}
	record, err := row.RecordOrRaise()
	if err != nil {
		return err
	}
	env.Printf("slen=%s, stail=%s, find(ll)=%s",
		show(lookup(record.Bins, "slen")),
		show(lookup(record.Bins, "stail")),
		show(lookup(record.Bins, "atLl")))
	return nil
}

// reset puts `message` back to "hello" so each section starts from the same
// value.
func reset(env *exrun.Env, key *as.Key) error {
	stream, err := env.Session.Upsert(key).Bin("message").SetTo("hello").Execute()
	if err != nil {
		return err
	}
	_, err = stream.FirstOrRaise()
	return err
}

// lookup adapts a bin map read to the (value, present) shape show expects.
func lookup(bins as.BinMap, name string) (any, bool) {
	v, ok := bins[name]
	return v, ok
}

// show renders an operation or projection result. A modify operation such as
// the uppercase returns no value, so its slot reads nil.
func show(v any, present bool) string {
	if !present {
		return "<none>"
	}
	return exrun.Render(v)
}

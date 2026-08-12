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

// Package mapremoverange shows what a map *modify* expression returns when it is
// used as a read.
//
// Port of the Java SDK's MapRemoveByKeyRangeTest, by way of the Rust SDK's
// `map_remove_by_key_range`. [as.ExpMapRemoveByKeyRange] is documented to accept
// only the NONE and INVERTED return types, while a map modify expression is
// documented to yield the bin's value. This example runs the same removal under
// six return types against a known map and prints what the server actually
// answers — then confirms the stored record was never touched, because a read
// expression evaluates against a copy.
package mapremoverange

import (
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Run executes the example.
func Run(env *exrun.Env) error {
	set, err := env.DataSet("map_remove_test")
	if err != nil {
		return err
	}
	key := set.Key(int64(1))

	source := map[any]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
	stream, err := env.Session.Upsert(key).Bin("m").SetTo(source).Execute()
	if err != nil {
		return err
	}
	if _, err := stream.FirstOrRaise(); err != nil {
		return err
	}
	env.Printf("Source map: %s", exrun.Render(source))
	env.Printf("")

	// The range ["b", "e") — begin inclusive, end exclusive — covers b, c and d,
	// so three items are removed and {a=1, e=5} is left.
	cases := []struct {
		name       string
		returnType as.MapReturnTypes
		note       string
		expected   string
	}{
		{
			"NONE", as.MapReturnType.NONE,
			"Documented as valid. Module doc says modify expressions return the bin's value.",
			"the modified map {a=1, e=5}",
		},
		{
			"INVERTED", as.MapReturnType.INVERTED,
			"Documented as valid. Inverted removes everything OUTSIDE the range.",
			"remove {a=1, e=5}, leaving {b=2, c=3, d=4}",
		},
		{
			"COUNT", as.MapReturnType.COUNT,
			"Not documented as valid.",
			"the count of removed items (3), if the doc is wrong",
		},
		{
			"KEY", as.MapReturnType.KEY,
			"Not documented as valid.",
			"the removed keys [b, c, d], if the doc is wrong",
		},
		{
			"VALUE", as.MapReturnType.VALUE,
			"Not documented as valid.",
			"the removed values [2, 3, 4], if the doc is wrong",
		},
		{
			"KEY_VALUE", as.MapReturnType.KEY_VALUE,
			"Not documented as valid.",
			"the removed entries {b=2, c=3, d=4}, if the doc is wrong",
		},
	}

	for i, c := range cases {
		env.Printf("=== Test %d: ExpMapRemoveByKeyRange(%s, \"b\", \"e\") ===", i+1, c.name)
		env.Printf("%s", c.note)
		env.Printf("Expected: %s", c.expected)
		value, err := selectExp(env, key, removeBToE(c.returnType))
		if err != nil {
			env.Printf("ERROR:    %v", err)
		} else {
			env.Printf("Actual:   %s", exrun.Render(value))
			env.Printf("Type:     %s", typeName(value))
		}
		env.Printf("")
	}

	// A read expression never mutates: the stored map is still the original.
	env.Printf("=== Verify original map is unchanged ===")
	stream, err = env.Session.Query(key).Execute()
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
	env.Printf("Original map after all tests: %s", exrun.Render(record.Bins["m"]))
	return nil
}

// removeBToE builds the removal over the map bin `m`.
func removeBToE(returnType as.MapReturnTypes) *as.Expression {
	return as.ExpMapRemoveByKeyRange(returnType,
		as.ExpStringVal("b"), as.ExpStringVal("e"), as.ExpMapBin("m"))
}

// selectExp evaluates an expression as a read into a projection bin.
func selectExp(env *exrun.Env, key *as.Key, exp *as.Expression) (any, error) {
	stream, err := env.Session.Query(key).
		Bin("result").SelectFrom(exp, false).
		Execute()
	if err != nil {
		return nil, err
	}
	row, err := stream.FirstOrRaise()
	if err != nil {
		return nil, err
	}
	record, err := row.RecordOrRaise()
	if err != nil {
		return nil, err
	}
	return record.Bins["result"], nil
}

// typeName reports the Go type a result arrived as — the Go answer to Java's
// result.getClass().getName().
func typeName(v any) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%T", v)
}

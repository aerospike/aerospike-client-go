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

// Fixture factories for the server-side CDT (list/map) operation examples.

package fixtures

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

func OperateList() Fixture {
	keys := []string{"listkey", "listkey2"}
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			// listkey: append 3, insert at 1, pop index 0, remove all "task2".
			if err := AssertBinDeepEquals("listkey", "tasks",
				[]any{"inserted", "task3"}); err != nil {
				return err
			}
			// listkey2: append 11 to the last inner list.
			return AssertBinDeepEquals("listkey2", "bin",
				[]any{
					[]any{7, 9, 5},
					[]any{1, 2, 3},
					[]any{6, 5, 4, 1, 11},
				})
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func OperateString() Fixture {
	keys := []string{"opstr_read", "opstr_modify", "opstr_tostring"}
	return Fixture{
		Setup:   func() error { return DeleteKeys(keys...) },
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func OperateMap() Fixture {
	keys := []string{"mapkey", "mapkey_scores", "mapkey_nested"}
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			// mapkey: put 3 entries, remove "temp".
			if err := AssertBinDeepEquals("mapkey", "profile",
				map[any]any{"name": "Alice", "age": 30}); err != nil {
				return err
			}
			// mapkey_scores: carol +5 (76->81), bob -4 (98->94). The map is
			// KEY_ORDERED, so it reads back as ordered pairs, not a plain map.
			if err := AssertBinDeepEquals("mapkey_scores", "scores",
				[]as.MapPair{
					{Key: "alice", Value: 55},
					{Key: "bob", Value: 94},
					{Key: "carol", Value: 81},
					{Key: "dave", Value: 82},
				}); err != nil {
				return err
			}
			// mapkey_nested: key21 inside "key2" updated to 11.
			return AssertBinDeepEquals("mapkey_nested", "bin",
				map[any]any{
					"key1": map[any]any{"key11": 9, "key12": 4},
					"key2": map[any]any{"key21": 11, "key22": 5},
				})
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

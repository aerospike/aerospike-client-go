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

// Fixture factories for the basic record-operation examples.

package fixtures

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func Put() Fixture {
	const key = "putkey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		Validate: func() error {
			if err := AssertBin(key, "bin1", "value1"); err != nil {
				return err
			}
			return AssertBin(key, "bin2", "value2")
		},
		Cleanup: func() error {
			return DeleteKeys(key)
		},
	}
}

func Get() Fixture {
	const key = "getkey"
	return Fixture{
		Setup: func() error {
			return SeedRecords(map[string]as.BinMap{key: {"bin1": "value1", "bin2": "value2"}})
		},
		Cleanup: func() error {
			return DeleteKeys(key)
		},
	}
}

func Simple() Fixture {
	const key = "simplekey"
	return Fixture{
		Setup: func() error {
			return DeleteKeys(key)
		},
		Validate: func() error {
			return AssertRecordMissing(key)
		},
		Cleanup: func() error {
			return DeleteKeys(key)
		},
	}
}

func Add() Fixture {
	const key = "addkey"
	return Fixture{
		Setup:    func() error { return DeleteKeys(key) },
		Validate: func() error { return AssertBin(key, "addbin", 45) },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func Append() Fixture {
	const key = "appendkey"
	return Fixture{
		Setup:    func() error { return DeleteKeys(key) },
		Validate: func() error { return AssertBin(key, "appendbin", "Hello World") },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func Prepend() Fixture {
	const key = "prependkey"
	return Fixture{
		Setup:    func() error { return DeleteKeys(key) },
		Validate: func() error { return AssertBin(key, "prependbin", "Hello World") },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func Replace() Fixture {
	keys := []string{"replacekey", "replaceonlykey"}
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			// REPLACE must have discarded bin1/bin2 and written bin3.
			if err := AssertBin("replacekey", "bin3", "value3"); err != nil {
				return err
			}
			if err := AssertBin("replacekey", "bin1", nil); err != nil {
				return err
			}
			// REPLACE_ONLY on a missing record must not have created it.
			return AssertRecordMissing("replaceonlykey")
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func Operate() Fixture {
	const key = "opkey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		Validate: func() error {
			if err := AssertBin(key, "optintbin", 11); err != nil {
				return err
			}
			return AssertBin(key, "optstringbin", "new string")
		},
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

func Generation() Fixture {
	const key = "genkey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		// The stale-generation write must have been rejected, leaving the
		// value from the matching-generation write.
		Validate: func() error { return AssertBin(key, "genbin", "genvalue3") },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func Blob() Fixture {
	const key = "blobkey"
	want := map[string]string{"bin1": "Albert Einstein", "bin2": "Richard Feynman"}
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		Validate: func() error {
			k, err := as.NewKey(namespace, set, key)
			if err != nil {
				return err
			}
			record, err := client.Get(nil, k)
			if err != nil {
				return err
			}
			for bin, name := range want {
				blob, ok := record.Bins[bin].([]byte)
				if !ok || !bytes.Equal(blob, []byte(name)) {
					return fmt.Errorf("blob bin %q mismatch: got %v", bin, record.Bins[bin])
				}
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

func Batch() Fixture {
	// The example writes the records itself; the fixture only guarantees a
	// clean slate and verifies what was written.
	keys := numberedKeys("batchkey", 8)
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			for i, key := range keys {
				if err := AssertBin(key, "batchbin", "batchvalue"+strconv.Itoa(i+1)); err != nil {
					return err
				}
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func Expire() Fixture {
	const key = "expirekey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		// The final write replaced the expired record with a never-expiring
		// one.
		Validate: func() error { return AssertBin(key, "expirebin", "noexpirevalue") },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func Touch() Fixture {
	const key = "touchkey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		// The example ends after the touched TTL passed, so the record must
		// be gone.
		Validate: func() error { return AssertRecordMissing(key) },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

func ListMap() Fixture {
	keys := []string{"listkey1", "listkey2", "mapkey1", "mapkey2", "listmapkey"}
	blob := []byte{3, 52, 125}
	inner := []any{"string2", 5}
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			// After the operate step, listbin1 holds the unique fourth item.
			if err := AssertBinDeepEquals("listkey1", "listbin1",
				[]any{"string1", "string2", "string3", "string4"}); err != nil {
				return err
			}
			if err := AssertBinDeepEquals("listkey2", "listbin2",
				[]any{"string1", 2, blob}); err != nil {
				return err
			}
			if err := AssertBinDeepEquals("mapkey1", "mapbin1",
				map[any]any{"key1": "string1", "key2": "string2", "key3": "string3"}); err != nil {
				return err
			}
			if err := AssertBinDeepEquals("mapkey2", "mapbin2",
				map[any]any{"key1": "string1", "key2": 2, "key3": blob,
					"key4": []any{100034, 12384955, 3, 512}}); err != nil {
				return err
			}
			return AssertBinDeepEquals("listmapkey", "listmapbin",
				[]any{"string1", 8, inner,
					map[any]any{"a": 1, 2: "b", 3: blob, "list": inner}})
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func ListIter(expected [][]int64) Fixture {
	const key = "addkey"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		// The custom packer must produce the same wire format as reflection:
		// the read-back value must deep-equal the dataset that was written.
		Validate: func() error {
			k, err := as.NewKey(namespace, set, key)
			if err != nil {
				return err
			}
			record, err := client.Get(nil, k, "bin")
			if err != nil {
				return err
			}
			rows, ok := record.Bins["bin"].([]any)
			if !ok {
				return fmt.Errorf("bin is not a list: %T", record.Bins["bin"])
			}
			received := make([][]int64, 0, len(rows))
			for _, row := range rows {
				cols := []int64{}
				for _, col := range row.([]any) {
					cols = append(cols, int64(col.(int)))
				}
				received = append(received, cols)
			}
			if !reflect.DeepEqual(received, expected) {
				return fmt.Errorf("read-back data does not match the written dataset (%d rows)", len(rows))
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

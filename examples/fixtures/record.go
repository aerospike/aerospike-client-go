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

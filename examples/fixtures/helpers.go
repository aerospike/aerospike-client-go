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

// Shared helpers used by the fixture factories: the toolkit for seeding,
// deleting and asserting database state.

package fixtures

import (
	"fmt"
	"reflect"
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// DeleteKeys removes the given records from the target namespace/set.
// Missing records are not an error, so it doubles as idempotent setup.
func DeleteKeys(userKeys ...string) error {
	for _, userKey := range userKeys {
		key, err := as.NewKey(namespace, set, userKey)
		if err != nil {
			return err
		}
		if _, err := client.Delete(nil, key); err != nil {
			return err
		}
	}
	return nil
}

// SeedRecords writes the given records so an example has known state to
// operate on. Existing records are overwritten.
func SeedRecords(records map[string]as.BinMap) error {
	for userKey, bins := range records {
		key, err := as.NewKey(namespace, set, userKey)
		if err != nil {
			return err
		}
		if err := client.Put(nil, key, bins); err != nil {
			return err
		}
	}
	return nil
}

// AssertBin reads a record back from the database and compares one bin
// against the expected value.
func AssertBin(userKey, bin string, want any) error {
	key, err := as.NewKey(namespace, set, userKey)
	if err != nil {
		return err
	}
	record, err := client.Get(nil, key, bin)
	if err != nil {
		return err
	}
	if got := record.Bins[bin]; got != want {
		return fmt.Errorf("key %q bin %q: got %v (%T), want %v (%T)", userKey, bin, got, got, want, want)
	}
	return nil
}

// DeleteIntKeys removes records keyed by the integers from..to inclusive.
func DeleteIntKeys(from, to int) error {
	for i := from; i <= to; i++ {
		key, err := as.NewKey(namespace, set, i)
		if err != nil {
			return err
		}
		if _, err := client.Delete(nil, key); err != nil {
			return err
		}
	}
	return nil
}

// AssertBinDeepEquals reads a record back and deep-compares one bin against
// the expected structure (for list/map bins).
func AssertBinDeepEquals(userKey, bin string, want any) error {
	key, err := as.NewKey(namespace, set, userKey)
	if err != nil {
		return err
	}
	record, err := client.Get(nil, key, bin)
	if err != nil {
		return err
	}
	if got := record.Bins[bin]; !reflect.DeepEqual(got, want) {
		return fmt.Errorf("key %q bin %q: got %#v, want %#v", userKey, bin, got, want)
	}
	return nil
}

// AssertRecordMissing verifies that a record does not exist.
func AssertRecordMissing(userKey string) error {
	key, err := as.NewKey(namespace, set, userKey)
	if err != nil {
		return err
	}
	exists, err := client.Exists(nil, key)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("key %q: record exists, want missing", userKey)
	}
	return nil
}

// numberedKeys returns userKeys prefix1..prefixN.
func numberedKeys(prefix string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i+1)
	}
	return keys
}

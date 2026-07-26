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

// Fixture factories for the scan and query examples.

package fixtures

import (
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func ScanSerial() Fixture {
	const size = 25
	keys := numberedKeys("scankey", size)
	return Fixture{
		Setup: func() error {
			records := make(map[string]as.BinMap, len(keys))
			for i, key := range keys {
				records[key] = as.BinMap{"scanbin": i + 1}
			}
			return SeedRecords(records)
		},
		// The set may hold unrelated records on a customer cluster, so the
		// independent recount asserts a lower bound rather than equality.
		Validate: func() error {
			count, err := countSetRecords()
			if err != nil {
				return err
			}
			if count < size {
				return fmt.Errorf("scanned %d records, want at least %d", count, size)
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

// countSetRecords counts the records currently in the target set.
func countSetRecords() (int, error) {
	recordset, err := client.ScanAll(nil, namespace, set)
	if err != nil {
		return 0, err
	}
	defer recordset.Close()

	count := 0
	for res := range recordset.Results() {
		if res.Err != nil {
			return 0, res.Err
		}
		count++
	}
	return count, nil
}

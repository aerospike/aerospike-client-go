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
	"strings"

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

func Expressions() Fixture {
	keys := []string{"jacksapplekey1", "jillgrapekey2", "mangokey3", "Jimkey4apple",
		"JuliaGrapekey5", "SallyMANGOkey6", "SeanaPPlekey7", "SamGRAPEkey8",
		"Susankey9", "SandraPeachkey0"}
	return Fixture{
		// The example truncates the set and seeds its own records.
		Validate: func() error {
			queryPolicy := as.NewQueryPolicy()
			queryPolicy.FilterExpression = as.ExpRegexCompare("^.*apple.*",
				as.ExpRegexFlagICASE, as.ExpKey(as.ExpTypeSTRING))
			recordset, err := client.Query(queryPolicy, as.NewStatement(namespace, set))
			if err != nil {
				return err
			}
			defer recordset.Close()

			count := 0
			for res := range recordset.Results() {
				if res.Err != nil {
					return res.Err
				}
				userKey := res.Record.Key.Value().GetObject().(string)
				if !strings.Contains(strings.ToLower(userKey), "apple") {
					return fmt.Errorf("wrong key returned: %q, want a key containing \"apple\"", userKey)
				}
				count++
			}
			if count != 3 {
				return fmt.Errorf("query returned %d records, want 3", count)
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func ScanPaginate() Fixture {
	keys := numberedKeys("pagekey", 100)
	return Fixture{
		Setup: func() error {
			records := make(map[string]as.BinMap, len(keys))
			for i, key := range keys {
				records[key] = as.BinMap{"pagebin": i + 1}
			}
			return SeedRecords(records)
		},
		// The paginated scan must have covered at least the seeded records.
		Validate: func() error {
			count, err := countSetRecords()
			if err != nil {
				return err
			}
			if count < len(keys) {
				return fmt.Errorf("set holds %d records, want at least %d", count, len(keys))
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func ScanParallel() Fixture {
	keys := numberedKeys("spkey", 25)
	return Fixture{
		Setup: func() error {
			records := make(map[string]as.BinMap, len(keys))
			for i, key := range keys {
				records[key] = as.BinMap{"spbin": i + 1}
			}
			return SeedRecords(records)
		},
		Validate: func() error {
			count, err := countSetRecords()
			if err != nil {
				return err
			}
			if count < len(keys) {
				return fmt.Errorf("set holds %d records, want at least %d", count, len(keys))
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func CountSetObjects() Fixture {
	keys := numberedKeys("cntkey", 10)
	return Fixture{
		Setup: func() error {
			records := make(map[string]as.BinMap, len(keys))
			for i, key := range keys {
				records[key] = as.BinMap{"cntbin": i + 1}
			}
			return SeedRecords(records)
		},
		Validate: func() error {
			count, err := countSetRecords()
			if err != nil {
				return err
			}
			if count < len(keys) {
				return fmt.Errorf("set holds %d records, want at least %d", count, len(keys))
			}
			return nil
		},
		Cleanup: func() error { return DeleteKeys(keys...) },
	}
}

func GeoJSONQuery() Fixture {
	const indexName = "testset_geo_index"
	return Fixture{
		Setup: func() error {
			// Best effort: a leftover index or records from a failed run.
			_ = client.DropIndex(nil, namespace, set, indexName)
			return DeleteIntKeys(0, 3)
		},
		// All 4 seeded points lie within the queried radius.
		Validate: func() error {
			statement := as.NewStatement(namespace, set)
			statement.SetFilter(as.NewGeoWithinRadiusFilter("coord", 13.009318762, 80.003157854, 50000))
			recordset, err := client.Query(nil, statement)
			if err != nil {
				return err
			}
			defer recordset.Close()

			count := 0
			for res := range recordset.Results() {
				if res.Err != nil {
					return res.Err
				}
				count++
			}
			if count != 4 {
				return fmt.Errorf("geo query returned %d records, want 4", count)
			}
			return nil
		},
		Cleanup: func() error {
			if err := DeleteIntKeys(0, 3); err != nil {
				return err
			}
			_ = client.DropIndex(nil, namespace, set, indexName)
			return nil
		},
	}
}

func QueryAggregateAverage() Fixture {
	return Fixture{
		Setup: func() error { return DeleteIntKeys(1, 1000) },
		// Re-run the aggregation independently: sum(1..1000) and count.
		Validate: func() error {
			statement := as.NewStatement(namespace, set)
			recordset, err := client.QueryAggregate(nil, statement, "average", "average", as.StringValue("bin1"))
			if err != nil {
				return err
			}
			defer recordset.Close()

			verified := false
			for rec := range recordset.Results() {
				if rec.Err != nil {
					return rec.Err
				}
				result, ok := rec.Record.Bins["SUCCESS"].(map[any]any)
				if !ok {
					return fmt.Errorf("unexpected aggregation result: %v", rec.Record.Bins)
				}
				if result["sum"] != float64(500500) || result["count"] != float64(1000) {
					return fmt.Errorf("aggregation returned sum=%v count=%v, want sum=500500 count=1000",
						result["sum"], result["count"])
				}
				verified = true
			}
			if !verified {
				return fmt.Errorf("aggregation returned no result")
			}
			return nil
		},
		Cleanup: func() error {
			if err := DeleteIntKeys(1, 1000); err != nil {
				return err
			}
			// Best effort: module may be absent if the run failed early.
			if task, err := client.RemoveUDF(nil, "average.lua"); err == nil {
				<-task.OnComplete()
			}
			return nil
		},
	}
}

func QueryAggregateSum() Fixture {
	return Fixture{
		Setup: func() error { return DeleteIntKeys(1, 1000) },
		Validate: func() error {
			statement := as.NewStatement(namespace, set)
			recordset, err := client.QueryAggregate(nil, statement, "sum_single_bin", "sum_single_bin", as.StringValue("bin1"))
			if err != nil {
				return err
			}
			defer recordset.Close()

			verified := false
			for rec := range recordset.Results() {
				if rec.Err != nil {
					return rec.Err
				}
				if rec.Record.Bins["SUCCESS"] != float64(500500) {
					return fmt.Errorf("aggregation returned %v, want 500500", rec.Record.Bins["SUCCESS"])
				}
				verified = true
			}
			if !verified {
				return fmt.Errorf("aggregation returned no result")
			}
			return nil
		},
		Cleanup: func() error {
			if err := DeleteIntKeys(1, 1000); err != nil {
				return err
			}
			if task, err := client.RemoveUDF(nil, "sum_single_bin.lua"); err == nil {
				<-task.OnComplete()
			}
			return nil
		},
	}
}

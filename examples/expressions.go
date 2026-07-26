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

// Filter a query with an expression: a case-insensitive regex compare on the
// record's key.
func runExpressions() error {
	// Store the user key with each record so the expression can compare it.
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true

	if err := client.Truncate(nil, ns, set, nil); err != nil {
		return err
	}

	records := map[string]as.BinMap{
		"jacksapplekey1":  {"Jack": 26},
		"jillgrapekey2":   {"Jill": 20},
		"mangokey3":       {"James": 38},
		"Jimkey4apple":    {"Jim": 46},
		"JuliaGrapekey5":  {"Julia": 62},
		"SallyMANGOkey6":  {"Sally": 32},
		"SeanaPPlekey7":   {"Sean": 24},
		"SamGRAPEkey8":    {"Sam": 12},
		"Susankey9":       {"Susan": 42},
		"SandraPeachkey0": {"Sandra": 34},
	}
	for userKey, bins := range records {
		key, err := as.NewKey(ns, set, userKey)
		if err != nil {
			return err
		}
		if err := client.Put(writePolicy, key, bins); err != nil {
			return err
		}
	}

	// Query with a filter expression: only records whose key matches
	// ".*apple.*", ignoring case.
	queryPolicy := as.NewQueryPolicy()
	queryPolicy.FilterExpression = as.ExpRegexCompare("^.*apple.*",
		as.ExpRegexFlagICASE, as.ExpKey(as.ExpTypeSTRING))

	recordset, err := client.Query(queryPolicy, as.NewStatement(ns, set))
	if err != nil {
		return err
	}
	defer recordset.Close()

	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		log.Printf("Matched key: %v bins: %v", res.Record.Key.Value(), res.Record.Bins)
	}

	return nil
}

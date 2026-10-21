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
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	topKSize = 20
	topK     = 5
)

// Query the k records with the highest bin values. Server nodes that support
// Top-K return only their own best records; the client merges them into the
// global top k, and ranks every match itself on a mixed cluster.
func runQueryTopK() error {
	if err := createIndex("topkindex", "topkbin", as.NUMERIC); err != nil {
		return err
	}

	// Values 1..20. The top 5 descending are 20, 19, 18, 17, 16.
	for i := 1; i <= topKSize; i++ {
		key, err := as.NewKey(ns, set, "topkkey"+strconv.Itoa(i))
		if err != nil {
			return err
		}
		if err := client.Put(nil, key, as.BinMap{"topkbin": i}); err != nil {
			return err
		}
	}

	statement := as.NewStatement(ns, set, "topkbin")
	if err := statement.SetFilter(as.NewRangeFilter("topkbin", 1, 1000)); err != nil {
		return err
	}
	if err := statement.SetOrderBy("topkbin", as.BinDataTypeInteger, as.OrderDesc); err != nil {
		return err
	}
	if err := statement.SetTopK(topK); err != nil {
		return err
	}

	recordset, err := client.Query(nil, statement)
	if err != nil {
		return err
	}
	defer recordset.Close()

	// Records arrive best first.
	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		log.Printf("Top-K record: topkbin=%v", res.Record.Bins["topkbin"])
	}
	return nil
}

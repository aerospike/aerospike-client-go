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

// Query the k records with the highest bin values, breaking ties with a second
// bin. Each node returns its own best records and the client merges them into
// the global top k. Every node in the cluster must support Top-K.
func runQueryTopK() error {
	if err := createIndex("topkindex", "topkbin", as.NUMERIC); err != nil {
		return err
	}

	// Twenty records whose scores tie in pairs: records 1 and 2 score 1,
	// records 3 and 4 score 2, and so on up to records 19 and 20, which score
	// 10. Ranked by score, highest first, with ties broken by the lower id,
	// the top 5 (score, id) are (10, 19), (10, 20), (9, 17), (9, 18), (8, 15).
	for i := 1; i <= topKSize; i++ {
		key, err := as.NewKey(ns, set, "topkkey"+strconv.Itoa(i))
		if err != nil {
			return err
		}
		if err := client.Put(nil, key, as.BinMap{"topkbin": (i + 1) / 2, "topkid": i}); err != nil {
			return err
		}
	}

	statement := as.NewStatement(ns, set, "topkbin", "topkid")
	if err := statement.SetFilter(as.NewRangeFilter("topkbin", 1, 1000)); err != nil {
		return err
	}
	if err := statement.SetOrderBy("topkbin", as.BinDataTypeInteger, as.OrderDesc); err != nil {
		return err
	}
	if err := statement.ThenOrderBy("topkid", as.BinDataTypeInteger, as.OrderAsc); err != nil {
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
		log.Printf("Top-K record: topkbin=%v topkid=%v", res.Record.Bins["topkbin"], res.Record.Bins["topkid"])
	}
	return nil
}

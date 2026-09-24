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
	"errors"
	"log"
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
)

const querySize = 50

// Query records using a secondary index: a numeric-range filter and a
// string-equality filter.
func runQuery() error {
	if err := createQueryIndexes(); err != nil {
		return err
	}

	for i := 1; i <= querySize; i++ {
		key, err := as.NewKey(ns, set, "querykey"+strconv.Itoa(i))
		if err != nil {
			return err
		}
		category := "even"
		if i%2 != 0 {
			category = "odd"
		}
		if err := client.Put(nil, key, as.BinMap{"queryint": i, "querycategory": category}); err != nil {
			return err
		}
	}

	// Range query: find records with queryint between 10 and 15.
	rangeStatement := as.NewStatement(ns, set)
	rangeStatement.SetFilter(as.NewRangeFilter("queryint", 10, 15))

	rangeResults, err := client.Query(nil, rangeStatement)
	if err != nil {
		return err
	}
	defer rangeResults.Close()

	count := 0
	for res := range rangeResults.Results() {
		if res.Err != nil {
			return res.Err
		}
		count++
	}
	log.Printf("Range query (queryint in [10, 15]) found %d records", count)

	// Equality query: find records where querycategory equals "even".
	equalStatement := as.NewStatement(ns, set)
	equalStatement.SetFilter(as.NewEqualFilter("querycategory", "even"))

	equalResults, err := client.Query(nil, equalStatement)
	if err != nil {
		return err
	}
	defer equalResults.Close()

	count = 0
	for res := range equalResults.Results() {
		if res.Err != nil {
			return res.Err
		}
		count++
	}
	log.Printf("Equality query (querycategory = \"even\") found %d records", count)

	return nil
}

// createQueryIndexes creates the secondary indexes the queries filter on.
// Queries only work on indexed bins; an index only needs to be created once.
func createQueryIndexes() error {
	if err := createIndex("query_int_idx", "queryint", as.NUMERIC); err != nil {
		return err
	}
	return createIndex("query_category_idx", "querycategory", as.STRING)
}

func createIndex(indexName, binName string, indexType as.IndexType) error {
	task, err := client.CreateIndex(nil, ns, set, indexName, binName, indexType)
	if err != nil {
		if errors.Is(err, &as.AerospikeError{ResultCode: ast.INDEX_FOUND}) {
			return nil
		}
		return err
	}
	return <-task.OnComplete()
}

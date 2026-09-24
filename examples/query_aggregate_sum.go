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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Sum a single bin across a set with a map/reduce stream UDF.
func runQueryAggregateSum() error {
	as.SetLuaPath(luaPath)

	regTask, err := client.RegisterUDFFromFile(nil, luaPath+"sum_single_bin.lua", "sum_single_bin.lua", as.LUA)
	if err != nil {
		return err
	}
	if err := <-regTask.OnComplete(); err != nil {
		return err
	}

	// Seed records 1..N; the expected sum of bin1 is N*(N+1)/2.
	sum := 0
	for i := 1; i <= aggregateKeyCount; i++ {
		sum += i
		key, err := as.NewKey(ns, set, i)
		if err != nil {
			return err
		}
		if err := client.PutBins(nil, key, as.NewBin("bin1", i)); err != nil {
			return err
		}
	}

	begin := time.Now()
	statement := as.NewStatement(ns, set)
	recordset, err := client.QueryAggregate(nil, statement, "sum_single_bin", "sum_single_bin", as.StringValue("bin1"))
	if err != nil {
		return err
	}
	defer recordset.Close()

	for rec := range recordset.Results() {
		if rec.Err != nil {
			return rec.Err
		}
		log.Printf("Result %v should equal %d", rec.Record.Bins["SUCCESS"], sum)
	}
	log.Println("Map/Reduce took", time.Since(begin))
	return nil
}

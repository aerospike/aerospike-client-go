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

const aggregateKeyCount = 1000

// Compute an average over a bin with a map/reduce stream UDF. The Lua module
// runs on the server for the map phase and on the client for the reduce
// phase, so the .lua file must be available locally via SetLuaPath.
func runQueryAggregateAverage() error {
	as.SetLuaPath(luaPath)

	regTask, err := client.RegisterUDFFromFile(nil, luaPath+"average.lua", "average.lua", as.LUA)
	if err != nil {
		return err
	}
	if err := <-regTask.OnComplete(); err != nil {
		return err
	}

	// Seed records 1..N; the expected average of bin1 is (N+1)/2.
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
	average := float64(sum) / float64(aggregateKeyCount)

	begin := time.Now()
	statement := as.NewStatement(ns, set)
	recordset, err := client.QueryAggregate(nil, statement, "average", "average", as.StringValue("bin1"))
	if err != nil {
		return err
	}
	defer recordset.Close()

	for rec := range recordset.Results() {
		if rec.Err != nil {
			return rec.Err
		}
		result := rec.Record.Bins["SUCCESS"].(map[any]any)
		log.Printf("Result from Map/Reduce: %v", result)
		log.Printf("Result %f should equal %f", result["sum"].(float64)/result["count"].(float64), average)
	}
	log.Println("Map/Reduce took", time.Since(begin))
	return nil
}

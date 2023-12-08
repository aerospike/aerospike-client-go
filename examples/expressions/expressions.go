/*
 * Copyright 2014-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
	"strings"

	as "github.com/aerospike/aerospike-client-go/v7"
	shared "github.com/aerospike/aerospike-client-go/v7/examples/shared"
)

func main() {
	testKeyRegex(shared.Client)

	log.Println("Example finished successfully.")
}

/**
 * Use an expression to regex compare on a key
 */
func testKeyRegex(client *as.Client) {
	wp := as.NewWritePolicy(0, 0)
	wp.SendKey = true

	if err := client.Truncate(nil, *shared.Namespace, *shared.Set, nil); err != nil {
		log.Fatalln(err.Error())
	}

	putBins(wp, "jacksapplekey1", as.BinMap{"Jack": 26})
	putBins(wp, "jillgrapekey2", as.BinMap{"Jill": 20})
	putBins(wp, "mangokey3", as.BinMap{"James": 38})
	putBins(wp, "Jimkey4apple", as.BinMap{"Jim": 46})
	putBins(wp, "JuliaGrapekey5", as.BinMap{"Julia": 62})
	putBins(wp, "SallyMANGOkey6", as.BinMap{"Sally": 32})
	putBins(wp, "SeanaPPlekey7", as.BinMap{"Sean": 24})
	putBins(wp, "SamGRAPEkey8", as.BinMap{"Sam": 12})
	putBins(wp, "Susankey9", as.BinMap{"Susan": 42})
	putBins(wp, "SandraPeachkey0", as.BinMap{"Sandra": 34})

	stmt := as.NewStatement(*shared.Namespace, *shared.Set)
	exp := as.ExpRegexCompare("^.*apple.*", as.ExpRegexFlagICASE, as.ExpKey(as.ExpTypeSTRING))

	qp := as.NewQueryPolicy()
	qp.FilterExpression = exp
	rs, err := client.Query(qp, stmt)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for res := range rs.Results() {
		if res.Err != nil {
			log.Fatalln(err.Error())
		}
		log.Println(res.Record)
		if !strings.Contains(strings.ToLower(res.Record.Key.Value().GetObject().(string)), "apple") {
			log.Fatalf("Wrong key returned: %s. Expected to include 'apple' (case-insensitive)", res.Record.Key.Value())
		}
	}
}

func putBins(wp *as.WritePolicy, akey string, bins as.BinMap) {
	key, err := as.NewKey(*shared.Namespace, *shared.Set, akey)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if err = shared.Client.Put(wp, key, bins); err != nil {
		log.Fatalln(err.Error())
	}
}

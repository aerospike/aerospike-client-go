// Copyright 2014-2019 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/examples/shared"
)

func main() {
	prepareExample(shared.Client)
	runExample(shared.Client)

	log.Println("Example finished successfully.")
}

func prepareExample(client *as.Client) {
	// The Data
	bins := []as.BinMap{
		{
			"name":     "Bike Shop",
			"demand":   17923,
			"capacity": 17,
			"coord":    as.GeoJSONValue(`{"type" : "Point", "coordinates": [13.009318762,80.003157854]}`),
		},
		{
			"name":     "Residential Block",
			"demand":   2429,
			"capacity": 2974,
			"coord":    as.GeoJSONValue(`{"type" : "Point", "coordinates": [13.00961276, 80.003422154]}`),
		},
		{
			"name":     "Restaurant",
			"demand":   49589,
			"capacity": 4231,
			"coord":    as.GeoJSONValue(`{"type" : "Point", "coordinates": [13.009318762,80.003157854]}`),
		},
		{
			"name":     "Cafe",
			"demand":   247859,
			"capacity": 26,
			"coord":    as.GeoJSONValue(`{"type" : "Point", "coordinates": [13.00961276, 80.003422154]}`),
		},
	}

	// write the records to the database
	for i, b := range bins {
		// define some bins
		key, _ := as.NewKey(*shared.Namespace, *shared.Set, i)
		err := client.Put(nil, key, b)
		shared.PanicOnError(err)
	}

	log.Println("Sample records were written to the disk...")

	// queries only work on indices; you should create the index only once
	// The index is created on the namespace, set and bin that should be indexed.
	client.CreateIndex(nil, *shared.Namespace, *shared.Set, "testset_geo_index", "coord", as.GEO2DSPHERE)
}

func runExample(client *as.Client) {
	stm := as.NewStatement(*shared.Namespace, *shared.Set)
	// there are multiple different types of filters. You can find the list in the docs.
	stm.SetFilter(as.NewGeoWithinRadiusFilter("coord", float64(13.009318762), float64(80.003157854), float64(50000)))
	recordset, err := client.Query(nil, stm)
	shared.PanicOnError(err)

	count := 0
	for res := range recordset.Results() {
		shared.PanicOnError(res.Err)
		log.Println(res.Record.Bins)
		count++
	}

	// 4 region should be found
	log.Println("Records found:", count)
}

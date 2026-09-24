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

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
)

// Store GeoJSON points, index them, and query the ones within a radius.
func runGeoJSONQuery() error {
	if err := prepareGeoJSON(); err != nil {
		return err
	}

	statement := as.NewStatement(ns, set)
	// There are multiple different geo filters; this one matches points
	// within a radius (in meters) of a coordinate.
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
		log.Println(res.Record.Bins)
		count++
	}

	// All 4 points lie within the radius.
	log.Println("Records found:", count)
	return nil
}

// prepareGeoJSON writes sample records with GeoJSON point bins and creates
// the geo index the query needs.
func prepareGeoJSON() error {
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
	for i, b := range bins {
		key, err := as.NewKey(ns, set, i)
		if err != nil {
			return err
		}
		if err := client.Put(nil, key, b); err != nil {
			return err
		}
	}
	log.Println("Sample records were written...")

	// Geo queries need a GEO2DSPHERE index on the bin. Create it once and
	// wait until it is built; an already existing index is fine.
	task, err := client.CreateIndex(nil, ns, set, "testset_geo_index", "coord", as.GEO2DSPHERE)
	if err != nil {
		if errors.Is(err, &as.AerospikeError{ResultCode: ast.INDEX_FOUND}) {
			return nil
		}
		return err
	}
	return <-task.OnComplete()
}

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
	knnRecords = 20
	knnTopK    = 5
)

// Find the k nearest neighbours of a query vector. Each record stores a
// native vector bin; the query projects each record's distance to the query
// vector into a bin with a read expression, then keeps the k best by that bin
// (KNN = ORDER BY distance LIMIT k).
func runQueryVectorKNN() error {
	// Record i holds the vector (i, i, i, i), so the neighbours are easy to
	// check by eye.
	for i := 0; i < knnRecords; i++ {
		key, err := as.NewKey(ns, set, "knnkey"+strconv.Itoa(i))
		if err != nil {
			return err
		}
		vector := as.NewVectorFloat32([]float32{float32(i), float32(i), float32(i), float32(i)})
		if err := client.PutBins(nil, key, as.NewBin("id", i), as.NewBin("vec", vector)); err != nil {
			return err
		}
	}

	// Squared Euclidean distance: smaller is nearer, so ascending.
	// Nearest to (7.3, 7.3, 7.3, 7.3) are ids 7, 8, 6, 9, 5.
	if err := queryNearest(as.VectorEuclideanSquared, []float32{7.3, 7.3, 7.3, 7.3}, as.OrderAsc); err != nil {
		return err
	}

	// Dot product: larger is better, so descending.
	// Largest with (1, 1, 1, 1) are ids 19, 18, 17, 16, 15.
	return queryNearest(as.VectorDotProduct, []float32{1, 1, 1, 1}, as.OrderDesc)
}

// queryNearest projects the distance to query into "dist" and returns the
// knnTopK best records, best first.
func queryNearest(distance as.VectorDistanceMetric, query []float32, direction as.OrderDirection) error {
	statement := as.NewStatement(ns, set)
	statement.Operations = []*as.Operation{
		as.ExpReadOp("dist",
			as.ExpVectorDistance(distance, as.NewVectorFloat32(query), as.ExpVectorBin("vec")),
			as.ExpReadFlagDefault),
		as.GetBinOp("id"),
	}
	if err := statement.SetOrderBy("dist", as.BinDataTypeDouble, direction); err != nil {
		return err
	}
	if err := statement.SetTopK(knnTopK); err != nil {
		return err
	}

	recordset, err := client.Query(nil, statement)
	if err != nil {
		return err
	}
	defer recordset.Close()

	rank := 0
	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		rank++
		log.Printf("#%d: id=%v dist=%.4f", rank, res.Record.Bins["id"], res.Record.Bins["dist"])
	}
	return nil
}

/*
 * Copyright 2014-2021 Aerospike, Inc.
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
	"time"

	as "github.com/aerospike/aerospike-client-go/v4"
	shared "github.com/aerospike/aerospike-client-go/v4/examples/shared"
)

func main() {
	runExample(shared.Client)

	log.Println("Example finished successfully.")
}

func runExample(client *as.Client) {
	log.Printf("Scan parallel: namespace=" + *shared.Namespace + " set=" + *shared.Set)
	recordCount := 0
	begin := time.Now()
	policy := as.NewScanPolicy()
	policy.MaxRecords = 30

	pf := as.NewPartitionFilterAll()

	receivedRecords := 1
	for receivedRecords > 0 {
		receivedRecords = 0

		log.Println("Scanning Page:", recordCount/int(policy.MaxRecords))
		recordset, err := client.ScanPartitions(policy, pf, *shared.Namespace, *shared.Set)
		shared.PanicOnError(err)

		for rec := range recordset.Results() {
			if rec.Err != nil {
				// if there was an error, stop
				shared.PanicOnError(err)
			}

			recordCount++
			receivedRecords++
		}
	}

	log.Println("Total records returned: ", recordCount)
	log.Println("Elapsed time: ", time.Since(begin), " seconds")
}

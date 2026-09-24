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

// Scan a set page by page: a partition filter carries the scan position
// across calls, and MaxRecords limits each page.
func runScanPaginate() error {
	recordCount := 0
	begin := time.Now()

	policy := as.NewScanPolicy()
	policy.MaxRecords = 30

	// The partition filter carries the position across calls; IsDone reports
	// true once every partition has been scanned.
	partitionFilter := as.NewPartitionFilterAll()

	for !partitionFilter.IsDone() {
		log.Println("Scanning Page:", recordCount/int(policy.MaxRecords))

		recordset, err := client.ScanPartitions(policy, partitionFilter, ns, set)
		if err != nil {
			return err
		}
		for res := range recordset.Results() {
			if res.Err != nil {
				recordset.Close()
				return res.Err
			}
			recordCount++
		}
		recordset.Close()
	}

	log.Println("Total records returned: ", recordCount)
	log.Println("Elapsed time: ", time.Since(begin))
	return nil
}

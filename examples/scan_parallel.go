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
	"math"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Scan all cluster nodes in parallel and stream the combined results.
func runScanParallel() error {
	recordCount := 0
	begin := time.Now()

	policy := as.NewScanPolicy()
	recordset, err := client.ScanAll(policy, ns, set)
	if err != nil {
		return err
	}
	defer recordset.Close()

	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		recordCount++
		if (recordCount % 100000) == 0 {
			log.Println("Records ", recordCount)
		}
	}

	seconds := float64(time.Since(begin)) / float64(time.Second)
	log.Println("Total records returned: ", recordCount)
	log.Println("Elapsed time: ", seconds, " seconds")
	log.Println("Records/second: ", math.Round(float64(recordCount)/seconds))
	return nil
}

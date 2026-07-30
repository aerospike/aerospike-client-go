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

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Scan a set one cluster node at a time, limiting the records read per
// second to reduce load on the server.
func runScanSerial() error {
	policy := as.NewScanPolicy()
	policy.RecordsPerSecond = 5000

	total := 0
	for _, node := range client.GetNodes() {
		recordset, err := client.ScanNode(policy, node, ns, set)
		if err != nil {
			return err
		}

		count := 0
		for res := range recordset.Results() {
			if res.Err != nil {
				recordset.Close()
				return res.Err
			}
			count++
		}
		recordset.Close()

		log.Printf("Node %s: %d records", node.GetName(), count)
		total += count
	}

	log.Printf("Scanned %d records from %s.%s", total, ns, set)
	return nil
}

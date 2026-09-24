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

// Write a record with multiple bins.
func runPut() error {
	key, err := as.NewKey(ns, set, "putkey")
	if err != nil {
		return err
	}

	bins := as.BinMap{
		"bin1": "value1",
		"bin2": "value2",
	}

	// Write the record using the default write policy.
	if err := client.Put(nil, key, bins); err != nil {
		return err
	}

	log.Printf("Wrote record %v to key %v", bins, key.Value())
	return nil
}

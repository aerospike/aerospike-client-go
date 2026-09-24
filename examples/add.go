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

// Atomically increment an integer bin on the server.
func runAdd() error {
	key, err := as.NewKey(ns, set, "addkey")
	if err != nil {
		return err
	}

	// The initial add creates the record with value 10.
	if err := client.AddBins(nil, key, as.NewBin("addbin", 10)); err != nil {
		return err
	}

	// Add 5 to the existing value.
	if err := client.AddBins(nil, key, as.NewBin("addbin", 5)); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "addbin")
	if err != nil {
		return err
	}
	log.Printf("addbin after two adds: %v", record.Bins["addbin"])

	// Add and read back in a single operate call.
	record, err = client.Operate(nil, key, as.AddOp(as.NewBin("addbin", 30)), as.GetOp())
	if err != nil {
		return err
	}
	log.Printf("addbin after operate add: %v", record.Bins["addbin"])

	return nil
}

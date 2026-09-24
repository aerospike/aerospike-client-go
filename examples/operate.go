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

// Combine multiple operations on one record in a single atomic call.
func runOperate() error {
	key, err := as.NewKey(ns, set, "opkey")
	if err != nil {
		return err
	}

	// Write the initial record.
	if err := client.PutBins(nil, key, as.NewBin("optintbin", 7), as.NewBin("optstringbin", "string value")); err != nil {
		return err
	}

	// Add to the integer bin, overwrite the string bin and read the record
	// back — all in one atomic operate call.
	record, err := client.Operate(nil, key,
		as.AddOp(as.NewBin("optintbin", 4)),
		as.PutOp(as.NewBin("optstringbin", "new string")),
		as.GetOp())
	if err != nil {
		return err
	}

	log.Printf("Record after operate: %v", record.Bins)
	return nil
}

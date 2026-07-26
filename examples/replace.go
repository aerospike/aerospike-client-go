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

// Replace a record, discarding its previous bins, and demonstrate the
// replace-only policy on a missing record.
func runReplace() error {
	key, err := as.NewKey(ns, set, "replacekey")
	if err != nil {
		return err
	}

	// Write a record with two bins.
	if err := client.PutBins(nil, key, as.NewBin("bin1", "value1"), as.NewBin("bin2", "value2")); err != nil {
		return err
	}

	// REPLACE discards all existing bins: only bin3 remains afterwards.
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.RecordExistsAction = as.REPLACE
	if err := client.PutBins(writePolicy, key, as.NewBin("bin3", "value3")); err != nil {
		return err
	}

	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record after replace: %v", record.Bins)

	// REPLACE_ONLY requires the record to exist: writing a missing record
	// returns a key-not-found error.
	missingKey, err := as.NewKey(ns, set, "replaceonlykey")
	if err != nil {
		return err
	}
	writePolicy.RecordExistsAction = as.REPLACE_ONLY
	err = client.PutBins(writePolicy, missingKey, as.NewBin("bin", "value"))
	if !errors.Is(err, &as.AerospikeError{ResultCode: ast.KEY_NOT_FOUND_ERROR}) {
		return errors.New("replace-only write should have returned a key-not-found error")
	}
	log.Printf("Replace-only on a missing record returned key-not-found, as expected.")

	return nil
}

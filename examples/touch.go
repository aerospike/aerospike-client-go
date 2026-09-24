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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
)

// Extend a record's time-to-live with a touch operation.
func runTouch() error {
	key, err := as.NewKey(ns, set, "touchkey")
	if err != nil {
		return err
	}

	// Create the record with a 2 second expiration.
	writePolicy := as.NewWritePolicy(0, 2)
	if err := client.PutBins(writePolicy, key, as.NewBin("touchbin", "touchvalue")); err != nil {
		return err
	}

	// Touch the record, resetting its expiration to 5 seconds, and read the
	// new metadata back in the same call.
	writePolicy.Expiration = 5
	record, err := client.Operate(writePolicy, key, as.TouchOp(), as.GetHeaderOp())
	if err != nil {
		return err
	}
	log.Printf("After touch: generation=%d expiration=%d", record.Generation, record.Expiration)

	// After 3 seconds the record still exists — the original 2 second
	// expiration no longer applies.
	log.Printf("Sleeping 3 seconds ...")
	time.Sleep(3 * time.Second)
	record, err = client.Get(nil, key, "touchbin")
	if err != nil {
		return err
	}
	log.Printf("Record still exists after 3s: touchbin=%v", record.Bins["touchbin"])

	// After 4 more seconds the touched expiration has passed and the record
	// is gone.
	log.Printf("Sleeping 4 seconds ...")
	time.Sleep(4 * time.Second)
	_, err = client.Get(nil, key, "touchbin")
	if !errors.Is(err, &as.AerospikeError{ResultCode: ast.KEY_NOT_FOUND_ERROR}) {
		return errors.New("record should have expired after the touched TTL")
	}
	log.Printf("Record expired, as expected.")

	return nil
}

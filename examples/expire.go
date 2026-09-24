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
	"math"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
)

// Control record time-to-live: write a record that expires and a record that
// never expires.
func runExpire() error {
	key, err := as.NewKey(ns, set, "expirekey")
	if err != nil {
		return err
	}

	// Write a record that expires 2 seconds after the write.
	writePolicy := as.NewWritePolicy(0, 2)
	if err := client.PutBins(writePolicy, key, as.NewBin("expirebin", "expirevalue")); err != nil {
		return err
	}

	// Reading before expiration succeeds.
	record, err := client.Get(nil, key, "expirebin")
	if err != nil {
		return err
	}
	log.Printf("Before expiration: expirebin=%v", record.Bins["expirebin"])

	// After expiration the record is gone.
	log.Printf("Sleeping 3 seconds ...")
	time.Sleep(3 * time.Second)
	_, err = client.Get(nil, key, "expirebin")
	if !errors.Is(err, &as.AerospikeError{ResultCode: ast.KEY_NOT_FOUND_ERROR}) {
		return errors.New("record should have expired")
	}
	log.Printf("Record expired, as expected.")

	// Write a record that never expires (TTL -1).
	writePolicy.Expiration = math.MaxUint32
	if err := client.PutBins(writePolicy, key, as.NewBin("expirebin", "noexpirevalue")); err != nil {
		return err
	}

	// Read it back, showing it is there.
	record, err = client.Get(nil, key, "expirebin")
	if err != nil {
		return err
	}
	log.Printf("No-expire record: expirebin=%v", record.Bins["expirebin"])

	// Even after waiting well past any default namespace TTL, the record is
	// still there.
	log.Printf("Sleeping 10 seconds ...")
	time.Sleep(10 * time.Second)
	record, err = client.Get(nil, key, "expirebin")
	if err != nil {
		return err
	}
	log.Printf("Found record (correctly) after waiting: expirebin=%v", record.Bins["expirebin"])

	return nil
}

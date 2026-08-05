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
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Write records individually, then check, read and inspect them in single
// batch requests.
func runBatch() error {
	const (
		keyPrefix   = "batchkey"
		valuePrefix = "batchvalue"
		binName     = "batchbin"
		size        = 8
	)

	// Write records individually.
	for i := 1; i <= size; i++ {
		key, err := as.NewKey(ns, set, keyPrefix+strconv.Itoa(i))
		if err != nil {
			return err
		}
		if err := client.PutBins(nil, key, as.NewBin(binName, valuePrefix+strconv.Itoa(i))); err != nil {
			return err
		}
	}

	keys := make([]*as.Key, size)
	for i := range keys {
		key, err := as.NewKey(ns, set, keyPrefix+strconv.Itoa(i+1))
		if err != nil {
			return err
		}
		keys[i] = key
	}

	// Check the existence of all keys in one batch call.
	existsArray, err := client.BatchExists(nil, keys)
	if err != nil {
		return err
	}
	log.Printf("exists: %v", existsArray)

	// Read one bin of all records in one batch call.
	records, err := client.BatchGet(nil, keys, binName)
	if err != nil {
		return err
	}
	for i, record := range records {
		if record == nil {
			log.Printf("%v: not found", keys[i].Value())
			continue
		}
		log.Printf("%v: %s=%v", keys[i].Value(), binName, record.Bins[binName])
	}

	// Read record metadata (generation, expiration) in one batch call.
	headers, err := client.BatchGetHeader(nil, keys)
	if err != nil {
		return err
	}
	for i, header := range headers {
		if header == nil {
			log.Printf("%v: not found", keys[i].Value())
			continue
		}
		log.Printf("%v: generation=%d expiration=%d", keys[i].Value(), header.Generation, header.Expiration)
	}

	return nil
}

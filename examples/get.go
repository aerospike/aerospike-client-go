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

// Read a record: the whole record, selected bins, its metadata, or check bare
// existence.
func runGet() error {
	key, err := as.NewKey(ns, set, "getkey")
	if err != nil {
		return err
	}

	// Read the whole record.
	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record: %v", record.Bins)

	// Read selected bins only.
	record, err = client.Get(nil, key, "bin1")
	if err != nil {
		return err
	}
	log.Printf("bin1: %v", record.Bins["bin1"])

	// Read record metadata (generation and TTL) without any bins.
	header, err := client.GetHeader(nil, key)
	if err != nil {
		return err
	}
	log.Printf("generation=%d expiration=%d", header.Generation, header.Expiration)

	return nil
}

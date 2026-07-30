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

// Append to a string bin on the server.
func runAppend() error {
	key, err := as.NewKey(ns, set, "appendkey")
	if err != nil {
		return err
	}

	// The initial append creates the record.
	if err := client.AppendBins(nil, key, as.NewBin("appendbin", "Hello")); err != nil {
		return err
	}

	// Append to the existing value.
	if err := client.AppendBins(nil, key, as.NewBin("appendbin", " World")); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "appendbin")
	if err != nil {
		return err
	}
	log.Printf("appendbin: %v", record.Bins["appendbin"])

	return nil
}

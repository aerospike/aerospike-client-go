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

// Use the record generation (version) counter for optimistic concurrency:
// a write succeeds only when the record has not changed since it was read.
func runGeneration() error {
	key, err := as.NewKey(ns, set, "genkey")
	if err != nil {
		return err
	}

	// Write the record twice; each write increments its generation.
	if err := client.PutBins(nil, key, as.NewBin("genbin", "genvalue1")); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("genbin", "genvalue2")); err != nil {
		return err
	}

	// Read the record to learn its current generation.
	record, err := client.Get(nil, key, "genbin")
	if err != nil {
		return err
	}
	log.Printf("genbin=%v generation=%d", record.Bins["genbin"], record.Generation)

	// Write only if the generation still matches: succeeds.
	writePolicy := as.NewWritePolicy(0, 2)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.Generation = record.Generation
	if err := client.PutBins(writePolicy, key, as.NewBin("genbin", "genvalue3")); err != nil {
		return err
	}

	// Write with a stale generation: the server rejects it.
	writePolicy.Generation = 9999
	err = client.PutBins(writePolicy, key, as.NewBin("genbin", "genvalue4"))
	if !errors.Is(err, &as.AerospikeError{ResultCode: ast.GENERATION_ERROR}) {
		return errors.New("write with a stale generation should have returned a generation error")
	}
	log.Printf("Write with stale generation rejected, as expected.")

	return nil
}

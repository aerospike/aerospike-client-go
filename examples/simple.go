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

// A complete record lifecycle: write, read, update, and delete.
func runSimple() error {
	key, err := as.NewKey(ns, set, "simplekey")
	if err != nil {
		return err
	}

	// Write a record; bin values can be any supported type.
	bins := as.BinMap{
		"bin1": 42,
		"bin2": "An elephant is a mouse with an operating system",
		"bin3": []any{"Go", 17981},
	}
	if err := client.Put(nil, key, bins); err != nil {
		return err
	}

	// Read it back.
	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record: %v", record.Bins)

	// Update: increment an integer bin and read it back.
	if err := client.Add(nil, key, as.BinMap{"bin1": 1}); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("bin1 after add: %v", record.Bins["bin1"])

	// Update: prepend and append to a string bin and read it back.
	if err := client.Prepend(nil, key, as.BinMap{"bin2": "Frankly:  "}); err != nil {
		return err
	}
	if err := client.Append(nil, key, as.BinMap{"bin2": "."}); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("bin2 after prepend/append: %v", record.Bins["bin2"])

	// Update: delete one bin by writing nil, and read the record back.
	if err := client.Put(nil, key, as.BinMap{"bin3": nil}); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record after deleting bin3: %v", record.Bins)

	// Check existence, delete the record, and check again.
	exists, err := client.Exists(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record exists: %t", exists)

	existed, err := client.Delete(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Deleted record (existed=%t)", existed)

	exists, err = client.Exists(nil, key)
	if err != nil {
		return err
	}
	log.Printf("Record exists after delete: %t", exists)

	return nil
}

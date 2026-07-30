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

// Person is a custom type stored as a blob. Implementing EncodeBlob (the
// as.AerospikeBlob interface) lets the client serialize it automatically.
type Person struct {
	Name string
}

// EncodeBlob implements the as.AerospikeBlob interface.
func (p Person) EncodeBlob() ([]byte, error) {
	return []byte(p.Name), nil
}

// DecodeBlob restores a Person from its blob form.
func (p *Person) DecodeBlob(buf []byte) error {
	p.Name = string(buf)
	return nil
}

// Store and retrieve a custom type as a blob bin.
func runBlob() error {
	key, err := as.NewKey(ns, set, "blobkey")
	if err != nil {
		return err
	}

	// The client serializes Person via its EncodeBlob method. Because
	// EncodeBlob has a value receiver, values and pointers both satisfy the
	// interface.
	bins := as.BinMap{
		"bin1": Person{Name: "Albert Einstein"},
		"bin2": &Person{Name: "Richard Feynman"},
	}
	if err := client.Put(nil, key, bins); err != nil {
		return err
	}

	// Blobs come back as []byte; decode manually.
	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	for _, bin := range []string{"bin1", "bin2"} {
		person := &Person{}
		if err := person.DecodeBlob(record.Bins[bin].([]byte)); err != nil {
			return err
		}
		log.Printf("Decoded %s: %+v", bin, person)
	}

	return nil
}

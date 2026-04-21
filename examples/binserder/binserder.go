/*
 * Copyright 2014-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// This example demonstrates the zero-reflection read path backed by the
// BinSerDer interface. A user type that implements BinSerDer (and,
// optionally, BinSerDerMeta) can be populated from an Aerospike record
// without any reflection on the struct — the client asks the type
// which bins to request and hands each decoded bin value back via
// UnmarshalBin.
package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
	shared "github.com/aerospike/aerospike-client-go/v8/examples/shared"
)

// Person is a plain user struct. It implements BinSerDer to opt into
// the zero-reflection read path, and BinSerDerMeta to additionally
// receive the record's generation and TTL after the bins have been
// decoded.
type Person struct {
	Name string
	Age  int
	Bio  string

	// Populated by SetAerospikeMeta after all bins are decoded.
	Generation uint32
	TTL        uint32
}

// personBinNames is cached at package scope so every AerospikeBinNames
// call reuses the same backing array — no per-read allocation.
var personBinNames = []string{"name", "age", "bio"}

// AerospikeBinNames tells the client which bins to request for a
// Person. The returned slice is used verbatim; it must not be mutated.
func (p *Person) AerospikeBinNames() []string {
	return personBinNames
}

// UnmarshalBin is invoked once per returned bin. It dispatches on the
// bin name and assigns the correctly-typed value to the matching
// field. Unknown bin names are silently ignored (return nil), which
// lets a Person coexist with records that carry extra bins.
func (p *Person) UnmarshalBin(name string, value any) as.Error {
	if value == nil {
		return nil
	}
	switch name {
	case "name":
		if v, ok := value.(string); ok {
			p.Name = v
		}
	case "age":
		if v, ok := value.(int); ok {
			p.Age = v
		}
	case "bio":
		if v, ok := value.(string); ok {
			p.Bio = v
		}
	}
	return nil
}

// SetAerospikeMeta makes Person a BinSerDerMeta. The client calls it
// exactly once per successful read, after every UnmarshalBin call has
// completed.
func (p *Person) SetAerospikeMeta(generation uint32, expiration uint32) {
	p.Generation = generation
	p.TTL = expiration
}

func main() {
	runBinSerDerExample(shared.Client)
	log.Println("Example finished successfully.")
}

func runBinSerDerExample(client *as.Client) {
	key, err := as.NewKey(*shared.Namespace, *shared.Set, "binserder-example")
	shared.PanicOnError(err)

	// Write a record with bins that match the names Person requests.
	log.Printf("Put: namespace=%s set=%s key=%s", key.Namespace(), key.SetName(), key.Value())
	putErr := client.PutBins(shared.WritePolicy, key,
		as.NewBin("name", "Ada"),
		as.NewBin("age", 37),
		as.NewBin("bio", "analyst"),
	)
	shared.PanicOnError(putErr)

	// Read it back into a Person without any reflection on the struct.
	p := &Person{}
	if getErr := client.GetObjectBinSerDer(nil, key, p); getErr != nil {
		shared.PanicOnError(getErr)
	}

	log.Printf("Got Person: name=%q age=%d bio=%q generation=%d ttl=%d",
		p.Name, p.Age, p.Bio, p.Generation, p.TTL)
}

// Copyright 2013-2014 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"fmt"
)

// Container object for records.  Records are equivalent to rows.
type Record struct {
	// Record's Key. Might be empty, or may only consist of digest value.
	Key *Key

	// Node from which the Record is originating from.
	Node *Node

	// Map of requested name/value bins.
	Bins BinMap

	// List of all duplicate records (if any) for a given key.  Duplicates are only created when
	// the server configuration option "allow-versions" is true (default is false) and client
	// RecordExistsAction.DUPLICATE policy flag is set and there is a generation error.
	// Almost always nil.
	Duplicates []BinMap

	// Record modification count.
	Generation int

	// TTL (Time-To-Live). Number of seconds until record expires.
	Expiration int
}

func newRecord(node *Node, key *Key, bins BinMap, duplicates []BinMap, generation int, expiration int) *Record {
	r := &Record{
		Node:       node,
		Key:        key,
		Bins:       bins,
		Duplicates: duplicates,
		Generation: generation,
		Expiration: expiration,
	}

	// always assign a map of length zero if Bins is nil
	if r.Bins == nil {
		r.Bins = make(BinMap, 0)
	}

	return r
}

// Return string representation of record.
func (rc *Record) String() string {
	return fmt.Sprintf("%v %v", *rc.Key, rc.Bins)
}

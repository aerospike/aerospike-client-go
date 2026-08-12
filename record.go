// Copyright 2014-2022 Aerospike, Inc.
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

import "fmt"

// Record is the container struct for database records.
// Records are equivalent to rows.
type Record struct {
	// Key is the record's key.
	// Might be empty, or may only consist of digest value.
	Key *Key

	// Node from which the Record is originating from.
	Node *Node

	// Bins is the map of requested name/value bins.
	Bins BinMap

	// OpResults holds an operate command's results in the order the server
	// returned them.
	//
	// The Bins map cannot express several operations on the same bin: it holds
	// only the last value, or an OpResults slice under that one name. This
	// slice is the positional view, so a caller can address each result even
	// when two operations touched the same bin.
	//
	// Note that only operations that *produce* a value appear here. A
	// write-only operation such as a put or a touch sends no result back, so
	// the positions line up with the returned results, not with the request.
	// It is nil for commands that are not operates.
	OpResults []any

	// Generation shows record modification count.
	Generation uint32

	// Expiration is TTL (Time-To-Live).
	// Number of seconds until record expires.
	Expiration uint32
}

func newRecord(node *Node, key *Key, bins BinMap, generation, expiration uint32) *Record {
	r := &Record{
		Node:       node,
		Key:        key,
		Bins:       bins,
		Generation: generation,
		Expiration: expiration,
	}

	// always assign a map of length zero if Bins is nil
	if r.Bins == nil {
		r.Bins = make(BinMap)
	}

	return r
}

// OperationResult returns the i-th of an operate command's returned results,
// and whether that position exists.
//
// Only operations that produce a value are represented, so the positions follow
// the returned results rather than the request: a chain of put, add and get
// yields one result, from the get. The result is absent for a command that was
// not an operate.
func (rc *Record) OperationResult(i int) (any, bool) {
	if rc == nil || i < 0 || i >= len(rc.OpResults) {
		return nil, false
	}
	return rc.OpResults[i], true
}

// String implements the Stringer interface.
// Returns string representation of record.
func (rc *Record) String() string {
	return fmt.Sprintf("%s %v", rc.Key, rc.Bins)
}

// udfError returns the the error string returned by a UDF execute in a batch.
// Returns nil if an error did not occur.
func (rc *Record) udfError() string {
	return rc.Bins["FAILURE"].(string)
}

// udfResult returns the value returned by a UDF execute in a batch.
// The result may be nil.
func (rc *Record) udfResult() BinMap {
	return BinMap(rc.Bins["SUCCESS"].(map[string]any))
}

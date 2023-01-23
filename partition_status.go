// Copyright 2014-2022 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

import "fmt"

// partitionStatus encapsulates the pagination status in partitions.
type partitionStatus struct {
	// BVal
	BVal int64
	// Id shows the partition Id.
	Id int
	// Retry signifies if the partition requires a retry.
	Retry bool
	// Digest records the digest of the last key digest received from the server
	// for this partition.
	Digest []byte

	node         *Node
	replicaIndex int
	unavailable  bool
}

func newPartitionStatus(id int) *partitionStatus {
	return &partitionStatus{Id: id, Retry: true}
}

func (ps *partitionStatus) String() string {
	r := 'F'
	if ps.Retry {
		r = 'T'
	}
	return fmt.Sprintf("%04d:%c", ps.Id, r)
}

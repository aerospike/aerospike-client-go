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

type partitionStatus struct {
	bval   int64
	id     int
	retry  bool
	digest []byte
}

func newPartitionStatus(id int) *partitionStatus {
	return &partitionStatus{id: id, retry: true}
}

func (ps *partitionStatus) String() string {
	r := 'F'
	if ps.retry {
		r = 'T'
	}
	return fmt.Sprintf("%04d:%c", ps.id, r)
}

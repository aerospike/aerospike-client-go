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

// WritePolicy encapsulates parameters for policy attributes used in write operations.
// This object is passed into methods where database writes can occur.
type WritePolicy struct {
	BasePolicy

	// RecordExistsAction qualifies how to handle writes where the record already exists.
	RecordExistsAction RecordExistsAction //= RecordExistsAction.UPDATE;

	// GenerationPolicy qualifies how to handle record writes based on record generation. The default (NONE)
	// indicates that the generation is not used to restrict writes.
	GenerationPolicy GenerationPolicy //= GenerationPolicy.NONE;

	// Generation determines expected generation.
	// Generation is the number of times a record has been
	// modified (including creation) on the server.
	// If a write operation is creating a record, the expected generation would be 0.
	Generation int32

	// Expiration determimes record expiration in seconds. Also known as TTL (Time-To-Live).
	// Seconds record will live before being removed by the server.
	// Expiration values:
	// -1: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
	// versions >= 3.1.4.  Do not use -1 for older servers.
	// 0: Default to namespace configuration variable "default-ttl" on the server.
	// > 0: Actual expiration in seconds.
	Expiration int32

	// Send user defined key in addition to hash digest on a record put.
	// The default is to not send the user defined key.
	SendKey bool
}

// NewWritePolicy initializes a new WritePolicy instance with default parameters.
func NewWritePolicy(generation, expiration int32) *WritePolicy {
	return &WritePolicy{
		BasePolicy:         *NewPolicy(),
		RecordExistsAction: UPDATE,
		GenerationPolicy:   NONE,
		Generation:         generation,
		Expiration:         expiration,
		SendKey:            false,
	}
}

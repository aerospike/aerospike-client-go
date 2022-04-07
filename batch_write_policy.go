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

// Policy attributes used in batch write commands.
type BatchWritePolicy struct {
	// FilterExpression is optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord#resultCode is set to types.FILTERED_OUT.
	//
	// Default: nil
	FilterExpression *Expression

	// RecordExistsAction qualifies how to handle writes where the record already exists.
	RecordExistsAction RecordExistsAction //= RecordExistsAction.UPDATE;

	// Desired consistency guarantee when committing a transaction on the server. The default
	// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	// be successful before returning success to the client.
	//
	// Default: CommitLevel.COMMIT_ALL
	CommitLevel CommitLevel //= COMMIT_ALL

	// GenerationPolicy qualifies how to handle record writes based on record generation. The default (NONE)
	// indicates that the generation is not used to restrict writes.
	//
	// The server does not support this field for UDF execute() calls. The read-modify-write
	// usage model can still be enforced inside the UDF code itself.
	//
	// Default: GenerationPolicy.NONE
	// indicates that the generation is not used to restrict writes.
	GenerationPolicy GenerationPolicy //= GenerationPolicy.NONE;

	// Expected generation. Generation is the number of times a record has been modified
	// (including creation) on the server. If a write operation is creating a record,
	// the expected generation would be 0. This field is only relevant when
	// generationPolicy is not NONE.
	//
	// The server does not support this field for UDF execute() calls. The read-modify-write
	// usage model can still be enforced inside the UDF code itself.
	//
	// Default: 0
	Generation uint32

	// Expiration determines record expiration in seconds. Also known as TTL (Time-To-Live).
	// Seconds record will live before being removed by the server.
	// Expiration values:
	// TTLServerDefault (0): Default to namespace configuration variable "default-ttl" on the server.
	// TTLDontExpire (MaxUint32): Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3+ server
	// TTLDontUpdate (MaxUint32 - 1): Do not change ttl when record is written. Supported by Aerospike server versions >= 3.10.1
	// > 0: Actual expiration in seconds.
	Expiration uint32

	// DurableDelete leaves a tombstone for the record if the transaction results in a record deletion.
	// This prevents deleted records from reappearing after node failures.
	// Valid for Aerospike Server Enterprise Edition 3.10+ only.
	DurableDelete bool

	// SendKey determines to whether send user defined key in addition to hash digest on both reads and writes.
	// If the key is sent on a write, the key will be stored with the record on
	// the server.
	// The default is to not send the user defined key.
	SendKey bool // = false
}

func NewBatchWritePolicy() *BatchWritePolicy {
	return &BatchWritePolicy{
		RecordExistsAction: UPDATE,
		GenerationPolicy:   NONE,
		CommitLevel:        COMMIT_ALL,
	}
}

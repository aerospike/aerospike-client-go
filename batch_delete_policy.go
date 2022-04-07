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

// BatchDeletePolicy is used in batch delete commands.
type BatchDeletePolicy struct {
	// FilterExpression is optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord.ResultCode is set to type.FILTERED_OUT.
	// Default: nil
	FilterExpression *Expression

	// Desired consistency guarantee when committing a transaction on the server. The default
	// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	// be successful before returning success to the client.
	// Default: CommitLevel.COMMIT_ALL
	CommitLevel CommitLevel //= COMMIT_ALL

	// Qualify how to handle record deletes based on record generation. The default (NONE)
	// indicates that the generation is not used to restrict deletes.
	// Default: GenerationPolicy.NONE
	GenerationPolicy GenerationPolicy //= GenerationPolicy.NONE;

	// Expected generation. Generation is the number of times a record has been modified
	// (including creation) on the server. This field is only relevant when generationPolicy
	// is not NONE.
	// Default: 0
	Generation uint32

	// If the transaction results in a record deletion, leave a tombstone for the record.
	// This prevents deleted records from reappearing after node failures.
	// Valid for Aerospike Server Enterprise Edition only.
	// Default: false (do not tombstone deleted records).
	DurableDelete bool

	// Send user defined key in addition to hash digest.
	// If true, the key will be stored with the tombstone record on the server.
	// Default: false (do not send the user defined key)
	SendKey bool
}

// NewBatchDeletePolicy returns a default BatchDeletePolicy.
func NewBatchDeletePolicy() *BatchDeletePolicy {
	return &BatchDeletePolicy{
		CommitLevel:      COMMIT_ALL,
		GenerationPolicy: NONE,
	}
}

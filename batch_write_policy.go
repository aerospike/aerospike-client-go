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

import "github.com/aerospike/aerospike-client-go/v8/logger"

// BatchWritePolicy attributes used in batch write commands.
type BatchWritePolicy struct {
	// FilterExpression is optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord#resultCode is set to types.FILTERED_OUT.
	//
	// Default: nil
	FilterExpression *Expression

	// RecordExistsAction qualifies how to handle writes where the record already exists.
	RecordExistsAction RecordExistsAction //= RecordExistsAction.UPDATE;

	// Desired consistency guarantee when committing a command on the server. The default
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

	// DurableDelete leaves a tombstone for the record if the command results in a record deletion.
	// This prevents deleted records from reappearing after node failures.
	// Valid for Aerospike Server Enterprise Edition 3.10+ only.
	DurableDelete bool

	// Execute the write command only if the record is not already locked by this transaction.
	// If this field is true and the record is already locked by this transaction, the command
	// will return an error with the [types.MRT_ALREADY_LOCKED] error code.
	//
	// This field is useful for safely retrying non-idempotent writes as an alternative to simply
	// aborting the transaction.
	//
	// Default: false
	OnLockingOnly bool

	// SendKey determines to whether send user defined key in addition to hash digest on both reads and writes.
	// If the key is sent on a write, the key will be stored with the record on
	// the server.
	// The default is to not send the user defined key.
	SendKey bool // = false
}

// NewBatchWritePolicy returns a policy instance for BatchWrite commands.
func NewBatchWritePolicy() *BatchWritePolicy {
	return &BatchWritePolicy{
		RecordExistsAction: UPDATE,
		GenerationPolicy:   NONE,
		CommitLevel:        COMMIT_ALL,
	}
}

func (bwp *BatchWritePolicy) toWritePolicy(bp *BatchPolicy, dynConfig *DynConfig) *WritePolicy {
	wp := bp.toWritePolicy()

	if dynConfig != nil {
		wp.BasePolicy = *dynConfig.client.dynDefaultBatchWriteBasePolicy.Load()
	}

	if bwp != nil {
		if bwp.FilterExpression != nil {
			wp.FilterExpression = bwp.FilterExpression
		}
		wp.RecordExistsAction = bwp.RecordExistsAction
		wp.CommitLevel = bwp.CommitLevel
		wp.GenerationPolicy = bwp.GenerationPolicy
		wp.Generation = bwp.Generation
		wp.Expiration = bwp.Expiration
		wp.DurableDelete = bwp.DurableDelete
		wp.SendKey = bwp.SendKey
	}

	return wp
}

// copy creates a new BasePolicy instance and copies the values from the source BasePolicy.
func (bwp *BatchWritePolicy) copy() *BatchWritePolicy {
	if bwp == nil {
		return nil
	}

	response := *bwp
	return &response
}

// applyConfigToQueryPolicy applies the dynamic configuration and generates a new policy
func (bwp *BatchWritePolicy) patchDynamic(dynConfig *DynConfig) *BatchWritePolicy {
	if dynConfig == nil {
		return bwp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if bwp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultBatchWritePolicy.Load()
	}
	if config != nil && config.Dynamic != nil && config.Dynamic.BatchWrite != nil {
		// Dynamic configuration is exists for policy in question.
		// User has provided a custom policy. We need to apply the dynamic configuration.
		return bwp.copy().mapDynamic(dynConfig)
	} else {
		return bwp
	}
}

func (bwp *BatchWritePolicy) mapDynamic(dynConfig *DynConfig) *BatchWritePolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return bwp
	}

	if currentConfig.Dynamic.BatchWrite != nil {
		if currentConfig.Dynamic.BatchWrite.DurableDelete != nil {
			configValue := *currentConfig.Dynamic.BatchWrite.DurableDelete
			bwp.DurableDelete = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("DurableDelete set to %t", configValue)
			}
		}
		if currentConfig.Dynamic.BatchWrite.SendKey != nil {
			configValue := *currentConfig.Dynamic.BatchWrite.SendKey
			bwp.SendKey = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SendKey set to %t", configValue)
			}
		}
	}

	return bwp
}

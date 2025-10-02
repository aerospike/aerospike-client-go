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

// BatchDeletePolicy is used in batch delete commands.
type BatchDeletePolicy struct {
	// FilterExpression is optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord.ResultCode is set to type.FILTERED_OUT.
	// Default: nil
	FilterExpression *Expression

	// Desired consistency guarantee when committing a command on the server. The default
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

	// If the command results in a record deletion, leave a tombstone for the record.
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

func (bdp *BatchDeletePolicy) toWritePolicy(bp *BatchPolicy, dynConfig *DynConfig) *WritePolicy {
	wp := bp.toWritePolicy()

	if bdp != nil {
		if bdp.FilterExpression != nil {
			wp.FilterExpression = bdp.FilterExpression
		}
		wp.CommitLevel = bdp.CommitLevel
		wp.GenerationPolicy = bdp.GenerationPolicy
		wp.Generation = bdp.Generation
		wp.DurableDelete = bdp.DurableDelete
		wp.SendKey = bdp.SendKey
	}

	// In Case dynConfig is not initialized or running return the policy before
	// merge
	if dynConfig == nil {
		return wp
	}

	config := dynConfig.config
	if config != nil && config.Dynamic != nil && config.Dynamic.BatchDelete != nil {
		if config.Dynamic.BatchDelete.DurableDelete != nil {
			wp.DurableDelete = *config.Dynamic.BatchDelete.DurableDelete
		}
		if config.Dynamic.BatchDelete.SendKey != nil {
			wp.SendKey = *config.Dynamic.BatchDelete.SendKey
		}
	}

	return wp
}

// copy creates a new BasePolicy instance and copies the values from the source BatchDeletePolicy.
func (bd *BatchDeletePolicy) copy() *BatchDeletePolicy {
	if bd == nil {
		return nil
	}

	response := *bd
	return &response
}

// patchDynamic applies the dynamic configuration and generates a new policy
func (bdp *BatchDeletePolicy) patchDynamic(dynConfig *DynConfig) *BatchDeletePolicy {
	if dynConfig == nil {
		return bdp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if bdp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultBatchDeletePolicy.Load()
	}
	if config != nil && config.Dynamic != nil && config.Dynamic.BatchDelete != nil {
		// Dynamic configuration is exists for policy in question.
		// User has provided a custom policy. We need to apply the dynamic configuration.
		return bdp.copy().mapDynamic(dynConfig)
	} else {
		return bdp
	}
}

func (bdp *BatchDeletePolicy) mapDynamic(dynConfig *DynConfig) *BatchDeletePolicy {
	config := dynConfig.config
	if config == nil || config.Dynamic == nil {
		return bdp
	}

	if config.Dynamic.BatchDelete != nil {
		if config.Dynamic.BatchDelete.DurableDelete != nil {
			configValue := *config.Dynamic.BatchDelete.DurableDelete
			bdp.DurableDelete = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("DurableDelete set to %t", configValue)
			}
		}
		if config.Dynamic.BatchDelete.SendKey != nil {
			configValue := *config.Dynamic.BatchDelete.SendKey
			bdp.SendKey = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SendKey set to %t", configValue)
			}
		}
	}

	return bdp
}

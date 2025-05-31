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

// BatchUDFPolicy attributes used in batch UDF execute commands.
type BatchUDFPolicy struct {
	// Optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord.ResultCode is set to types.FILTERED_OUT.
	//
	// Default: nil
	FilterExpression *Expression

	// Desired consistency guarantee when committing a command on the server. The default
	// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	// be successful before returning success to the client.
	//
	// Default: CommitLevel.COMMIT_ALL
	CommitLevel CommitLevel //= COMMIT_ALL

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
	// If true and the UDF writes a record, the key will be stored with the record on the server.
	// The default is to not send the user defined key.
	SendKey bool // = false
}

// NewBatchUDFPolicy returns a policy instance for Batch UDF commands.
func NewBatchUDFPolicy() *BatchUDFPolicy {
	return &BatchUDFPolicy{
		CommitLevel: COMMIT_ALL,
	}
}

func NewBatchUdfPolicyOrDefaultFromCache(dynConfig *DynConfig) *BatchUDFPolicy {
	if dynConfig == nil {
		return NewBatchUDFPolicy()
	}

	return dynConfig.client.dynDefaultBatchUDFPolicy.Load()
}

func (bup *BatchUDFPolicy) toWritePolicy(bp *BatchPolicy) *WritePolicy {
	wp := bp.toWritePolicy()

	if bup != nil {
		if bup.FilterExpression != nil {
			wp.FilterExpression = bup.FilterExpression
		}
		wp.CommitLevel = bup.CommitLevel
		wp.Expiration = bup.Expiration
		wp.DurableDelete = bup.DurableDelete
		wp.SendKey = bup.SendKey
	}
	return wp
}

func (bup *BatchUDFPolicy) toWritePolicyWithConfig(bp *BatchPolicy, dynConfig *DynConfig) *WritePolicy {
	wp := bp.toWritePolicy()

	if bup != nil {
		if bup.FilterExpression != nil {
			wp.FilterExpression = bup.FilterExpression
		}
		wp.CommitLevel = bup.CommitLevel
		wp.Expiration = bup.Expiration
		wp.DurableDelete = bup.DurableDelete
		wp.SendKey = bup.SendKey
	}

	// In Case dynConfig is not initialized or running return the policy before
	// merge
	if dynConfig == nil {
		return wp
	}

	config := dynConfig.config
	if config != nil && config.Dynamic.BatchUdf != nil {
		if config.Dynamic.BatchUdf.DurableDelete != nil {
			wp.DurableDelete = *config.Dynamic.BatchUdf.DurableDelete
		}
		if config.Dynamic.BatchUdf.SendKey != nil {
			wp.SendKey = *config.Dynamic.BatchUdf.SendKey
		}
	}

	return wp
}

// copyBatchUDFPolicy creates a new BasePolicy instance and copies the values from the source BatchUDFPolicy.
func copyBatchUDFPolicy(src *BatchUDFPolicy) *BatchUDFPolicy {
	if src == nil {
		return nil
	}

	response := *src
	return &response
}

// applyConfigToBatchUDFPolicy applies the dynamic configuration and generates a new policy
func applyConfigToBatchUDFPolicy(policy *BatchUDFPolicy, dynConfig *DynConfig) *BatchUDFPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotInitialized()

	if policy == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultBatchUDFPolicy.Load()
	}
	if config != nil && config.Dynamic != nil && config.Dynamic.BatchUdf != nil {
		// Dynamic configuration is exists for policy in question.
		var responsePolicy *BatchUDFPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responsePolicy = copyBatchUDFPolicy(policy)
		responsePolicy = mapDynamicBatchUdfPolicy(responsePolicy, dynConfig)

		return responsePolicy
	} else {
		return policy
	}
}

func mapDynamicBatchUdfPolicy(policy *BatchUDFPolicy, dynConfig *DynConfig) *BatchUDFPolicy {
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return policy
	}

	if dynConfig.config.Dynamic.BatchUdf != nil {
		if dynConfig.config.Dynamic.BatchUdf.DurableDelete != nil {
			policy.DurableDelete = *dynConfig.config.Dynamic.BatchUdf.DurableDelete
		}
		if dynConfig.config.Dynamic.BatchUdf.SendKey != nil {
			policy.SendKey = *dynConfig.config.Dynamic.BatchUdf.SendKey
		}
	}

	return policy
}

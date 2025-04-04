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

import (
	pc "github.com/aerospike/aerospike-client-go/v8/internal/cache"
)

// BatchReadPolicy attributes used in batch read commands.
type BatchReadPolicy struct {
	// FilterExpression is the optional expression filter. If FilterExpression exists and evaluates to false, the specific batch key
	// request is not performed and BatchRecord.ResultCode is set to types.FILTERED_OUT.
	//
	// Default: nil
	FilterExpression *Expression

	// ReadModeAP indicates read policy for AP (availability) namespaces.
	ReadModeAP ReadModeAP //= ONE

	// ReadModeSC indicates read policy for SC (strong consistency) namespaces.
	ReadModeSC ReadModeSC //= SESSION;

	// ReadTouchTTLPercent determines how record TTL (time to live) is affected on reads. When enabled, the server can
	// efficiently operate as a read-based LRU cache where the least recently used records are expired.
	// The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	// within this interval of the record’s end of life will generate a touch.
	//
	// For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	// 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	// recent write) will result in a touch, resetting the TTL to another 10 hours.
	//
	// Values:
	//
	// 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.
	// -1 : Do not reset record TTL on reads.
	// 1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.
	// Default: 0
	ReadTouchTTLPercent int32
}

// NewBatchReadPolicy returns a policy instance for BatchRead commands.
func NewBatchReadPolicy() *BatchReadPolicy {
	return &BatchReadPolicy{
		ReadModeAP: ReadModeAPOne,
		ReadModeSC: ReadModeSCSession,
	}
}

func NewBatchReadPolicyOrDefaultFromCache(dynConfig *DynConfig) *BatchReadPolicy {
	if dynConfig == nil {
		return NewBatchReadPolicy()
	}

	dynConfig.lock.RLock()
	defer dynConfig.lock.RUnlock()

	return dynConfig.mappedPolicies.Get(pc.BATCH_READ_POLICY).(*BatchReadPolicy)

}

func (brp *BatchReadPolicy) toWritePolicy(bp *BatchPolicy) *WritePolicy {
	wp := bp.toWritePolicy()

	if brp != nil {
		if brp.FilterExpression != nil {
			wp.FilterExpression = brp.FilterExpression
		}

		wp.ReadModeAP = brp.ReadModeAP
		wp.ReadModeSC = brp.ReadModeSC
		wp.ReadTouchTTLPercent = brp.ReadTouchTTLPercent
	}
	return wp
}

func (brp *BatchReadPolicy) ToWritePolicyWithConfig(bp *BatchPolicy, dynConfig *DynConfig) *WritePolicy {
	var wp *WritePolicy

	if dynConfig == nil {
		wp = bp.toWritePolicy()
	} else {
		dynConfig.lock.RLock()
		defer dynConfig.lock.RUnlock()

		wp = dynConfig.mappedPolicies.Get(pc.BATCH_PARENT_WRITE_POLICY).(*WritePolicy)
	}

	if brp != nil {
		if brp.FilterExpression != nil {
			wp.FilterExpression = brp.FilterExpression
		}

		wp.ReadModeAP = brp.ReadModeAP
		wp.ReadModeSC = brp.ReadModeSC
		wp.ReadTouchTTLPercent = brp.ReadTouchTTLPercent
	}

	return wp
}

// copyBAtchReadPolicy creates a new BasePolicy instance and copies the values from the source BatchReadPolicy.
func copyBatchReadPolicy(src *BatchReadPolicy) *BatchReadPolicy {
	if src == nil {
		return nil
	}

	response := NewBatchReadPolicy()

	response.FilterExpression = src.FilterExpression
	response.ReadModeAP = src.ReadModeAP
	response.ReadModeSC = src.ReadModeSC
	response.ReadTouchTTLPercent = src.ReadTouchTTLPercent

	return response
}

// applyConfigToBatchReadPolicy applies the dynamic configuration and generates a new policy.
func applyConfigToBatchReadPolicy(policy *BatchReadPolicy, dynConfig *DynConfig) *BatchReadPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotInitialized()

	dynConfig.lock.RLock()
	defer dynConfig.lock.RUnlock()

	if policy == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.mappedPolicies.Get(pc.BATCH_READ_POLICY).(*BatchReadPolicy)
	} else if config != nil && config.Dynamic != nil && config.Dynamic.BatchRead != nil {
		// Dynamic configuration is exists for policy in question.
		var responsePolicy *BatchReadPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		// Copy the existing write policy to preserve any custom settings.
		responsePolicy = copyBatchReadPolicy(policy)
		responsePolicy = mapDynamicBatchReadPolicy(responsePolicy, dynConfig)

		return responsePolicy
	} else {
		return policy
	}
}

func mapDynamicBatchReadPolicy(policy *BatchReadPolicy, dynConfig *DynConfig) *BatchReadPolicy {
	if dynConfig.config == nil && dynConfig.config.Dynamic == nil {
		return policy
	}

	if dynConfig.config.Dynamic.BatchRead != nil {
		if dynConfig.config.Dynamic.BatchRead.ReadModeAp != nil {
			policy.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.BatchRead.ReadModeAp)
		}
		if dynConfig.config.Dynamic.BatchRead.ReadModeSc != nil {
			policy.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.BatchRead.ReadModeSc)
		}
	}

	return policy
}

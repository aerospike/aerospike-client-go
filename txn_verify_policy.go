// Copyright 2014-2024 Aerospike, Inc.
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
	"time"
)

// Transaction policy fields used to batch verify record versions on commit.
// Used a placeholder for now as there are no additional fields beyond BatchPolicy.
type TxnVerifyPolicy struct {
	BatchPolicy
}

// NewTxnRollPolicy creates a new TxnVerifyPolicy instance with default values.
func NewTxnVerifyPolicy() *TxnVerifyPolicy {
	mp := *NewBatchPolicy()
	mp.ReadModeSC = ReadModeSCLinearize
	mp.ReplicaPolicy = MASTER
	mp.MaxRetries = 5
	mp.SocketTimeout = 3 * time.Second
	mp.TotalTimeout = 10 * time.Second
	mp.SleepBetweenRetries = 1 * time.Second

	return &TxnVerifyPolicy{
		BatchPolicy: mp,
	}
}

func NewTxnVerifyPolicyOrDefaultFromCache(dynConfig *DynConfig) *TxnVerifyPolicy {
	if dynConfig == nil {
		return NewTxnVerifyPolicy()
	}

	return dynConfig.client.dynDefaultTxnVerifyPolicy.Load()
}

func copyTxnVerifyPolicy(src *TxnVerifyPolicy) *TxnVerifyPolicy {
	if src == nil {
		return nil
	}

	response := *src
	return &response
}

// applyConfigToTxnRollPolicy applies the dynamic configuration and generates a new policy.
func applyConfigToTxnVerifyPolicy(policy *TxnVerifyPolicy, dynConfig *DynConfig) *TxnVerifyPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if policy == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultTxnVerifyPolicy.Load()
	} else if config != nil && config.Dynamic != nil && config.Dynamic.TxnVerify != nil {
		// Dynamic configuration is exists for policy in question.
		var responsePolicy *TxnVerifyPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responsePolicy = copyTxnVerifyPolicy(policy)
		responsePolicy = mapDynamicTxnVerifyPolicy(responsePolicy, dynConfig)

		return responsePolicy
	} else {
		return policy
	}
}

func mapDynamicTxnVerifyPolicy(policy *TxnVerifyPolicy, dynConfig *DynConfig) *TxnVerifyPolicy {
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return policy
	}

	if dynConfig.config.Dynamic.TxnVerify != nil {
		if dynConfig.config.Dynamic.TxnVerify.ReadModeAp != nil {
			policy.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.TxnVerify.ReadModeAp)
		}
		if dynConfig.config.Dynamic.TxnVerify.ReadModeSc != nil {
			policy.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.TxnVerify.ReadModeSc)
		}
		if dynConfig.config.Dynamic.TxnVerify.TotalTimeout != nil {
			policy.TotalTimeout = time.Duration(*dynConfig.config.Dynamic.TxnVerify.TotalTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnVerify.SocketTimeout != nil {
			policy.SocketTimeout = time.Duration(*dynConfig.config.Dynamic.TxnVerify.SocketTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnVerify.MaxRetries != nil {
			policy.MaxRetries = *dynConfig.config.Dynamic.TxnVerify.MaxRetries
		}
		if dynConfig.config.Dynamic.TxnVerify.SleepBetweenRetries != nil {
			policy.SleepBetweenRetries = time.Duration(*dynConfig.config.Dynamic.TxnVerify.SleepBetweenRetries) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnVerify.Replica != nil {
			policy.ReplicaPolicy = mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.TxnVerify.Replica)
		}
		if dynConfig.config.Dynamic.TxnVerify.MaxRetries != nil {
			policy.MaxRetries = *dynConfig.config.Dynamic.TxnVerify.MaxRetries
		}
	}

	return policy
}

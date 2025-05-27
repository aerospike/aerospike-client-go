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

	pc "github.com/aerospike/aerospike-client-go/v8/internal/cache"
)

// Transaction policy fields used to batch roll forward/backward records on
// commit or abort. Used a placeholder for now as there are no additional fields beyond BatchPolicy.
type TxnRollPolicy struct {
	BatchPolicy
}

// NewTxnRollPolicy creates a new TxnRollPolicy instance with default values.
func NewTxnRollPolicy() *TxnRollPolicy {
	mp := *NewBatchPolicy()
	mp.ReplicaPolicy = MASTER
	mp.MaxRetries = 5
	mp.SocketTimeout = 3 * time.Second
	mp.TotalTimeout = 10 * time.Second
	mp.SleepBetweenRetries = 1 * time.Second

	return &TxnRollPolicy{
		BatchPolicy: mp,
	}
}

func NewTxnRollPolicyOrDefaultFromCache(dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig == nil {
		return NewTxnRollPolicy()
	}

	dynConfig.lock.RLock()
	defer dynConfig.lock.RUnlock()

	return dynConfig.mappedPolicies.Get(pc.TXN_ROLL_POLICY).(*TxnRollPolicy)
}

func copyTxnRollPolicy(src *TxnRollPolicy) *TxnRollPolicy {
	if src == nil {
		return nil
	}

	response := *src
	return &response
}

// applyConfigToTxnRollPolicy applies the dynamic configuration and generates a new policy.
func applyConfigToTxnRollPolicy(policy *TxnRollPolicy, dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotInitialized()

	dynConfig.lock.RLock()
	defer dynConfig.lock.RUnlock()

	if policy == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.mappedPolicies.Get(pc.TXN_ROLL_POLICY).(*TxnRollPolicy)
	} else if config != nil && config.Dynamic != nil && config.Dynamic.TxnRoll != nil {
		// Dynamic configuration is exists for policy in question.
		var responseTxnRollPolicy *TxnRollPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responseTxnRollPolicy = copyTxnRollPolicy(policy)
		responseTxnRollPolicy = mapDynamicTxnRollPolicy(responseTxnRollPolicy, dynConfig)

		return responseTxnRollPolicy
	} else {
		return policy
	}
}

func mapDynamicTxnRollPolicy(policy *TxnRollPolicy, dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig.config == nil && dynConfig.config.Dynamic == nil {
		return policy
	}

	if dynConfig.config.Dynamic.TxnRoll != nil {
		if dynConfig.config.Dynamic.TxnRoll.ReadModeAp != nil {
			policy.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.TxnRoll.ReadModeAp)
		}
		if dynConfig.config.Dynamic.TxnRoll.ReadModeSc != nil {
			policy.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.TxnRoll.ReadModeSc)
		}
		if dynConfig.config.Dynamic.TxnRoll.Replica != nil {
			policy.ReplicaPolicy = mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.TxnRoll.Replica)
		}
		if dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries != nil {
			policy.SleepBetweenRetries = time.Duration(*dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries)
		}
		if dynConfig.config.Dynamic.TxnRoll.SocketTimeout != nil {
			policy.SocketTimeout = time.Duration(*dynConfig.config.Dynamic.TxnRoll.SocketTimeout)
		}
		if dynConfig.config.Dynamic.TxnRoll.TotalTimeout != nil {
			policy.TotalTimeout = time.Duration(*dynConfig.config.Dynamic.TxnRoll.TotalTimeout)
		}
		if dynConfig.config.Dynamic.TxnRoll.MaxRetries != nil {
			policy.MaxRetries = *dynConfig.config.Dynamic.TxnRoll.MaxRetries
		}
		if dynConfig.config.Dynamic.TxnRoll.RespondAllKeys != nil {
			policy.RespondAllKeys = *dynConfig.config.Dynamic.TxnRoll.RespondAllKeys
		}
	}

	return policy
}

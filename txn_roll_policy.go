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

func NewDynamicTxnRollPolicy(dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig == nil {
		return NewTxnRollPolicy()
	}

	return dynConfig.client.dynDefaultTxnRollPolicy.Load()
}

func (trp *TxnRollPolicy) copy() *TxnRollPolicy {
	if trp == nil {
		return nil
	}

	response := *trp
	return &response
}

// patchDynamic applies the dynamic configuration and generates a new policy.
func (trp *TxnRollPolicy) patchDynamic(dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig == nil {
		return trp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if trp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultTxnRollPolicy.Load()
	} else if config != nil && config.Dynamic != nil && config.Dynamic.TxnRoll != nil {
		// Dynamic configuration is exists for policy in question.
		var responseTxnRollPolicy *TxnRollPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responseTxnRollPolicy = trp.copy()
		responseTxnRollPolicy = responseTxnRollPolicy.mapDynamic(dynConfig)

		return responseTxnRollPolicy
	} else {
		return trp
	}
}

func (trp *TxnRollPolicy) mapDynamic(dynConfig *DynConfig) *TxnRollPolicy {
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return trp
	}

	if dynConfig.config.Dynamic.TxnRoll != nil {
		if dynConfig.config.Dynamic.TxnRoll.ReadModeAp != nil {
			trp.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.TxnRoll.ReadModeAp)
		}
		if dynConfig.config.Dynamic.TxnRoll.ReadModeSc != nil {
			trp.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.TxnRoll.ReadModeSc)
		}
		if dynConfig.config.Dynamic.TxnRoll.Replica != nil {
			trp.ReplicaPolicy = mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.TxnRoll.Replica)
		}
		if dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries != nil {
			trp.SleepBetweenRetries = time.Duration(*dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnRoll.SocketTimeout != nil {
			trp.SocketTimeout = time.Duration(*dynConfig.config.Dynamic.TxnRoll.SocketTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnRoll.TotalTimeout != nil {
			trp.TotalTimeout = time.Duration(*dynConfig.config.Dynamic.TxnRoll.TotalTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.TxnRoll.MaxRetries != nil {
			trp.MaxRetries = *dynConfig.config.Dynamic.TxnRoll.MaxRetries
		}
		if dynConfig.config.Dynamic.TxnRoll.RespondAllKeys != nil {
			trp.RespondAllKeys = *dynConfig.config.Dynamic.TxnRoll.RespondAllKeys
		}
	}

	return trp
}

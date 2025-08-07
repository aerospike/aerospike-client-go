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

	"github.com/aerospike/aerospike-client-go/v8/logger"
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

// copy creates a new TxnRollPolicy instance and copies the values from the source TxnRollPolicy.
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
			configValue := mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.TxnRoll.ReadModeAp)
			trp.ReadModeAP = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("ReadModeAP set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.ReadModeSc != nil {
			configValue := mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.TxnRoll.ReadModeSc)
			trp.ReadModeSC = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("ReadModeSC set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.Replica != nil {
			configValue := mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.TxnRoll.Replica)
			trp.ReplicaPolicy = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("ReplicaPolicy set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnRoll.SleepBetweenRetries) * time.Millisecond
			trp.SleepBetweenRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("SleepBetweenRetries set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.SocketTimeout != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnRoll.SocketTimeout) * time.Millisecond
			trp.SocketTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("SocketTimeout set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.TotalTimeout != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnRoll.TotalTimeout) * time.Millisecond
			trp.TotalTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("TotalTimeout set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.MaxRetries != nil {
			configValue := *dynConfig.config.Dynamic.TxnRoll.MaxRetries
			trp.MaxRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("MaxRetries set to %d", configValue)
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.RespondAllKeys != nil {
			configValue := *dynConfig.config.Dynamic.TxnRoll.RespondAllKeys
			trp.RespondAllKeys = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Debug("RespondAllKeys set to %t", configValue)
			}
		}
		if dynConfig.config.Dynamic.TxnRoll.TimeoutDelay != nil {
			trp.TimeoutDelay = time.Duration(*dynConfig.config.Dynamic.TxnRoll.TimeoutDelay) * time.Millisecond
		}
	}

	return trp
}

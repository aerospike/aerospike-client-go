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
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return trp
	}

	if currentConfig.Dynamic.TxnRoll != nil {
		if currentConfig.Dynamic.TxnRoll.ReadModeAp != nil {
			configValue := mapReadModeAPToReadModeAP(*currentConfig.Dynamic.TxnRoll.ReadModeAp)
			trp.ReadModeAP = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReadModeAP set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.ReadModeSc != nil {
			configValue := mapReadModeSCToReadModeSC(*currentConfig.Dynamic.TxnRoll.ReadModeSc)
			trp.ReadModeSC = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReadModeSC set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.Replica != nil {
			configValue := mapReplicaToReplicaPolicy(*currentConfig.Dynamic.TxnRoll.Replica)
			trp.ReplicaPolicy = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReplicaPolicy set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.SleepBetweenRetries != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnRoll.SleepBetweenRetries) * time.Millisecond
			trp.SleepBetweenRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SleepBetweenRetries set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.SocketTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnRoll.SocketTimeout) * time.Millisecond
			trp.SocketTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SocketTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.TotalTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnRoll.TotalTimeout) * time.Millisecond
			trp.TotalTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TotalTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnRoll.MaxRetries != nil {
			configValue := *currentConfig.Dynamic.TxnRoll.MaxRetries
			trp.MaxRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.TxnRoll.RespondAllKeys != nil {
			configValue := *currentConfig.Dynamic.TxnRoll.RespondAllKeys
			trp.RespondAllKeys = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("RespondAllKeys set to %t", configValue)
			}
		}
		if currentConfig.Dynamic.TxnRoll.TimeoutDelay != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnRoll.TimeoutDelay) * time.Millisecond
			trp.TimeoutDelay = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TimeoutDelay set to %s", configValue.String())
			}
		}
	}

	return trp
}

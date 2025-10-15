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

// copy creates a new TxnVerifyPolicy instance and copies the values from the source TxnVerifyPolicy.
func (tvp *TxnVerifyPolicy) copy() *TxnVerifyPolicy {
	if tvp == nil {
		return nil
	}

	response := *tvp
	return &response
}

// patchDynamic applies the dynamic configuration and generates a new policy.
func (tvp *TxnVerifyPolicy) patchDynamic(dynConfig *DynConfig) *TxnVerifyPolicy {
	if dynConfig == nil {
		return tvp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if tvp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultTxnVerifyPolicy.Load()
	} else if config != nil && config.Dynamic != nil && config.Dynamic.TxnVerify != nil {
		// Dynamic configuration is exists for policy in question.
		var responsePolicy *TxnVerifyPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responsePolicy = tvp.copy()
		responsePolicy = responsePolicy.mapDynamic(dynConfig)

		return responsePolicy
	} else {
		return tvp
	}
}

func (tvp *TxnVerifyPolicy) mapDynamic(dynConfig *DynConfig) *TxnVerifyPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return tvp
	}

	if currentConfig.Dynamic.TxnVerify != nil {
		if currentConfig.Dynamic.TxnVerify.ReadModeAp != nil {
			configValue := mapReadModeAPToReadModeAP(*currentConfig.Dynamic.TxnVerify.ReadModeAp)
			tvp.ReadModeAP = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReadModeAP set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.ReadModeSc != nil {
			configValue := mapReadModeSCToReadModeSC(*currentConfig.Dynamic.TxnVerify.ReadModeSc)
			tvp.ReadModeSC = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReadModeSC set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.TotalTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnVerify.TotalTimeout) * time.Millisecond
			tvp.TotalTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TotalTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.SocketTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnVerify.SocketTimeout) * time.Millisecond
			tvp.SocketTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SocketTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.MaxRetries != nil {
			configValue := *currentConfig.Dynamic.TxnVerify.MaxRetries
			tvp.MaxRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.TxnVerify.SleepBetweenRetries != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnVerify.SleepBetweenRetries) * time.Millisecond
			tvp.SleepBetweenRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SleepBetweenRetries set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.Replica != nil {
			configValue := mapReplicaToReplicaPolicy(*currentConfig.Dynamic.TxnVerify.Replica)
			tvp.ReplicaPolicy = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReplicaPolicy set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.TxnVerify.MaxRetries != nil {
			configValue := *currentConfig.Dynamic.TxnVerify.MaxRetries
			tvp.MaxRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.TxnVerify.TimeoutDelay != nil {
			configValue := time.Duration(*currentConfig.Dynamic.TxnVerify.TimeoutDelay) * time.Millisecond
			tvp.TimeoutDelay = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TimeoutDelay set to %s", configValue.String())
			}
		}
	}

	return tvp
}

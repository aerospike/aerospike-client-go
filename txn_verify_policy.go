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
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return tvp
	}

	if dynConfig.config.Dynamic.TxnVerify != nil {
		if dynConfig.config.Dynamic.TxnVerify.ReadModeAp != nil {
			configValue := mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.TxnVerify.ReadModeAp)
			tvp.ReadModeAP = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("ReadModeAP set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.ReadModeSc != nil {
			configValue := mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.TxnVerify.ReadModeSc)
			tvp.ReadModeSC = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("ReadModeSC set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.TotalTimeout != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnVerify.TotalTimeout) * time.Millisecond
			tvp.TotalTimeout = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("TotalTimeout set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.SocketTimeout != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnVerify.SocketTimeout) * time.Millisecond
			tvp.SocketTimeout = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("SocketTimeout set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.MaxRetries != nil {
			configValue := *dynConfig.config.Dynamic.TxnVerify.MaxRetries
			tvp.MaxRetries = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.SleepBetweenRetries != nil {
			configValue := time.Duration(*dynConfig.config.Dynamic.TxnVerify.SleepBetweenRetries) * time.Millisecond
			tvp.SleepBetweenRetries = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("SleepBetweenRetries set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.Replica != nil {
			configValue := mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.TxnVerify.Replica)
			tvp.ReplicaPolicy = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("ReplicaPolicy set to %s", configValue.String())
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.MaxRetries != nil {
			configValue := *dynConfig.config.Dynamic.TxnVerify.MaxRetries
			tvp.MaxRetries = configValue
			if dynConfig.configInitialized.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if dynConfig.config.Dynamic.TxnVerify.TimeoutDelay != nil {
			tvp.TimeoutDelay = time.Duration(*dynConfig.config.Dynamic.TxnVerify.TimeoutDelay) * time.Millisecond
		}
	}

	return tvp
}

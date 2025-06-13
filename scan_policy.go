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
	"time"
)

// ScanPolicy encapsulates parameters used in scan operations.
//
// Inherited Policy fields Policy.Txn are ignored in scan commands.
type ScanPolicy struct {
	MultiPolicy
}

// NewScanPolicy creates a new ScanPolicy instance with default values.
// Set MaxRetries for scans on server versions >= 4.9. All other
// scans are not retried.
//
// The latest servers support retries on individual data partitions.
// This feature is useful when a cluster is migrating and partition(s)
// are missed or incomplete on the first scan attempt.
//
// If the first scan attempt misses 2 of 4096 partitions, then only
// those 2 partitions are retried in the next scan attempt from the
// last key digest received for each respective partition.  A higher
// default MaxRetries is used because it's wasteful to invalidate
// all scan results because a single partition was missed.
func NewScanPolicy() *ScanPolicy {
	mp := *NewMultiPolicy()
	mp.TotalTimeout = 0

	return &ScanPolicy{
		MultiPolicy: mp,
	}
}

func NewDynamicScanPolicy(dynConfig *DynConfig) *ScanPolicy {
	if dynConfig == nil {
		return NewScanPolicy()
	}

	return dynConfig.client.dynDefaultScanPolicy.Load()
}

// copyQueryPolicy creates a new BasePolicy instance and copies the values from the source BasePolicy.
func (sp *ScanPolicy) copy() *ScanPolicy {
	if sp == nil {
		return nil
	}

	response := *sp
	return &response
}

// applyConfigToQueryPolicy applies the dynamic configuration and generates a new policy.
func (sp *ScanPolicy) patchDynamic(dynConfig *DynConfig) *ScanPolicy {
	if dynConfig == nil {
		return sp
	}

	config := dynConfig.config

	if config == nil && !dynConfig.configInitialized.Load() {
		// On initial load it is possible that the config is not yet loaded. This will kick things off to make sure
		// config is loaded.
		dynConfig.loadConfig()
		config = dynConfig.config
	}

	if sp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultScanPolicy.Load()
	} else if config != nil && config.Dynamic != nil && config.Dynamic.Scan != nil {
		// Dynamic configuration is exists for policy in question.
		// User has provided a custom policy. We need to apply the dynamic configuration.
		return sp.copy().mapDynamic(dynConfig)
	} else {
		return sp
	}
}

func (sp *ScanPolicy) mapDynamic(dynConfig *DynConfig) *ScanPolicy {
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return sp
	}

	if dynConfig.config.Dynamic.Scan != nil {
		if dynConfig.config.Dynamic.Scan.ReadModeAp != nil {
			sp.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.Scan.ReadModeAp)
		}
		if dynConfig.config.Dynamic.Scan.ReadModeSc != nil {
			sp.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.Scan.ReadModeSc)
		}
		if dynConfig.config.Dynamic.Scan.TotalTimeout != nil {
			sp.TotalTimeout = time.Duration(*dynConfig.config.Dynamic.Scan.TotalTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.Scan.SocketTimeout != nil {
			sp.SocketTimeout = time.Duration(*dynConfig.config.Dynamic.Scan.SocketTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.Scan.MaxRetries != nil {
			sp.MaxRetries = *dynConfig.config.Dynamic.Scan.MaxRetries
		}
		if dynConfig.config.Dynamic.Scan.SleepBetweenRetries != nil {
			sp.SleepBetweenRetries = time.Duration(*dynConfig.config.Dynamic.Scan.SleepBetweenRetries) * time.Millisecond
		}
		if dynConfig.config.Dynamic.Scan.Replica != nil {
			sp.ReplicaPolicy = mapReplicaToReplicaPolicy(*dynConfig.config.Dynamic.Scan.Replica)
		}
		if dynConfig.config.Dynamic.Scan.MaxConcurrentNodes != nil {
			sp.MaxConcurrentNodes = *dynConfig.config.Dynamic.Scan.MaxConcurrentNodes
		}
	}
	return sp
}

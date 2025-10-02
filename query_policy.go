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

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

// QueryPolicy encapsulates parameters for policy attributes used in query operations.
//
// Inherited Policy fields Policy.Txn are ignored in query commands.
type QueryPolicy struct {
	MultiPolicy

	// Expected query duration. The server treats the query in different ways depending on the expected duration.
	// This field is ignored for aggregation queries, background queries and server versions < 6.0.
	//
	// Default: LONG
	ExpectedDuration QueryDuration

	// ShortQuery determines wether query expected to return less than 100 records.
	// If true, the server will optimize the query for a small record set.
	// This field is ignored for aggregation queries, background queries
	// and server versions 6.0+.
	//
	// Default: false
	// This field is deprecated and will eventually be removed. Use ExpectedDuration instead.
	// For backwards compatibility: If ShortQuery is true, the query is treated as a short query and
	// ExpectedDuration is ignored. If shortQuery is false, ExpectedDuration is used defaults to {@link QueryDuration#LONG}.
	ShortQuery bool
}

// NewQueryPolicy generates a new QueryPolicy instance with default values.
// Set MaxRetries for non-aggregation queries with a nil filter on
// server versions >= 4.9. All other queries are not retried.
//
// The latest servers support retries on individual data partitions.
// This feature is useful when a cluster is migrating and partition(s)
// are missed or incomplete on the first query (with nil filter) attempt.
//
// If the first query attempt misses 2 of 4096 partitions, then only
// those 2 partitions are retried in the next query attempt from the
// last key digest received for each respective partition. A higher
// default MaxRetries is used because it's wasteful to invalidate
// all query results because a single partition was missed.
func NewQueryPolicy() *QueryPolicy {
	return &QueryPolicy{
		MultiPolicy: *NewMultiPolicy(),
	}
}

// copy creates a new BasePolicy instance and copies the values from the source BasePolicy.
func (qp *QueryPolicy) copy() *QueryPolicy {
	if qp == nil {
		return nil
	}

	response := *qp
	return &response
}

// applyConfigToQueryPolicy applies the dynamic configuration and generates a new policy.
func (qp *QueryPolicy) pathDynamic(dynConfig *DynConfig) *QueryPolicy {
	if dynConfig == nil {
		return qp
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if qp == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultQueryPolicy.Load()

	} else if config != nil && config.Dynamic != nil && config.Dynamic.Query != nil {
		// Dynamic configuration is exists for policy in question.
		// User has provided a custom policy. We need to apply the dynamic configuration.
		return qp.copy().mapDynamic(dynConfig)
	} else {
		return qp
	}
}

func (qp *QueryPolicy) mapDynamic(dynConfig *DynConfig) *QueryPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return qp
	}

	if currentConfig.Dynamic.Query != nil {
		if currentConfig.Dynamic.Query.TotalTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Query.TotalTimeout) * time.Millisecond
			qp.TotalTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TotalTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Query.SocketTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Query.SocketTimeout) * time.Millisecond
			qp.SocketTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SocketTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Query.MaxRetries != nil {
			configValue := *currentConfig.Dynamic.Query.MaxRetries
			qp.MaxRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("MaxRetries set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.Query.SleepBetweenRetries != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Query.SleepBetweenRetries) * time.Millisecond
			qp.SleepBetweenRetries = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("SleepBetweenRetries set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Query.Replica != nil {
			configValue := mapReplicaToReplicaPolicy(*currentConfig.Dynamic.Query.Replica)
			qp.ReplicaPolicy = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ReplicaPolicy set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Query.IncludeBinData != nil {
			configValue := *currentConfig.Dynamic.Query.IncludeBinData
			qp.IncludeBinData = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("IncludeBinData set to %t", configValue)
			}
		}
		if currentConfig.Dynamic.Query.ExpectedDuration != nil {
			configValue := mapQueryDuration(*currentConfig.Dynamic.Query.ExpectedDuration)
			qp.ExpectedDuration = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ExpectedDuration set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Query.TimeoutDelay != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Query.TimeoutDelay) * time.Millisecond
			qp.TimeoutDelay = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TimeoutDelay set to %s", configValue.String())
			}
		}
	}

	return qp
}

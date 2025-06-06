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

// BatchPolicy encapsulates parameters for policy attributes used in write operations.
// This object is passed into methods where database writes can occur.
type BatchPolicy struct {
	BasePolicy

	// Maximum number of concurrent batch request goroutines to server nodes at any point in time.
	// If there are 16 node/namespace combinations requested and ConcurrentNodes is 8,
	// then batch requests will be made for 8 node/namespace combinations in concurrent goroutines.
	// When a request completes, a new request will be issued until all 16 goroutines are complete.
	//
	// Values:
	// 1: Issue batch requests sequentially.  This mode has a performance advantage for small
	// to medium sized batch sizes because requests can be issued in the main command goroutine.
	// This is the default.
	// 0: Issue all batch requests in concurrent goroutines.  This mode has a performance
	// advantage for extremely large batch sizes because each node can process the request
	// immediately.  The downside is extra goroutines will need to be created (or taken from
	// a goroutine pool).
	// > 0: Issue up to ConcurrentNodes batch requests in concurrent goroutines.  When a request
	// completes, a new request will be issued until all goroutines are complete.  This mode
	// prevents too many concurrent goroutines being created for large cluster implementations.
	// The downside is extra goroutines will still need to be created (or taken from a goroutine pool).
	ConcurrentNodes int // = 1

	// Allow batch to be processed immediately in the server's receiving thread when the server
	// deems it to be appropriate.  If false, the batch will always be processed in separate
	// command goroutines.  This field is only relevant for the new batch index protocol.
	//
	// For batch exists or batch reads of smaller sized records (<= 1K per record), inline
	// processing will be significantly faster on "in memory" namespaces.  The server disables
	// inline processing on disk based namespaces regardless of this policy field.
	//
	// Inline processing can introduce the possibility of unfairness because the server
	// can process the entire batch before moving onto the next command.
	AllowInline bool //= true

	// Allow batch to be processed immediately in the server's receiving thread for SSD
	// namespaces. If false, the batch will always be processed in separate service threads.
	// Server versions before 6.0 ignore this field.
	//
	// Inline processing can introduce the possibility of unfairness because the server
	// can process the entire batch before moving onto the next command.
	//
	// Default: false
	AllowInlineSSD bool // = false

	// Should all batch keys be attempted regardless of errors. This field is used on both
	// the client and server. The client handles node specific errors and the server handles
	// key specific errors.
	//
	// If true, every batch key is attempted regardless of previous key specific errors.
	// Node specific errors such as timeouts stop keys to that node, but keys directed at
	// other nodes will continue to be processed.
	//
	// If false, the server will stop the batch to its node on most key specific errors.
	// The exceptions are types.KEY_NOT_FOUND_ERROR and types.FILTERED_OUT which never stop the batch.
	// The client will stop the entire batch on node specific errors for sync commands
	// that are run in sequence (MaxConcurrentThreads == 1). The client will not stop
	// the entire batch for async commands or sync commands run in parallel.
	//
	// Server versions &lt; 6.0 do not support this field and treat this value as false
	// for key specific errors.
	//
	// Default: true
	RespondAllKeys bool //= true;

	// AllowPartialResults determines if the results for some nodes should be returned in case
	// some nodes encounter an error. The result for the unreceived records will be nil.
	// The returned records will be safe to use, since only fully received data will be parsed
	// and set.
	//
	// This flag is only supported for BatchGet and BatchGetHeader methods. BatchGetComplex always returns
	// partial results by design.
	AllowPartialResults bool //= false
}

// NewBatchPolicy initializes a new BatchPolicy instance with default parameters.
func NewBatchPolicy() *BatchPolicy {
	return &BatchPolicy{
		BasePolicy:          *NewPolicy(),
		ConcurrentNodes:     1,
		AllowInline:         true,
		AllowPartialResults: false,
		RespondAllKeys:      true,
	}
}

func NewBatchPolicyOrDefaultFromCache(dynConfig *DynConfig) *BatchPolicy {
	if dynConfig == nil {
		return NewBatchPolicy()
	}

	return dynConfig.client.dynDefaultBatchPolicy.Load()
}

// NewReadBatchPolicy initializes a new BatchPolicy instance for reads.
func NewReadBatchPolicy() *BatchPolicy {
	return NewBatchPolicy()
}

// NewWriteBatchPolicy initializes a new BatchPolicy instance for writes.
func NewWriteBatchPolicy() *BatchPolicy {
	res := NewBatchPolicy()
	res.MaxRetries = 0
	return res
}

func (p *BatchPolicy) toWritePolicy() *WritePolicy {
	wp := NewWritePolicy(0, 0)
	if p != nil {
		wp.BasePolicy = p.BasePolicy
	}
	return wp
}

// copyQueryPolicy creates a new BasePolicy instance and copies the values from the source BasePolicy.
func copyBatchPolicy(src *BatchPolicy) *BatchPolicy {
	if src == nil {
		return nil
	}

	response := *src
	return &response
}

// applyConfigToQueryPolicy applies the dynamic configuration and generates a new policy
func applyConfigToBatchPolicy(policy *BatchPolicy, dynConfig *DynConfig) *BatchPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if policy == nil {
		// Passed in policy is nil, fetch mapped default policy from cache.
		return dynConfig.client.dynDefaultBatchPolicy.Load()
	} else if config != nil && config.Dynamic != nil && config.Dynamic.BatchRead != nil {
		// Dynamic configuration exists for policy in question.
		var responsePolicy *BatchPolicy
		// User has provided a custom policy. We need to apply the dynamic configuration.
		responsePolicy = copyBatchPolicy(policy)
		responsePolicy = mapDynamicBatchPolicy(responsePolicy, dynConfig)

		return responsePolicy
	} else {
		return policy
	}
}

func mapDynamicBatchPolicy(policy *BatchPolicy, dynConfig *DynConfig) *BatchPolicy {
	if dynConfig.config == nil || dynConfig.config.Dynamic == nil {
		return policy
	}

	if dynConfig.config.Dynamic.BatchRead != nil {
		if dynConfig.config.Dynamic.BatchRead.ReadModeAp != nil {
			policy.ReadModeAP = mapReadModeAPToReadModeAP(*dynConfig.config.Dynamic.BatchRead.ReadModeAp)
		}
		if dynConfig.config.Dynamic.BatchRead.ReadModeSc != nil {
			policy.ReadModeSC = mapReadModeSCToReadModeSC(*dynConfig.config.Dynamic.BatchRead.ReadModeSc)
		}
		if dynConfig.config.Dynamic.BatchRead.TotalTimeout != nil {
			policy.TotalTimeout = time.Duration(*dynConfig.config.Dynamic.BatchRead.TotalTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.BatchRead.SocketTimeout != nil {
			policy.SocketTimeout = time.Duration(*dynConfig.config.Dynamic.BatchRead.SocketTimeout) * time.Millisecond
		}
		if dynConfig.config.Dynamic.BatchRead.MaxRetries != nil {
			policy.MaxRetries = *dynConfig.config.Dynamic.BatchRead.MaxRetries
		}
		if dynConfig.config.Dynamic.BatchRead.SleepBetweenRetries != nil {
			policy.SleepBetweenRetries = time.Duration(*dynConfig.config.Dynamic.BatchRead.SleepBetweenRetries) * time.Millisecond
		}
		if dynConfig.config.Dynamic.BatchRead.AllowInline != nil {
			policy.AllowInline = *dynConfig.config.Dynamic.BatchRead.AllowInline
		}
		if dynConfig.config.Dynamic.BatchRead.AllowInlineSSD != nil {
			policy.AllowInlineSSD = *dynConfig.config.Dynamic.BatchRead.AllowInlineSSD
		}
		if dynConfig.config.Dynamic.BatchRead.RespondAllKeys != nil {
			policy.RespondAllKeys = *dynConfig.config.Dynamic.BatchRead.RespondAllKeys
		}
	}

	return policy
}

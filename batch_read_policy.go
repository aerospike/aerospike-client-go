// Copyright 2014-2021 Aerospike, Inc.
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

// Policy attributes used in batch read commands.
type BatchReadPolicy struct {
	// Optional expression filter. If filterExp exists and evaluates to false, the specific batch key
	// request is not performed and {@link com.aerospike.client.BatchRecord#resultCode} is set to
	// {@link com.aerospike.client.ResultCode#FILTERED_OUT}.
	//
	// If exists, this filter overrides the batch parent filter {@link com.aerospike.client.policy.Policy#filterExp}
	// for the specific key in batch commands that allow a different policy per key.
	// Otherwise, this filter is ignored.
	//
	// Default: null
	FilterExpression *Expression

	// ReadModeAP indicates read policy for AP (availability) namespaces.
	ReadModeAP ReadModeAP //= ONE

	// ReadModeSC indicates read policy for SC (strong consistency) namespaces.
	ReadModeSC ReadModeSC //= SESSION;
}

// NewBatchReadPolicy returns a policy instance for BatchRead commands.
func NewBatchReadPolicy() *BatchReadPolicy {
	return &BatchReadPolicy{
		ReadModeAP: ReadModeAPOne,
		ReadModeSC: ReadModeSCSession,
	}
}

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

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/types"
)

// ScanPolicy encapsulates parameters used in scan operations.
type ScanPolicy struct {
	MultiPolicy

	// ScanPercent determines percent of data to scan.
	// Valid integer range is 1 to 100.
	//
	// This field is supported on server versions < 4.9.
	// For server versions >= 4.9, use MultiPolicy.MaxRecords.
	//
	// Default is 100.
	ScanPercent int //= 100;

	// ConcurrentNodes determines how to issue scan requests (in parallel or sequentially).
	// The value of MultiPolicy.MaxConcurrentNodes will determine how many nodes will be
	// called in parallel.
	// This value is deprected and will be removed in the future.
	ConcurrentNodes bool //= true;
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
		MultiPolicy:     mp,
		ScanPercent:     100,
		ConcurrentNodes: true,
	}
}

// Verify policies fields are within range.
func (sp *ScanPolicy) validate() {
	if sp.ScanPercent <= 0 || sp.ScanPercent > 100 {
		panic(types.NewAerospikeError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid scan percent: %d", sp.ScanPercent)))
	}
}

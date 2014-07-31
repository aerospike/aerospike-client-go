// Copyright 2013-2014 Aerospike, Inc.
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

// Container object for optional parameters used in scan operations.
type ScanPolicy struct {
	BasePolicy

	// Percent of data to scan.  Valid integer range is 1 to 100.
	// Default is 100.
	ScanPercent int //= 100;

	// Maximum number of concurrent requests to server nodes at any poin int time.
	// If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then scan requests
	// will be made to 8 nodes in parallel.  When a scan completes, a new scan request will
	// be issued until all 16 nodes have been scanned.
	//
	// This field is only relevant when concurrentNodes is true.
	// Default (0) is to issue requests to all server nodes in parallel.
	MaxConcurrentNodes int

	// Issue scan requests in parallel or serially.
	ConcurrentNodes bool //= true;

	// Indicates if bin data is retrieved. If false, only record digests are retrieved.
	IncludeBinData bool //= true;

	// Terminate scan if cluster in fluctuating state.
	FailOnClusterChange bool
}

func NewScanPolicy() *ScanPolicy {
	return &ScanPolicy{
		BasePolicy:          *NewPolicy(),
		ScanPercent:         100,
		ConcurrentNodes:     true,
		MaxConcurrentNodes:  0,
		IncludeBinData:      true,
		FailOnClusterChange: true,
	}
}

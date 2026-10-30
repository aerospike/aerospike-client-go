// Copyright 2014-2026 Aerospike, Inc.
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

// NewStaticSeedCandidateProvider returns the seed candidate provider the
// client uses by default: it always returns the given seeds, in order, and
// never needs a periodic refresh.
func NewStaticSeedCandidateProvider(seeds ...*Host) SeedCandidateProvider {
	return staticSeedCandidateProvider{seeds: copyHosts(seeds)}
}

type staticSeedCandidateProvider struct {
	seeds []*Host
}

// RefreshSeedCandidates implements the SeedCandidateProvider interface.
func (provider staticSeedCandidateProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	return provider.seeds, nil
}

// NeedsPeriodicRefresh implements the SeedCandidateProvider interface.
func (staticSeedCandidateProvider) NeedsPeriodicRefresh() bool {
	return false
}

// SupportsDeadline implements the SeedCandidateProvider interface.
func (staticSeedCandidateProvider) SupportsDeadline() bool {
	return true
}

// Close implements the SeedCandidateProvider interface.
func (staticSeedCandidateProvider) Close() {
	// no-op
}

// copyHosts returns a deep copy of the given hosts: a new slice with a new
// Host per element, so later changes to the originals are not shared.
func copyHosts(hosts []*Host) []*Host {
	copied := make([]*Host, 0, len(hosts))
	for _, host := range hosts {
		if host == nil {
			continue
		}
		hostCopy := *host
		copied = append(copied, &hostCopy)
	}
	return copied
}

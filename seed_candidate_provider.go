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

// SeedCandidateContext describes one seed refresh call to a
// SeedCandidateProvider.
type SeedCandidateContext struct {
	// ClusterName is the configured ClientPolicy.ClusterName.
	ClusterName string

	// ConfiguredSeeds are the seed hosts the client was constructed with.
	// The slice and its hosts are read-only.
	ConfiguredSeeds []*Host

	// TendIteration counts refresh calls to this provider: the cluster tend
	// iteration in tend mode, the refresh loop iteration in thread mode.
	// It is zero on the first call during client initialization. Gate one-time
	// setup on provider state, not on this value.
	TendIteration int64
}

// SeedCandidateProvider supplies bootstrap and re-seed hosts. It does not
// define cluster membership. Nodes come from the server's peers lists.
type SeedCandidateProvider interface {
	// RefreshSeedCandidates returns seed candidates in preference order.
	// An empty list is valid, not an error.
	RefreshSeedCandidates(seedContext *SeedCandidateContext) ([]*Host, error)

	// NeedsPeriodicRefresh is true when candidates can change over time.
	// False means the provider is called once at client initialization.
	NeedsPeriodicRefresh() bool

	// SupportsDeadline is true when RefreshSeedCandidates bounds its own run
	// time to ClientPolicy.DiscoveryTendRefreshDeadline. Required for
	// DiscoveryExecutionTend.
	SupportsDeadline() bool

	// Close releases provider resources. Called once when the client closes,
	// after every other call has returned.
	Close()
}

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
	"encoding/json"
	"sync"

	iatomic "github.com/aerospike/aerospike-client-go/v7/internal/atomic"
)

// nodeStats keeps track of client's internal node statistics
// These statistics are aggregated once per tend in the cluster object
// and then are served to the end-user.
type nodeStats struct {
	m sync.Mutex
	// Attempts to open a connection (failed + successful)
	ConnectionsAttempts iatomic.Int `json:"connections-attempts"`
	// Successful attempts to open a connection
	ConnectionsSuccessful iatomic.Int `json:"connections-successful"`
	// Failed attempts to use a connection (includes all errors)
	ConnectionsFailed iatomic.Int `json:"connections-failed"`
	// Connection Timeout errors
	ConnectionsTimeoutErrors iatomic.Int `json:"connections-error-timeout"`
	// Connection errors other than timeouts
	ConnectionsOtherErrors iatomic.Int `json:"connections-error-other"`
	// Number of times circuit breaker was hit
	CircuitBreakerHits iatomic.Int `json:"circuit-breaker-hits"`
	// The command polled the connection pool, but no connections were in the pool
	ConnectionsPoolEmpty iatomic.Int `json:"connections-pool-empty"`
	// The command offered the connection to the pool, but the pool was full and the connection was closed
	ConnectionsPoolOverflow iatomic.Int `json:"connections-pool-overflow"`
	// The connection was idle and was dropped
	ConnectionsIdleDropped iatomic.Int `json:"connections-idle-dropped"`
	// Number of open connections at a given time
	ConnectionsOpen iatomic.Int `json:"open-connections"`
	// Number of connections that were closed, for any reason (idled out, errored out, etc)
	ConnectionsClosed iatomic.Int `json:"closed-connections"`
	// Total number of attempted tends (failed + success)
	TendsTotal iatomic.Int `json:"tends-total"`
	// Total number of successful tends
	TendsSuccessful iatomic.Int `json:"tends-successful"`
	// Total number of failed tends
	TendsFailed iatomic.Int `json:"tends-failed"`
	// Total number of partition map updates
	PartitionMapUpdates iatomic.Int `json:"partition-map-updates"`
	// Total number of times nodes were added to the client (not the same as actual nodes added. Network disruptions between client and server may cause a node being dropped and re-added client-side)
	NodeAdded iatomic.Int `json:"node-added-count"`
	// Total number of times nodes were removed from the client (not the same as actual nodes removed. Network disruptions between client and server may cause a node being dropped client-side)
	NodeRemoved iatomic.Int `json:"node-removed-count"`
}

// latest returns the latest values to be used in aggregation and then resets the values
func (ns *nodeStats) getAndReset() *nodeStats {
	ns.m.Lock()

	res := &nodeStats{
		ConnectionsAttempts:      ns.ConnectionsAttempts.CloneAndSet(0),
		ConnectionsSuccessful:    ns.ConnectionsSuccessful.CloneAndSet(0),
		ConnectionsFailed:        ns.ConnectionsFailed.CloneAndSet(0),
		ConnectionsTimeoutErrors: ns.ConnectionsTimeoutErrors.CloneAndSet(0),
		ConnectionsOtherErrors:   ns.ConnectionsOtherErrors.CloneAndSet(0),
		CircuitBreakerHits:       ns.CircuitBreakerHits.CloneAndSet(0),
		ConnectionsPoolEmpty:     ns.ConnectionsPoolEmpty.CloneAndSet(0),
		ConnectionsPoolOverflow:  ns.ConnectionsPoolOverflow.CloneAndSet(0),
		ConnectionsIdleDropped:   ns.ConnectionsIdleDropped.CloneAndSet(0),
		ConnectionsOpen:          ns.ConnectionsOpen.CloneAndSet(0),
		ConnectionsClosed:        ns.ConnectionsClosed.CloneAndSet(0),
		TendsTotal:               ns.TendsTotal.CloneAndSet(0),
		TendsSuccessful:          ns.TendsSuccessful.CloneAndSet(0),
		TendsFailed:              ns.TendsFailed.CloneAndSet(0),
		PartitionMapUpdates:      ns.PartitionMapUpdates.CloneAndSet(0),
		NodeAdded:                ns.NodeAdded.CloneAndSet(0),
		NodeRemoved:              ns.NodeRemoved.CloneAndSet(0),
	}

	ns.m.Unlock()
	return res
}

func (ns *nodeStats) clone() nodeStats {
	ns.m.Lock()

	res := nodeStats{
		ConnectionsAttempts:      ns.ConnectionsAttempts.Clone(),
		ConnectionsSuccessful:    ns.ConnectionsSuccessful.Clone(),
		ConnectionsFailed:        ns.ConnectionsFailed.Clone(),
		ConnectionsTimeoutErrors: ns.ConnectionsTimeoutErrors.Clone(),
		ConnectionsOtherErrors:   ns.ConnectionsOtherErrors.Clone(),
		CircuitBreakerHits:       ns.CircuitBreakerHits.Clone(),
		ConnectionsPoolEmpty:     ns.ConnectionsPoolEmpty.Clone(),
		ConnectionsPoolOverflow:  ns.ConnectionsPoolOverflow.Clone(),
		ConnectionsIdleDropped:   ns.ConnectionsIdleDropped.Clone(),
		ConnectionsOpen:          ns.ConnectionsOpen.Clone(),
		ConnectionsClosed:        ns.ConnectionsClosed.Clone(),
		TendsTotal:               ns.TendsTotal.Clone(),
		TendsSuccessful:          ns.TendsSuccessful.Clone(),
		TendsFailed:              ns.TendsFailed.Clone(),
		PartitionMapUpdates:      ns.PartitionMapUpdates.Clone(),
		NodeAdded:                ns.NodeAdded.Clone(),
		NodeRemoved:              ns.NodeRemoved.Clone(),
	}

	ns.m.Unlock()
	return res
}

func (ns *nodeStats) aggregate(newStats *nodeStats) {
	ns.m.Lock()

	ns.ConnectionsAttempts.AddAndGet(newStats.ConnectionsAttempts.Get())
	ns.ConnectionsSuccessful.AddAndGet(newStats.ConnectionsSuccessful.Get())
	ns.ConnectionsFailed.AddAndGet(newStats.ConnectionsFailed.Get())
	ns.ConnectionsTimeoutErrors.AddAndGet(newStats.ConnectionsTimeoutErrors.Get())
	ns.ConnectionsOtherErrors.AddAndGet(newStats.ConnectionsOtherErrors.Get())
	ns.CircuitBreakerHits.AddAndGet(newStats.CircuitBreakerHits.Get())
	ns.ConnectionsPoolEmpty.AddAndGet(newStats.ConnectionsPoolEmpty.Get())
	ns.ConnectionsPoolOverflow.AddAndGet(newStats.ConnectionsPoolOverflow.Get())
	ns.ConnectionsIdleDropped.AddAndGet(newStats.ConnectionsIdleDropped.Get())
	ns.ConnectionsOpen.AddAndGet(newStats.ConnectionsOpen.Get())
	ns.ConnectionsClosed.AddAndGet(newStats.ConnectionsClosed.Get())
	ns.TendsTotal.AddAndGet(newStats.TendsTotal.Get())
	ns.TendsSuccessful.AddAndGet(newStats.TendsSuccessful.Get())
	ns.TendsFailed.AddAndGet(newStats.TendsFailed.Get())
	ns.PartitionMapUpdates.AddAndGet(newStats.PartitionMapUpdates.Get())
	ns.NodeAdded.AddAndGet(newStats.NodeAdded.Get())
	ns.NodeRemoved.AddAndGet(newStats.NodeRemoved.Get())

	ns.m.Unlock()
}

func (ns nodeStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ConnectionsAttempts      int `json:"connections-attempts"`
		ConnectionsSuccessful    int `json:"connections-successful"`
		ConnectionsFailed        int `json:"connections-failed"`
		ConnectionsTimeoutErrors int `json:"connections-error-timeout"`
		ConnectionsOtherErrors   int `json:"connections-error-other"`
		CircuitBreakerHits       int `json:"circuit-breaker-hits"`
		ConnectionsPoolEmpty     int `json:"connections-pool-empty"`
		ConnectionsPoolOverflow  int `json:"connections-pool-overflow"`
		ConnectionsIdleDropped   int `json:"connections-idle-dropped"`
		ConnectionsOpen          int `json:"open-connections"`
		ConnectionsClosed        int `json:"closed-connections"`
		TendsTotal               int `json:"tends-total"`
		TendsSuccessful          int `json:"tends-successful"`
		TendsFailed              int `json:"tends-failed"`
		PartitionMapUpdates      int `json:"partition-map-updates"`
		NodeAdded                int `json:"node-added-count"`
		NodeRemoved              int `json:"node-removed-count"`
	}{
		ns.ConnectionsAttempts.Get(),
		ns.ConnectionsSuccessful.Get(),
		ns.ConnectionsFailed.Get(),
		ns.ConnectionsTimeoutErrors.Get(),
		ns.ConnectionsOtherErrors.Get(),
		ns.CircuitBreakerHits.Get(),
		ns.ConnectionsPoolEmpty.Get(),
		ns.ConnectionsPoolOverflow.Get(),
		ns.ConnectionsIdleDropped.Get(),
		ns.ConnectionsOpen.Get(),
		ns.ConnectionsClosed.Get(),
		ns.TendsTotal.Get(),
		ns.TendsSuccessful.Get(),
		ns.TendsFailed.Get(),
		ns.PartitionMapUpdates.Get(),
		ns.NodeAdded.Get(),
		ns.NodeRemoved.Get(),
	})
}

func (ns *nodeStats) UnmarshalJSON(data []byte) error {
	aux := struct {
		ConnectionsAttempts      int `json:"connections-attempts"`
		ConnectionsSuccessful    int `json:"connections-successful"`
		ConnectionsFailed        int `json:"connections-failed"`
		ConnectionsTimeoutErrors int `json:"connections-error-timeout"`
		ConnectionsOtherErrors   int `json:"connections-error-other"`
		CircuitBreakerHits       int `json:"circuit-breaker-hits"`
		ConnectionsPoolEmpty     int `json:"connections-pool-empty"`
		ConnectionsPoolOverflow  int `json:"connections-pool-overflow"`
		ConnectionsIdleDropped   int `json:"connections-idle-dropped"`
		ConnectionsOpen          int `json:"open-connections"`
		ConnectionsClosed        int `json:"closed-connections"`
		TendsTotal               int `json:"tends-total"`
		TendsSuccessful          int `json:"tends-successful"`
		TendsFailed              int `json:"tends-failed"`
		PartitionMapUpdates      int `json:"partition-map-updates"`
		NodeAdded                int `json:"node-added-count"`
		NodeRemoved              int `json:"node-removed-count"`
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ns.ConnectionsAttempts.Set(aux.ConnectionsAttempts)
	ns.ConnectionsSuccessful.Set(aux.ConnectionsSuccessful)
	ns.ConnectionsFailed.Set(aux.ConnectionsFailed)
	ns.ConnectionsTimeoutErrors.Set(aux.ConnectionsTimeoutErrors)
	ns.ConnectionsOtherErrors.Set(aux.ConnectionsOtherErrors)
	ns.CircuitBreakerHits.Set(aux.CircuitBreakerHits)
	ns.ConnectionsPoolEmpty.Set(aux.ConnectionsPoolEmpty)
	ns.ConnectionsPoolOverflow.Set(aux.ConnectionsPoolOverflow)
	ns.ConnectionsIdleDropped.Set(aux.ConnectionsIdleDropped)
	ns.ConnectionsOpen.Set(aux.ConnectionsOpen)
	ns.ConnectionsClosed.Set(aux.ConnectionsClosed)
	ns.TendsTotal.Set(aux.TendsTotal)
	ns.TendsSuccessful.Set(aux.TendsSuccessful)
	ns.TendsFailed.Set(aux.TendsFailed)
	ns.PartitionMapUpdates.Set(aux.PartitionMapUpdates)
	ns.NodeAdded.Set(aux.NodeAdded)
	ns.NodeRemoved.Set(aux.NodeRemoved)

	return nil
}

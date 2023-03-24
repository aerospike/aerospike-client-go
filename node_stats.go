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

import "sync/atomic"

// nodeStats keeps track of client's internal node statistics
// These statistics are aggregated once per tend in the cluster object
// and then are served to the end-user.
type nodeStats struct {
	// Attempts to open a connection (failed + successful)
	ConnectionsAttempts int64 `json:"connections-attempts"`
	// Successful attempts to open a connection
	ConnectionsSuccessful int64 `json:"connections-successful"`
	// Failed attempts to use a connection (includes all errors)
	ConnectionsFailed int64 `json:"connections-failed"`
	// Connection Timeout errors
	ConnectionsTimeoutErrors int64 `json:"connections-error-timeout"`
	// Connection errors other than timeouts
	ConnectionsOtherErrors int64 `json:"connections-error-other"`
	// Number of times circuit breaker was hit
	CircuitBreakerHits int64 `json:"circuit-breaker-hits"`
	// The command polled the connection pool, but no connections were in the pool
	ConnectionsPoolEmpty int64 `json:"connections-pool-empty"`
	// The command offered the connection to the pool, but the pool was full and the connection was closed
	ConnectionsPoolOverflow int64 `json:"connections-pool-overflow"`
	// The connection was idle and was dropped
	ConnectionsIdleDropped int64 `json:"connections-idle-dropped"`
	// Number of open connections at a given time
	ConnectionsOpen int64 `json:"open-connections"`
	// Number of connections that were closed, for any reason (idled out, errored out, etc)
	ConnectionsClosed int64 `json:"closed-connections"`
	// Total number of attempted tends (failed + success)
	TendsTotal int64 `json:"tends-total"`
	// Total number of successful tends
	TendsSuccessful int64 `json:"tends-successful"`
	// Total number of failed tends
	TendsFailed int64 `json:"tends-failed"`
	// Total number of partition map updates
	PartitionMapUpdates int64 `json:"partition-map-updates"`
	// Total number of times nodes were added to the client (not the same as actual nodes added. Network disruptions between client and server may cause a node being dropped and re-added client-side)
	NodeAdded int64 `json:"node-added-count"`
	// Total number of times nodes were removed from the client (not the same as actual nodes removed. Network disruptions between client and server may cause a node being dropped client-side)
	NodeRemoved int64 `json:"node-removed-count"`
}

// latest returns the latest values to be used in aggregation and then resets the values
func (ns *nodeStats) getAndReset() *nodeStats {
	return &nodeStats{
		ConnectionsAttempts:      atomic.SwapInt64(&ns.ConnectionsAttempts, 0),
		ConnectionsSuccessful:    atomic.SwapInt64(&ns.ConnectionsSuccessful, 0),
		ConnectionsFailed:        atomic.SwapInt64(&ns.ConnectionsFailed, 0),
		ConnectionsTimeoutErrors: atomic.SwapInt64(&ns.ConnectionsTimeoutErrors, 0),
		ConnectionsOtherErrors:   atomic.SwapInt64(&ns.ConnectionsOtherErrors, 0),
		CircuitBreakerHits:       atomic.SwapInt64(&ns.CircuitBreakerHits, 0),
		ConnectionsPoolEmpty:     atomic.SwapInt64(&ns.ConnectionsPoolEmpty, 0),
		ConnectionsPoolOverflow:  atomic.SwapInt64(&ns.ConnectionsPoolOverflow, 0),
		ConnectionsIdleDropped:   atomic.SwapInt64(&ns.ConnectionsIdleDropped, 0),
		ConnectionsOpen:          atomic.SwapInt64(&ns.ConnectionsOpen, 0),
		ConnectionsClosed:        atomic.SwapInt64(&ns.ConnectionsClosed, 0),
		TendsTotal:               atomic.SwapInt64(&ns.TendsTotal, 0),
		TendsSuccessful:          atomic.SwapInt64(&ns.TendsSuccessful, 0),
		TendsFailed:              atomic.SwapInt64(&ns.TendsFailed, 0),
		PartitionMapUpdates:      atomic.SwapInt64(&ns.PartitionMapUpdates, 0),
		NodeAdded:                atomic.SwapInt64(&ns.NodeAdded, 0),
		NodeRemoved:              atomic.SwapInt64(&ns.NodeRemoved, 0),
	}
}

// latest returns the latest values to be used in aggregation and then resets the values
func (ns *nodeStats) clone() nodeStats {
	return nodeStats{
		ConnectionsAttempts:      atomic.LoadInt64(&ns.ConnectionsAttempts),
		ConnectionsSuccessful:    atomic.LoadInt64(&ns.ConnectionsSuccessful),
		ConnectionsFailed:        atomic.LoadInt64(&ns.ConnectionsFailed),
		ConnectionsTimeoutErrors: atomic.LoadInt64(&ns.ConnectionsTimeoutErrors),
		ConnectionsOtherErrors:   atomic.LoadInt64(&ns.ConnectionsOtherErrors),
		CircuitBreakerHits:       atomic.LoadInt64(&ns.CircuitBreakerHits),
		ConnectionsPoolEmpty:     atomic.LoadInt64(&ns.ConnectionsPoolEmpty),
		ConnectionsPoolOverflow:  atomic.LoadInt64(&ns.ConnectionsPoolOverflow),
		ConnectionsIdleDropped:   atomic.LoadInt64(&ns.ConnectionsIdleDropped),
		ConnectionsOpen:          atomic.LoadInt64(&ns.ConnectionsOpen),
		ConnectionsClosed:        atomic.LoadInt64(&ns.ConnectionsClosed),
		TendsTotal:               atomic.LoadInt64(&ns.TendsTotal),
		TendsSuccessful:          atomic.LoadInt64(&ns.TendsSuccessful),
		TendsFailed:              atomic.LoadInt64(&ns.TendsFailed),
		PartitionMapUpdates:      atomic.LoadInt64(&ns.PartitionMapUpdates),
		NodeAdded:                atomic.LoadInt64(&ns.NodeAdded),
		NodeRemoved:              atomic.LoadInt64(&ns.NodeRemoved),
	}
}

func (ns *nodeStats) aggregate(newStats *nodeStats) {
	atomic.AddInt64(&ns.ConnectionsAttempts, newStats.ConnectionsAttempts)
	atomic.AddInt64(&ns.ConnectionsSuccessful, newStats.ConnectionsSuccessful)
	atomic.AddInt64(&ns.ConnectionsFailed, newStats.ConnectionsFailed)
	atomic.AddInt64(&ns.ConnectionsTimeoutErrors, newStats.ConnectionsTimeoutErrors)
	atomic.AddInt64(&ns.ConnectionsOtherErrors, newStats.ConnectionsOtherErrors)
	atomic.AddInt64(&ns.CircuitBreakerHits, newStats.CircuitBreakerHits)
	atomic.AddInt64(&ns.ConnectionsPoolEmpty, newStats.ConnectionsPoolEmpty)
	atomic.AddInt64(&ns.ConnectionsPoolOverflow, newStats.ConnectionsPoolOverflow)
	atomic.AddInt64(&ns.ConnectionsIdleDropped, newStats.ConnectionsIdleDropped)
	atomic.AddInt64(&ns.ConnectionsOpen, newStats.ConnectionsOpen)
	atomic.AddInt64(&ns.ConnectionsClosed, newStats.ConnectionsClosed)
	atomic.AddInt64(&ns.TendsTotal, newStats.TendsTotal)
	atomic.AddInt64(&ns.TendsSuccessful, newStats.TendsSuccessful)
	atomic.AddInt64(&ns.TendsFailed, newStats.TendsFailed)
	atomic.AddInt64(&ns.PartitionMapUpdates, newStats.PartitionMapUpdates)
	atomic.AddInt64(&ns.NodeAdded, newStats.NodeAdded)
	atomic.AddInt64(&ns.NodeRemoved, newStats.NodeRemoved)
}

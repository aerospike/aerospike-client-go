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
	m                     sync.Mutex
	ConnectionsAttempts   iatomic.Int `json:"connections-attempts"`
	ConnectionsSuccessful iatomic.Int `json:"connections-successful"`
	ConnectionsFailed     iatomic.Int `json:"connections-failed"`
	ConnectionsPoolEmpty  iatomic.Int `json:"connections-pool-empty"`
	ConnectionsOpen       iatomic.Int `json:"open-connections"`
	ConnectionsClosed     iatomic.Int `json:"closed-connections"`
	TendsTotal            iatomic.Int `json:"tends-total"`
	TendsSuccessful       iatomic.Int `json:"tends-successful"`
	TendsFailed           iatomic.Int `json:"tends-failed"`
	PartitionMapUpdates   iatomic.Int `json:"partition-map-updates"`
	NodeAdded             iatomic.Int `json:"node-added-count"`
	NodeRemoved           iatomic.Int `json:"node-removed-count"`
}

// latest returns the latest values to be used in aggregation and then resets the values
func (ns *nodeStats) getAndReset() *nodeStats {
	ns.m.Lock()

	res := &nodeStats{
		ConnectionsAttempts:   ns.ConnectionsAttempts.CloneAndSet(0),
		ConnectionsSuccessful: ns.ConnectionsSuccessful.CloneAndSet(0),
		ConnectionsFailed:     ns.ConnectionsFailed.CloneAndSet(0),
		ConnectionsPoolEmpty:  ns.ConnectionsPoolEmpty.CloneAndSet(0),
		ConnectionsOpen:       ns.ConnectionsOpen.CloneAndSet(0),
		ConnectionsClosed:     ns.ConnectionsClosed.CloneAndSet(0),
		TendsTotal:            ns.TendsTotal.CloneAndSet(0),
		TendsSuccessful:       ns.TendsSuccessful.CloneAndSet(0),
		TendsFailed:           ns.TendsFailed.CloneAndSet(0),
		PartitionMapUpdates:   ns.PartitionMapUpdates.CloneAndSet(0),
		NodeAdded:             ns.NodeAdded.CloneAndSet(0),
		NodeRemoved:           ns.NodeRemoved.CloneAndSet(0),
	}

	ns.m.Unlock()
	return res
}

func (ns *nodeStats) clone() nodeStats {
	ns.m.Lock()

	res := nodeStats{
		ConnectionsAttempts:   ns.ConnectionsAttempts.Clone(),
		ConnectionsSuccessful: ns.ConnectionsSuccessful.Clone(),
		ConnectionsFailed:     ns.ConnectionsFailed.Clone(),
		ConnectionsPoolEmpty:  ns.ConnectionsPoolEmpty.Clone(),
		ConnectionsOpen:       ns.ConnectionsOpen.Clone(),
		ConnectionsClosed:     ns.ConnectionsClosed.Clone(),
		TendsTotal:            ns.TendsTotal.Clone(),
		TendsSuccessful:       ns.TendsSuccessful.Clone(),
		TendsFailed:           ns.TendsFailed.Clone(),
		PartitionMapUpdates:   ns.PartitionMapUpdates.Clone(),
		NodeAdded:             ns.NodeAdded.Clone(),
		NodeRemoved:           ns.NodeRemoved.Clone(),
	}

	ns.m.Unlock()
	return res
}

func (ns *nodeStats) aggregate(newStats *nodeStats) {
	ns.m.Lock()

	ns.ConnectionsAttempts.AddAndGet(newStats.ConnectionsAttempts.Get())
	ns.ConnectionsSuccessful.AddAndGet(newStats.ConnectionsSuccessful.Get())
	ns.ConnectionsFailed.AddAndGet(newStats.ConnectionsFailed.Get())
	ns.ConnectionsPoolEmpty.AddAndGet(newStats.ConnectionsPoolEmpty.Get())
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
		ConnectionsAttempts   int `json:"connections-attempts"`
		ConnectionsSuccessful int `json:"connections-successful"`
		ConnectionsFailed     int `json:"connections-failed"`
		ConnectionsPoolEmpty  int `json:"connections-pool-empty"`
		ConnectionsOpen       int `json:"open-connections"`
		ConnectionsClosed     int `json:"closed-connections"`
		TendsTotal            int `json:"tends-total"`
		TendsSuccessful       int `json:"tends-successful"`
		TendsFailed           int `json:"tends-failed"`
		PartitionMapUpdates   int `json:"partition-map-updates"`
		NodeAdded             int `json:"node-added-count"`
		NodeRemoved           int `json:"node-removed-count"`
	}{
		ns.ConnectionsAttempts.Get(),
		ns.ConnectionsSuccessful.Get(),
		ns.ConnectionsFailed.Get(),
		ns.ConnectionsPoolEmpty.Get(),
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
		ConnectionsAttempts   int `json:"connections-attempts"`
		ConnectionsSuccessful int `json:"connections-successful"`
		ConnectionsFailed     int `json:"connections-failed"`
		ConnectionsPoolEmpty  int `json:"connections-pool-empty"`
		ConnectionsOpen       int `json:"open-connections"`
		ConnectionsClosed     int `json:"closed-connections"`
		TendsTotal            int `json:"tends-total"`
		TendsSuccessful       int `json:"tends-successful"`
		TendsFailed           int `json:"tends-failed"`
		PartitionMapUpdates   int `json:"partition-map-updates"`
		NodeAdded             int `json:"node-added-count"`
		NodeRemoved           int `json:"node-removed-count"`
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ns.ConnectionsAttempts.Set(aux.ConnectionsAttempts)
	ns.ConnectionsSuccessful.Set(aux.ConnectionsSuccessful)
	ns.ConnectionsFailed.Set(aux.ConnectionsFailed)
	ns.ConnectionsPoolEmpty.Set(aux.ConnectionsPoolEmpty)
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

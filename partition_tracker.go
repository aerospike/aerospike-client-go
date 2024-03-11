// Copyright 2014-2022 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

import (
	"fmt"
	"strings"
	"time"

	atmc "github.com/aerospike/aerospike-client-go/v7/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v7/types"
)

type partitionTracker struct {
	partitions          []*PartitionStatus
	partitionsCapacity  int
	partitionBegin      int
	nodeCapacity        int
	nodeFilter          *Node
	partitionFilter     *PartitionFilter
	replica             ReplicaPolicy
	nodePartitionsList  []*nodePartitions
	recordCount         *atmc.Int
	maxRecords          int64
	sleepBetweenRetries time.Duration
	socketTimeout       time.Duration
	totalTimeout        time.Duration
	iteration           int //= 1
	deadline            time.Time
}

func newPartitionTrackerForNodes(policy *MultiPolicy, nodes []*Node) *partitionTracker {
	// Create initial partition capacity for each node as average + 25%.
	ppn := _PARTITIONS / len(nodes)
	ppn += ppn / 4

	pt := partitionTracker{
		partitionBegin:     0,
		nodeCapacity:       len(nodes),
		nodeFilter:         nil,
		replica:            policy.ReplicaPolicy,
		partitionsCapacity: ppn,
		maxRecords:         policy.MaxRecords,
		iteration:          1,
	}

	pt.partitions = pt.initPartitions(policy, _PARTITIONS, nil)
	pt.init(policy)
	return &pt
}

func newPartitionTrackerForNode(policy *MultiPolicy, nodeFilter *Node) *partitionTracker {
	pt := partitionTracker{
		partitionBegin:     0,
		nodeCapacity:       1,
		nodeFilter:         nodeFilter,
		replica:            policy.ReplicaPolicy,
		partitionsCapacity: _PARTITIONS,
		maxRecords:         policy.MaxRecords,
		iteration:          1,
	}

	pt.partitions = pt.initPartitions(policy, _PARTITIONS, nil)
	pt.init(policy)
	return &pt
}

func newPartitionTracker(policy *MultiPolicy, filter *PartitionFilter, nodes []*Node) *partitionTracker {
	// Validate here instead of initial PartitionFilter constructor because total number of
	// cluster partitions may change on the server and PartitionFilter will never have access
	// to Cluster instance.  Use fixed number of partitions for now.
	if !(filter.Begin >= 0 && filter.Begin < _PARTITIONS) {
		panic(newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid partition begin %d . Valid range: 0-%d", filter.Begin,
			(_PARTITIONS-1))))
	}

	if filter.Count <= 0 {
		panic(newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid partition count %d", filter.Count)))
	}

	if filter.Begin+filter.Count > _PARTITIONS {
		panic(newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid partition range (%d,%d)", filter.Begin, filter.Begin+filter.Count)))
	}

	// This is required for proxy server since there are no nodes represented there
	nodeCapacity := len(nodes)
	if nodeCapacity <= 0 {
		nodeCapacity = 1
	}

	pt := &partitionTracker{
		partitionBegin:     filter.Begin,
		nodeCapacity:       nodeCapacity,
		nodeFilter:         nil,
		replica:            policy.ReplicaPolicy,
		partitionsCapacity: filter.Count,
		maxRecords:         policy.MaxRecords,
		iteration:          1,
	}

	if len(filter.Partitions) == 0 {
		filter.Partitions = pt.initPartitions(policy, filter.Count, filter.Digest)
		filter.Retry = true
	} else {
		// Retry all partitions when maxRecords not specified.
		if policy.MaxRecords <= 0 {
			filter.Retry = true
		}

		// Reset replica sequence and last node used.
		for _, part := range filter.Partitions {
			part.sequence = 0
			part.node = nil
		}
	}

	pt.partitions = filter.Partitions
	pt.partitionFilter = filter
	pt.init(policy)
	return pt
}

func (pt *partitionTracker) init(policy *MultiPolicy) {
	pt.sleepBetweenRetries = policy.SleepBetweenRetries
	pt.socketTimeout = policy.SocketTimeout
	pt.totalTimeout = policy.TotalTimeout

	if pt.totalTimeout > 0 {
		pt.deadline = time.Now().Add(pt.totalTimeout)
		if pt.socketTimeout == 0 || pt.socketTimeout > pt.totalTimeout {
			pt.socketTimeout = pt.totalTimeout
		}
	}

	if pt.replica == RANDOM {
		panic(newError(types.PARAMETER_ERROR, "Invalid replica: RANDOM"))
	}
}

func (pt *partitionTracker) initPartitions(policy *MultiPolicy, partitionCount int, digest []byte) []*PartitionStatus {
	partsAll := make([]*PartitionStatus, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partsAll[i] = newPartitionStatus(pt.partitionBegin + i)
	}

	if digest != nil {
		partsAll[0].Digest = digest
	}

	return partsAll
}

func (pt *partitionTracker) SetSleepBetweenRetries(sleepBetweenRetries time.Duration) {
	pt.sleepBetweenRetries = sleepBetweenRetries
}

func (pt *partitionTracker) assignPartitionsToNodes(cluster *Cluster, namespace string) ([]*nodePartitions, Error) {
	list := make([]*nodePartitions, 0, pt.nodeCapacity)

	pMap := cluster.getPartitions()
	parts := pMap[namespace]

	if parts == nil {
		return nil, newError(types.INVALID_NAMESPACE, fmt.Sprintf("Invalid Partition Map for namespace `%s` in Partition Scan", namespace))
	}

	p := NewPartitionForReplicaPolicy(namespace, pt.replica)
	retry := (pt.partitionFilter == nil || pt.partitionFilter.Retry) && (pt.iteration == 1)

	for _, part := range pt.partitions {
		if retry || part.Retry {
			node, err := p.GetNodeQuery(cluster, parts, part)
			if err != nil {
				return nil, err
			}

			// Use node name to check for single node equality because
			// partition map may be in transitional state between
			// the old and new node with the same name.
			if pt.nodeFilter != nil && pt.nodeFilter.GetName() != node.GetName() {
				continue
			}

			np := pt.findNode(list, node)

			if np == nil {
				// If the partition map is in a transitional state, multiple
				// nodePartitions instances (each with different partitions)
				// may be created for a single node.
				np = newNodePartitions(node, pt.partitionsCapacity)
				list = append(list, np)
			}
			np.addPartition(part)
		}
	}

	nodeSize := len(list)
	if nodeSize <= 0 {
		return nil, newError(types.INVALID_NODE_ERROR, "No nodes were assigned")
	}

	// Set global retry to true because scan/query may terminate early and all partitions
	// will need to be retried if the PartitionFilter instance is reused in a new scan/query.
	// Global retry will be set to false if the scan/query completes normally and maxRecords
	// is specified.
	if pt.partitionFilter != nil {
		pt.partitionFilter.Retry = true
	}

	pt.recordCount = nil

	if pt.maxRecords > 0 {
		if pt.maxRecords >= int64(nodeSize) {
			// Distribute maxRecords across nodes.
			max := pt.maxRecords / int64(nodeSize)
			rem := pt.maxRecords - (max * int64(nodeSize))

			for i, np := range list {
				if int64(i) < rem {
					np.recordMax = max + 1
				} else {
					np.recordMax = max
				}
			}
		} else {
			// If maxRecords < nodeSize, the retry = true, ensure each node receives at least one max record
			// allocation and filter out excess records when receiving records from the server.
			for _, np := range list {
				np.recordMax = 1
			}

			// Track records returned for this iteration.
			pt.recordCount = atmc.NewInt(0)
		}
	}

	pt.nodePartitionsList = list
	return list, nil
}

func (pt *partitionTracker) findNode(list []*nodePartitions, node *Node) *nodePartitions {
	for _, nodePartition := range list {
		// Use pointer equality for performance.
		if nodePartition.node == node {
			return nodePartition
		}
	}
	return nil
}

func (pt *partitionTracker) partitionUnavailable(nodePartitions *nodePartitions, partitionId int) {
	ps := pt.partitions[partitionId-pt.partitionBegin]
	ps.Retry = true
	ps.sequence++
	nodePartitions.partsUnavailable++
}

func (pt *partitionTracker) setDigest(nodePartitions *nodePartitions, key *Key) {
	partitionId := key.PartitionId()
	pt.partitions[partitionId-pt.partitionBegin].Digest = key.Digest()

	// nodePartitions is nil in Proxy client
	if nodePartitions != nil {
		nodePartitions.recordCount++
	}
}

func (pt *partitionTracker) setLast(nodePartitions *nodePartitions, key *Key, bval *int64) {
	partitionId := key.PartitionId()
	if partitionId-pt.partitionBegin < 0 {
		panic(fmt.Sprintf("Partition mismatch: key.partitionId: %d, partitionBegin: %d", partitionId, pt.partitionBegin))
	}
	ps := pt.partitions[partitionId-pt.partitionBegin]
	ps.Digest = key.digest[:]
	if bval != nil {
		ps.BVal = *bval
	}

	// nodePartitions is nil in Proxy client
	if nodePartitions != nil {
		nodePartitions.recordCount++
	}
}

func (pt *partitionTracker) allowRecord(np *nodePartitions) bool {
	if pt.recordCount == nil || int64(pt.recordCount.AddAndGet(1)) <= pt.maxRecords {
		return true
	}

	// Record was returned, but would exceed maxRecords.
	// Discard record and increment disallowedCount.
	np.disallowedCount++
	return false
}

func (pt *partitionTracker) isClusterComplete(cluster *Cluster, policy *BasePolicy) (bool, Error) {
	return pt.isComplete(cluster.supportsPartitionQuery.Get(), policy, pt.nodePartitionsList)
}

func (pt *partitionTracker) isComplete(hasPartitionQuery bool, policy *BasePolicy, nodePartitionsList []*nodePartitions) (bool, Error) {
	recordCount := int64(0)
	partsUnavailable := 0

	for _, np := range nodePartitionsList {
		recordCount += np.recordCount
		partsUnavailable += np.partsUnavailable
	}

	if partsUnavailable == 0 {
		if pt.maxRecords <= 0 {
			if pt.partitionFilter != nil {
				pt.partitionFilter.Retry = false
				pt.partitionFilter.Done = true
			}
		} else if pt.iteration > 1 {
			if pt.partitionFilter != nil {
				// If errors occurred on a node, only that node's partitions are retried in the
				// next iteration. If that node finally succeeds, the other original nodes still
				// need to be retried if partition state is reused in the next scan/query command.
				// Force retry on all node partitions.
				pt.partitionFilter.Retry = true
				pt.partitionFilter.Done = false
			}
		} else {
			// Cluster will be nil for the Proxy client
			if hasPartitionQuery {
				// Server version >= 6.0 will return all records for each node up to
				// that node's max. If node's record count reached max, there still
				// may be records available for that node.
				done := true

				for _, np := range nodePartitionsList {
					if np.recordCount+np.disallowedCount >= np.recordMax {
						pt.markRetry(np)
						done = false
					}
				}

				if pt.partitionFilter != nil {
					pt.partitionFilter.Retry = false
					pt.partitionFilter.Done = done
				}
			} else {
				// Servers version < 6.0 can return less records than max and still
				// have more records for each node, so the node is only done if no
				// records were retrieved for that node.
				for _, np := range nodePartitionsList {
					if np.recordCount+np.disallowedCount > 0 {
						pt.markRetry(np)
					}
				}

				if pt.partitionFilter != nil {
					pt.partitionFilter.Retry = false
					pt.partitionFilter.Done = (recordCount == 0)
				}
			}
		}
		return true, nil
	}

	if pt.maxRecords > 0 && recordCount >= pt.maxRecords {
		return true, nil
	}

	// Check if limits have been reached.
	if pt.iteration > policy.MaxRetries {
		return false, newError(types.MAX_RETRIES_EXCEEDED, fmt.Sprintf("Max retries exceeded: %d", policy.MaxRetries))
	}

	if policy.TotalTimeout > 0 {
		// Check for total timeout.
		remaining := time.Until(pt.deadline) - pt.sleepBetweenRetries

		if remaining <= 0 {
			return false, ErrTimeout.err()
		}

		if remaining < pt.totalTimeout {
			pt.totalTimeout = remaining

			if pt.socketTimeout > pt.totalTimeout {
				pt.socketTimeout = pt.totalTimeout
			}
		}
	}

	// Prepare for next iteration.
	if pt.maxRecords > 0 {
		pt.maxRecords -= recordCount
	}

	pt.iteration++
	return false, nil
}

func (pt *partitionTracker) shouldRetry(nodePartitions *nodePartitions, e Error) bool {
	res := e.Matches(
		types.TIMEOUT,
		types.NETWORK_ERROR,
		types.SERVER_NOT_AVAILABLE,
		types.INDEX_NOTFOUND,
		types.INDEX_NOTREADABLE,
	)
	if res {
		pt.markRetrySequence(nodePartitions)
		nodePartitions.partsUnavailable = len(nodePartitions.partsFull) + len(nodePartitions.partsPartial)
	}
	return res
}

func (pt *partitionTracker) markRetrySequence(nodePartitions *nodePartitions) {
	// Mark retry for next replica.
	for _, ps := range nodePartitions.partsFull {
		ps.Retry = true
		ps.sequence++
	}

	for _, ps := range nodePartitions.partsPartial {
		ps.Retry = true
		ps.sequence++
	}
}

func (pt *partitionTracker) markRetry(nodePartitions *nodePartitions) {
	// Mark retry for same replica.
	for _, ps := range nodePartitions.partsFull {
		ps.Retry = true
	}

	for _, ps := range nodePartitions.partsPartial {
		ps.Retry = true
	}
}

func (pt *partitionTracker) partitionError() {
	// Mark all partitions for retry on fatal errors.
	if pt.partitionFilter != nil {
		pt.partitionFilter.Retry = true
	}
}

func (pt *partitionTracker) String() string {
	var sb strings.Builder
	for i, ps := range pt.partitions {
		sb.WriteString(ps.String())
		if (i+1)%16 == 0 {
			sb.WriteString("\n")
		} else {
			sb.WriteString("\t")
		}
	}
	return sb.String()
}

type nodePartitions struct {
	node             *Node
	partsFull        []*PartitionStatus
	partsPartial     []*PartitionStatus
	recordCount      int64
	recordMax        int64
	disallowedCount  int64
	partsUnavailable int
}

func newNodePartitions(node *Node, capacity int) *nodePartitions {
	return &nodePartitions{
		node:         node,
		partsFull:    make([]*PartitionStatus, 0, capacity),
		partsPartial: make([]*PartitionStatus, 0, capacity),
	}
}

func (np *nodePartitions) String() string {
	return fmt.Sprintf("Node %s: full: %d, partial: %d", np.node.String(), len(np.partsFull), len(np.partsPartial))
}

func (np *nodePartitions) addPartition(part *PartitionStatus) {
	if part.Digest == nil {
		np.partsFull = append(np.partsFull, part)
	} else {
		np.partsPartial = append(np.partsPartial, part)
	}
}

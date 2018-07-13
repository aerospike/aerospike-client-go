// Copyright 2013-2017 Aerospike, Inc.
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
	"bytes"
	"errors"
	"fmt"
	"strconv"

	. "github.com/aerospike/aerospike-client-go/types"
)

type Partitions struct {
	Replicas [][]*Node
	CPMode   bool
	regimes  []int
}

func newPartitions(partitionCount int, replicaCount int, cpMode bool) *Partitions {
	replicas := make([][]*Node, replicaCount)
	for i := range replicas {
		replicas[i] = make([]*Node, partitionCount)
	}

	return &Partitions{
		Replicas: replicas,
		CPMode:   cpMode,
		regimes:  make([]int, partitionCount),
	}
}

// Copy partition map while reserving space for a new replica count.
func clonePartitions(other *Partitions, replicaCount int) *Partitions {
	replicas := make([][]*Node, replicaCount)

	if len(other.Replicas) < replicaCount {
		// Copy existing entries.
		i := copy(replicas, other.Replicas)

		// Create new entries.
		for ; i < replicaCount; i++ {
			replicas[i] = make([]*Node, len(other.regimes))
		}
	} else {
		// Copy existing entries.
		copy(replicas, other.Replicas)
	}

	regimes := make([]int, len(other.regimes))
	copy(regimes, other.regimes)

	return &Partitions{
		Replicas: replicas,
		CPMode:   other.CPMode,
		regimes:  regimes,
	}
}

/*

	partitionMap

*/

type partitionMap map[string]*Partitions

// String implements stringer interface for partitionMap
func (pm partitionMap) clone() partitionMap {
	// Make shallow copy of map.
	pmap := make(partitionMap, len(pm))
	for ns, _ := range pm {
		pmap[ns] = clonePartitions(pm[ns], len(pm[ns].Replicas))
	}
	return pmap
}

// String implements stringer interface for partitionMap
func (pm partitionMap) merge(other partitionMap) {
	// merge partitions; iterate over the new partition and update the old one
	for ns, partitions := range other {
		replicaArray := partitions.Replicas
		if pm[ns] == nil {
			pm[ns] = clonePartitions(partitions, len(replicaArray))
		} else {
			if len(pm[ns].regimes) < len(partitions.regimes) {
				// expand regime size array
				regimes := make([]int, len(partitions.regimes))
				copy(regimes, pm[ns].regimes)
				pm[ns].regimes = regimes
			}

			for i, nodeArray := range replicaArray {
				if len(pm[ns].Replicas) <= i {
					pm[ns].Replicas = append(pm[ns].Replicas, make([]*Node, len(nodeArray)))
				} else if pm[ns].Replicas[i] == nil {
					pm[ns].Replicas[i] = make([]*Node, len(nodeArray))
				}

				// merge nodes into the partition map
				for j, node := range nodeArray {
					if pm[ns].regimes[i] <= partitions.regimes[i] && node != nil {
						pm[ns].Replicas[i][j] = node
					}
				}
			}
		}
	}
}

// String implements stringer interface for partitionMap
func (pm partitionMap) String() string {
	res := bytes.Buffer{}
	for ns, partitions := range pm {
		replicaArray := partitions.Replicas
		for i, nodeArray := range replicaArray {
			for j, node := range nodeArray {
				res.WriteString(ns)
				res.WriteString(",")
				res.WriteString(strconv.Itoa(i))
				res.WriteString(",")
				res.WriteString(strconv.Itoa(j))
				res.WriteString(",")
				if node != nil {
					res.WriteString(node.String())
				} else {
					res.WriteString("NIL")
				}
				res.WriteString("\n")
			}
		}
	}
	return res.String()
}

// naively validates the partition map
func (pm partitionMap) validate() error {
	masterNodePartitionNotDefined := map[string][]int{}
	replicaNodePartitionNotDefined := map[string][]int{}
	var errList []error

	for nsName, partition := range pm {
		if len(partition.regimes) != _PARTITIONS {
			errList = append(errList, fmt.Errorf("Wrong number of regimes for namespace `%s`. Must be %d, but found %d..", nsName, _PARTITIONS, len(partition.regimes)))
		}

		for replica, partitionNodes := range partition.Replicas {
			if len(partitionNodes) != _PARTITIONS {
				errList = append(errList, fmt.Errorf("Wrong number of partitions for namespace `%s`, replica `%d`. Must be %d, but found %d.", nsName, replica, _PARTITIONS, len(partitionNodes)))
			}

			for pIndex, node := range partitionNodes {
				if node == nil {
					if replica == 0 {
						masterNodePartitionNotDefined[nsName] = append(masterNodePartitionNotDefined[nsName], pIndex)
					} else {
						replicaNodePartitionNotDefined[nsName] = append(replicaNodePartitionNotDefined[nsName], pIndex)
					}
				}
			}
		}
	}

	if len(errList) > 0 || len(masterNodePartitionNotDefined) > 0 || len(replicaNodePartitionNotDefined) > 0 {
		for nsName, partitionList := range masterNodePartitionNotDefined {
			errList = append(errList, fmt.Errorf("Master partition nodes not defined for namespace `%s`: %d out of %d", nsName, len(partitionList), _PARTITIONS))
		}

		for nsName, partitionList := range replicaNodePartitionNotDefined {
			errList = append(errList, fmt.Errorf("Replica partition nodes not defined for namespace `%s`: %d out of %d", nsName, len(partitionList), _PARTITIONS))
		}

		errList = append(errList, errors.New("Partition map errors normally occur when the cluster has partitioned due to network anomaly or node crash, or is not configured properly. Refer to https://www.aerospike.com/docs/operations/configure for more information."))
		return NewAerospikeError(INVALID_CLUSTER_PARTITION_MAP, mergeErrors(errList).Error())
	}

	return nil
}

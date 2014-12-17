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

import (
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

type tendCommand int

const (
	_TEND_CMD_CLOSE tendCommand = iota
	_TEND_MSG_CLOSED
)

const (
	tendInterval = 1 * time.Second
)

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
type Cluster struct {
	// Initial host nodes specified by user.
	seeds []*Host

	// All aliases for all nodes in cluster.
	aliases map[Host]*Node

	// Active nodes in cluster.
	nodes []*Node

	// Hints for best node for a partition
	partitionWriteMap map[string][]*Node

	// Random node index.
	nodeIndex *AtomicInt

	// Size of node's connection pool.
	connectionQueueSize int

	// Initial connection timeout.
	connectionTimeout time.Duration

	mutex       sync.RWMutex
	tendChannel chan tendCommand
	closed      AtomicBool
}

// NewCluster generates a Cluster instance.
func NewCluster(policy *ClientPolicy, hosts []*Host) (*Cluster, error) {
	newCluster := &Cluster{
		seeds:               hosts,
		connectionQueueSize: policy.ConnectionQueueSize,
		connectionTimeout:   policy.Timeout,
		aliases:             make(map[Host]*Node),
		nodes:               []*Node{},
		partitionWriteMap:   make(map[string][]*Node),
		nodeIndex:           NewAtomicInt(0),
		tendChannel:         make(chan tendCommand),
	}

	// try to seed connections for first use
	newCluster.waitTillStabilized()

	// apply policy rules
	if policy.FailIfNotConnected && !newCluster.IsConnected() {
		return nil, fmt.Errorf("Failed to connect to host(s): %v", hosts)
	}

	// start up cluster maintenance go routine
	go newCluster.clusterBoss()

	Logger.Debug("New cluster initialized and ready to be used...")
	return newCluster, nil
}

// Maintains the cluster on intervals.
// All clean up code for cluster is here as well.
func (clstr *Cluster) clusterBoss() {

Loop:
	for {
		select {
		case cmd := <-clstr.tendChannel:
			switch cmd {
			case _TEND_CMD_CLOSE:
				break Loop
			}
		case <-time.After(tendInterval):
			if err := clstr.tend(); err != nil {
				Logger.Warn(err.Error())
			}
		}
	}

	// cleanup code goes here
	clstr.closed.Set(true)

	// close the nodes
	nodeArray := clstr.GetNodes()
	for _, node := range nodeArray {
		node.Close()
	}

	clstr.tendChannel <- _TEND_MSG_CLOSED
}

// AddSeeds adds new hosts to the cluster.
// They will be added to the cluster on next tend call.
func (clstr *Cluster) AddSeeds(hosts []*Host) {
	clstr.mutex.Lock()
	clstr.seeds = append(clstr.seeds, hosts...)
	clstr.mutex.Unlock()
}

func (clstr *Cluster) getSeeds() []*Host {
	clstr.mutex.RLock()
	seeds := clstr.seeds
	clstr.mutex.RUnlock()
	return seeds
}

// Updates cluster state
func (clstr *Cluster) tend() error {
	nodes := clstr.GetNodes()

	// All node additions/deletions are performed in tend goroutine.
	// If active nodes don't exist, seed cluster.
	if len(nodes) == 0 {
		Logger.Info("No connections available; seeding...")
		clstr.seedNodes()

		// refresh nodes list after seeding
		nodes = clstr.GetNodes()
	}

	// Refresh all known nodes.
	friendList := []*Host{}
	refreshCount := 0

	// Clear node reference counts.
	for _, node := range nodes {
		node.referenceCount = 0
		node.responded = false

		if node.IsActive() {
			if friends, err := node.Refresh(); err != nil {
				Logger.Warn("Node `%s` refresh failed: %s", node, err)
			} else {
				refreshCount++
				if friends != nil {
					friendList = append(friendList, friends...)
				}
			}
		}
	}

	// Add nodes in a batch.
	if addList := clstr.findNodesToAdd(friendList); len(addList) > 0 {
		clstr.addNodes(addList)
	}

	// IMPORTANT: Remove must come after add to remove aliases
	// Handle nodes changes determined from refreshes.
	// Remove nodes in a batch.
	if removeList := clstr.findNodesToRemove(refreshCount); len(removeList) > 0 {
		clstr.removeNodes(removeList)
	}

	Logger.Info("Tend finished. Live node count: %d", len(clstr.GetNodes()))
	return nil
}

// Tend the cluster until it has stabilized and return control.
// This helps avoid initial database request timeout issues when
// a large number of threads are initiated at client startup.
//
// If the cluster has not stabilized by the timeout, return
// control as well.  Do not return an error since future
// database requests may still succeed.
func (clstr *Cluster) waitTillStabilized() {
	count := -1

	doneCh := make(chan bool)

	// will run until the cluster is stablized
	go func() {
		for {
			if err := clstr.tend(); err != nil {
				Logger.Warn(err.Error())
			}

			// Check to see if cluster has changed since the last Tend().
			// If not, assume cluster has stabilized and return.
			if count == len(clstr.GetNodes()) {
				break
			}

			time.Sleep(time.Millisecond)

			count = len(clstr.GetNodes())
		}
		doneCh <- true
	}()

	// returns either on timeout or on cluster stablization
	timeout := time.After(clstr.connectionTimeout)
	select {
	case <-timeout:
		return
	case <-doneCh:
		return
	}
}

func (clstr *Cluster) findAlias(alias *Host) *Node {
	clstr.mutex.RLock()
	nd := clstr.aliases[*alias]
	clstr.mutex.RUnlock()
	return nd
}

func (clstr *Cluster) setPartitions(partMap map[string][]*Node) {
	clstr.mutex.Lock()
	clstr.partitionWriteMap = partMap
	clstr.mutex.Unlock()
}

func (clstr *Cluster) getPartitions() map[string][]*Node {
	clstr.mutex.RLock()
	res := clstr.partitionWriteMap
	clstr.mutex.RUnlock()
	return res
}

func (clstr *Cluster) updatePartitions(conn *Connection, node *Node) error {
	// TODO: Cluster should not care about version of tokenizer
	// decouple clstr interface
	var nmap map[string][]*Node
	if node.useNewInfo {
		Logger.Info("Updating partitions using new protocol...")
		tokens, err := newPartitionTokenizerNew(conn)
		if err != nil {
			return err
		}
		nmap, err = tokens.UpdatePartition(clstr.getPartitions(), node)
		if err != nil {
			return err
		}
	} else {
		Logger.Info("Updating partitions using old protocol...")
		tokens, err := newPartitionTokenizerOld(conn)
		if err != nil {
			return err
		}
		nmap, err = tokens.UpdatePartition(clstr.getPartitions(), node)
		if err != nil {
			return err
		}
	}

	// update partition write map
	if nmap != nil {
		clstr.setPartitions(nmap)
	}

	Logger.Info("Partitions updated...")
	return nil
}

// Adds seeds to the cluster
func (clstr *Cluster) seedNodes() {
	// Must copy array reference for copy on write semantics to work.
	seedArray := clstr.getSeeds()

	Logger.Info("Seeding the cluster. Seeds count: %d", len(seedArray))

	// Add all nodes at once to avoid copying entire array multiple times.
	list := []*Node{}

	for _, seed := range seedArray {
		seedNodeValidator, err := newNodeValidator(seed, clstr.connectionTimeout)
		if err != nil {
			Logger.Warn("Seed %s failed: %s", seed.String(), err.Error())
			continue
		}

		var nv *nodeValidator
		// Seed host may have multiple aliases in the case of round-robin dns configurations.
		for _, alias := range seedNodeValidator.aliases {

			if *alias == *seed {
				nv = seedNodeValidator
			} else {
				nv, err = newNodeValidator(alias, clstr.connectionTimeout)
				if err != nil {
					Logger.Warn("Seed %s failed: %s", seed.String(), err.Error())
					continue
				}
			}

			if !clstr.findNodeName(list, nv.name) {
				node := clstr.createNode(nv)
				clstr.addAliases(node)
				list = append(list, node)
			}
		}
	}

	if len(list) > 0 {
		clstr.addNodesCopy(list)
	}
}

// Finds a node by name in a list of nodes
func (clstr *Cluster) findNodeName(list []*Node, name string) bool {
	for _, node := range list {
		if node.GetName() == name {
			return true
		}
	}
	return false
}

func (clstr *Cluster) addAlias(host *Host, node *Node) {
	if host != nil && node != nil {
		clstr.mutex.Lock()
		clstr.aliases[*host] = node
		clstr.mutex.Unlock()
	}
}

func (clstr *Cluster) removeAlias(alias *Host) {
	if alias != nil {
		clstr.mutex.Lock()
		delete(clstr.aliases, *alias)
		clstr.mutex.Unlock()
	}
}

func (clstr *Cluster) findNodesToAdd(hosts []*Host) []*Node {
	list := make([]*Node, 0, len(hosts))

	for _, host := range hosts {
		if nv, err := newNodeValidator(host, clstr.connectionTimeout); err != nil {
			Logger.Warn("Add node %s failed: %s", err.Error())
		} else {
			node := clstr.findNodeByName(nv.name)
			// make sure node is not already in the list to add
			if node == nil {
				for _, n := range list {
					if n.GetName() == nv.name {
						node = n
						break
					}
				}
			}

			if node != nil {
				// Duplicate node name found.  This usually occurs when the server
				// services list contains both internal and external IP addresses
				// for the same node.  Add new host to list of alias filters
				// and do not add new node.
				node.referenceCount++
				node.AddAlias(host)
				clstr.addAlias(host, node)
				continue
			}
			node = clstr.createNode(nv)
			list = append(list, node)
		}
	}
	return list
}

func (clstr *Cluster) createNode(nv *nodeValidator) *Node {
	return newNode(clstr, nv)
}

func (clstr *Cluster) findNodesToRemove(refreshCount int) []*Node {
	nodes := clstr.GetNodes()

	removeList := []*Node{}

	for _, node := range nodes {
		if !node.IsActive() {
			// Inactive nodes must be removed.
			removeList = append(removeList, node)
			continue
		}

		switch len(nodes) {
		case 1:
			// Single node clusters rely solely on node health.
			if node.IsUnhealthy() {
				removeList = append(removeList, node)
			}

		case 2:
			// Two node clusters require at least one successful refresh before removing.
			if node.refreshCount > 0 && refreshCount == 1 && node.referenceCount == 0 && !node.responded {
				// Node is not referenced nor did it respond.
				removeList = append(removeList, node)
			}

		default:
			// Multi-node clusters require two successful node refreshes before removing.
			if refreshCount >= 2 && node.referenceCount == 0 {
				// Node is not referenced by other nodes.
				// Check if node responded to info request.
				if node.responded {
					// Node is alive, but not referenced by other nodes.  Check if mapped.
					if !clstr.findNodeInPartitionMap(node) {
						// Node doesn't have any partitions mapped to it.
						// There is not point in keeping it in the cluster.
						removeList = append(removeList, node)
					}
				} else {
					// Node not responding. Remove it.
					removeList = append(removeList, node)
				}
			}
		}
	}
	return removeList
}

func (clstr *Cluster) findNodeInPartitionMap(filter *Node) bool {
	partitions := clstr.getPartitions()

	for j := range partitions {
		max := len(partitions[j])

		for i := 0; i < max; i++ {
			node := partitions[j][i]
			// Use reference equality for performance.
			if node == filter {
				return true
			}
		}
	}
	return false
}

func (clstr *Cluster) addNodes(nodesToAdd []*Node) {
	// Add all nodes at once to avoid copying entire array multiple times.
	for _, node := range nodesToAdd {
		clstr.addAliases(node)
	}
	clstr.addNodesCopy(nodesToAdd)
}

func (clstr *Cluster) addAliases(node *Node) {
	// Add node's aliases to global alias set.
	// Aliases are only used in tend goroutine, so synchronization is not necessary.
	for _, alias := range node.GetAliases() {
		clstr.aliases[*alias] = node
	}
}

func (clstr *Cluster) addNodesCopy(nodesToAdd []*Node) {
	clstr.mutex.Lock()
	clstr.nodes = append(clstr.nodes, nodesToAdd...)
	clstr.mutex.Unlock()
}

func (clstr *Cluster) removeNodes(nodesToRemove []*Node) {
	// There is no need to delete nodes from partitionWriteMap because the nodes
	// have already been set to inactive. Further connection requests will result
	// in an exception and a different node will be tried.

	// Cleanup node resources.
	for _, node := range nodesToRemove {
		// Remove node's aliases from cluster alias set.
		// Aliases are only used in tend goroutine, so synchronization is not necessary.
		for _, alias := range node.GetAliases() {
			Logger.Debug("Removing alias ", alias)
			clstr.removeAlias(alias)
		}
		go node.Close()
	}

	// Remove all nodes at once to avoid copying entire array multiple times.
	clstr.removeNodesCopy(nodesToRemove)
}

func (clstr *Cluster) setNodes(nodes []*Node) {
	clstr.mutex.Lock()
	// Replace nodes with copy.
	clstr.nodes = nodes
	clstr.mutex.Unlock()
}

func (clstr *Cluster) removeNodesCopy(nodesToRemove []*Node) {
	// Create temporary nodes array.
	// Since nodes are only marked for deletion using node references in the nodes array,
	// and the tend goroutine is the only goroutine modifying nodes, we are guaranteed that nodes
	// in nodesToRemove exist.  Therefore, we know the final array size.
	nodes := clstr.GetNodes()
	nodeArray := make([]*Node, len(nodes)-len(nodesToRemove))
	count := 0

	// Add nodes that are not in remove list.
	for _, node := range nodes {
		if clstr.nodeExists(node, nodesToRemove) {
			Logger.Info("Removed node `%s`", node)
		} else {
			nodeArray[count] = node
			count++
		}
	}

	// Do sanity check to make sure assumptions are correct.
	if count < len(nodeArray) {
		Logger.Warn(fmt.Sprintf("Node remove mismatch. Expected %d, Received %d", len(nodeArray), count))

		// Resize array.
		nodeArray2 := make([]*Node, count)
		copy(nodeArray2, nodeArray)
		nodeArray = nodeArray2
	}

	clstr.setNodes(nodeArray)
}

func (clstr *Cluster) nodeExists(search *Node, nodeList []*Node) bool {
	for _, node := range nodeList {
		if node.Equals(search) {
			return true
		}
	}
	return false
}

// IsConnected returns true if cluster has nodes and is not already closed.
func (clstr *Cluster) IsConnected() bool {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	return (len(nodeArray) > 0) && !clstr.closed.Get()
}

// GetNode returns a node for the provided partition.
func (clstr *Cluster) GetNode(partition *Partition) (*Node, error) {
	// Must copy hashmap reference for copy on write semantics to work.
	nmap := clstr.getPartitions()
	if nodeArray, exists := nmap[partition.Namespace]; exists {
		node := nodeArray[partition.PartitionId]

		if node != nil && node.IsActive() {
			return node, nil
		}
	}
	return clstr.GetRandomNode()
}

// GetRandomNode returns a random node on the cluster
func (clstr *Cluster) GetRandomNode() (*Node, error) {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	length := len(nodeArray)
	for i := 0; i < length; i++ {
		// Must handle concurrency with other non-tending goroutines, so nodeIndex is consistent.
		index := int(math.Abs(float64(clstr.nodeIndex.GetAndIncrement() % length)))
		node := nodeArray[index]

		if node.IsActive() {
			// Logger.Debug("Node `%s` is active. index=%d", node, index)
			return node, nil
		}
	}
	return nil, NewAerospikeError(INVALID_NODE_ERROR)
}

// GetNodes returns a list of all nodes in the cluster
func (clstr *Cluster) GetNodes() []*Node {
	clstr.mutex.RLock()
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.nodes
	clstr.mutex.RUnlock()
	return nodeArray
}

// GetNodeByName finds a node by name and returns an
// error if the node is not found.
func (clstr *Cluster) GetNodeByName(nodeName string) (*Node, error) {
	node := clstr.findNodeByName(nodeName)

	if node == nil {
		return nil, NewAerospikeError(INVALID_NODE_ERROR)
	}
	return node, nil
}

func (clstr *Cluster) findNodeByName(nodeName string) *Node {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()

	for _, node := range nodeArray {
		if node.GetName() == nodeName {
			return node
		}
	}
	return nil
}

// Close closes all cached connections to the cluster nodes
// and stops the tend goroutine.
func (clstr *Cluster) Close() {
	if !clstr.closed.Get() {
		// send close signal to maintenance channel
		clstr.tendChannel <- _TEND_CMD_CLOSE

		// wait until tendChannel returns
		<-clstr.tendChannel
	}
}

// MigrationInProgress determines if any node in the cluster
// is participating in a data migration
func (clstr *Cluster) MigrationInProgress(timeout time.Duration) (res bool, err error) {
	if timeout <= 0 {
		timeout = _DEFAULT_TIMEOUT
	}

	done := make(chan bool)

	go func() {
		// this function is guaranteed to return after _DEFAULT_TIMEOUT
		nodes := clstr.GetNodes()
		for _, node := range nodes {
			if node.IsActive() {
				if res, err = node.MigrationInProgress(); res || err != nil {
					done <- true
					return
				}
			}
		}

		res, err = false, nil
		done <- false
	}()

	dealine := time.After(timeout)
	for {
		select {
		case <-dealine:
			return false, NewAerospikeError(TIMEOUT)
		case <-done:
			return res, err
		}
	}
}

// WaitUntillMigrationIsFinished will block until all
// migration operations in the cluster all finished.
func (clstr *Cluster) WaitUntillMigrationIsFinished(timeout time.Duration) (err error) {
	if timeout <= 0 {
		timeout = _NO_TIMEOUT
	}
	done := make(chan error)

	go func() {
		// this function is guaranteed to return after timeout
		// no go routines will be leaked
		for {
			if res, err := clstr.MigrationInProgress(timeout); err != nil || !res {
				done <- err
				return
			}
		}
	}()

	dealine := time.After(timeout)
	select {
	case <-dealine:
		return NewAerospikeError(TIMEOUT)
	case err = <-done:
		return err
	}
}

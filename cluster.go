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
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/citrusleaf/aerospike-client-go/logger"

	. "github.com/citrusleaf/aerospike-client-go/types"
	. "github.com/citrusleaf/aerospike-client-go/types/atomic"
)

type tendCommand int

const (
	_TEND_CMD_CLOSE tendCommand = iota
	_TEND_MSG_CLOSED
)

const (
	tendInterval = 1 * time.Second
)

type atomicNodeArray struct {
	AtomicArray
}

type Cluster struct {
	// Initial host nodes specified by user.
	seeds []*Host

	// All aliases for all nodes in cluster.
	aliases map[*Host]*Node

	// Active nodes in cluster.
	nodes []*Node

	// Hints for best node for a partition
	partitionWriteMap map[string]atomicNodeArray

	// Random node index.
	nodeIndex *AtomicInt

	// Size of node's connection pool.
	connectionQueueSize int

	// Initial connection timeout.
	connectionTimeout time.Duration

	mutex       *sync.RWMutex
	tendChannel chan tendCommand
	closed      AtomicBool
}

func NewCluster(policy *ClientPolicy, hosts []*Host) (*Cluster, error) {
	newCluster := &Cluster{
		seeds:               hosts,
		connectionQueueSize: policy.ConnectionQueueSize, // Add one connection for tend thread.
		connectionTimeout:   policy.Timeout,
		aliases:             make(map[*Host]*Node),
		nodes:               []*Node{},
		partitionWriteMap:   make(map[string]atomicNodeArray),
		nodeIndex:           NewAtomicInt(0),
		mutex:               new(sync.RWMutex),
		tendChannel:         make(chan tendCommand),
	}

	// try to seed connections for first use
	newCluster.tend()

	// apply policy rules
	if policy.FailIfNotConnected && !newCluster.IsConnected() {
		return nil, errors.New(fmt.Sprintf("Failed to connect to host(s): %v", hosts))
	}

	// start up cluster maintenance go routine
	go newCluster.clusterBoss()

	Logger.Debug("New cluster initialized and ready to be used...")
	return newCluster, nil
}

// Maintains the cluster on intervals.
// All clean up code for cluster is here as well.
func (this *Cluster) clusterBoss() {

Loop:
	for {
		select {
		case cmd := <-this.tendChannel:
			switch cmd {
			case _TEND_CMD_CLOSE:
				break Loop
			}
		case <-time.After(tendInterval):
			this.tend()
		}
	}

	// cleanup code goes here
	this.closed.Set(true)

	// close the nodes asynchronously
	go func() {
		nodeArray := this.GetNodes()
		for _, node := range nodeArray {
			node.Close()
		}
	}()

	this.tendChannel <- _TEND_MSG_CLOSED
}

// Adds new hosts to the cluster
// They will be added to the cluster on next tend
func (this *Cluster) AddSeeds(hosts []*Host) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.seeds = append(this.seeds, hosts...)
}

func (this *Cluster) getSeeds() []*Host {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	seeds := this.seeds
	return seeds
}

// TODO: name doesn't imply return type
func (this *Cluster) findSeed(search *Host) bool {
	tmpSeeds := this.getSeeds()
	for _, seed := range tmpSeeds {
		if *seed == *search {
			return true
		}
	}
	return false
}

// Updates cluster state
func (this *Cluster) tend() error {
	nodes := this.GetNodes()

	// All node additions/deletions are performed in tend thread.
	// If active nodes don't exist, seed cluster.
	if len(nodes) == 0 {
		Logger.Info("No connections available; seeding...")
		this.seedNodes()
	}

	// Clear node reference counts.
	for _, node := range nodes {
		node.referenceCount.Set(0)
		node.responded.Set(false)
	}

	// Refresh all known nodes.
	friendList := []*Host{}
	refreshCount := 0

	for _, node := range nodes {
		if node.IsActive() {
			if err := node.Refresh(friendList); err == nil {
				refreshCount++
			} else {
				Logger.Warn("Node `%s` refresh failed: %s", node, err)
			}
		}
	}

	// Handle nodes changes determined from refreshes.
	// Remove nodes in a batch.
	if removeList := this.findNodesToRemove(refreshCount); len(removeList) > 0 {
		this.removeNodes(removeList)
	}

	// Add nodes in a batch.
	if addList := this.findNodesToAdd(friendList); len(addList) > 0 {
		this.addNodes(addList)
	}

	Logger.Info("Tend finished. Live node count: %d", len(this.GetNodes()))

	return nil
}

func (this *Cluster) findAlias(alias *Host) *Node {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.aliases[alias]
}

func (this *Cluster) setPartitions(partMap map[string]atomicNodeArray) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.partitionWriteMap = partMap
}

func (this *Cluster) getPartitions() map[string]atomicNodeArray {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	partMap := this.partitionWriteMap
	return partMap
}

func (this *Cluster) updatePartitions(conn *Connection, node *Node) error {
	// TODO: Cluster should not care about version of tokenizer
	// decouple this interface
	var nmap map[string]atomicNodeArray
	if node.getUseNewInfo() {
		if tokens, err := NewPartitionTokenizerNew(conn); err != nil {
			return err
		} else {
			nmap = tokens.UpdatePartition(this.getPartitions(), node)
		}
	} else {
		if tokens, err := NewPartitionTokenizerOld(conn); err != nil {
			return err
		} else {
			nmap = tokens.UpdatePartition(this.getPartitions(), node)
		}
	}

	// update partition write map
	if nmap != nil {
		this.setPartitions(nmap)
	}
	return nil
}

// Adds seeds to the cluster
func (this *Cluster) seedNodes() {
	// Must copy array reference for copy on write semantics to work.
	seedArray := this.getSeeds()

	Logger.Info("Seeding the cluster. Seeds count: %d", len(seedArray))

	// Add all nodes at once to avoid copying entire array multiple times.
	list := []*Node{}

	for _, seed := range seedArray {
		seedNodeValidator, err := NewNodeValidator(seed, this.connectionTimeout)
		if err != nil {
			Logger.Info("Seed %s failed: %s", seed.String(), err.Error())
		}

		var nv *NodeValidator
		// Seed host may have multiple aliases in the case of round-robin dns configurations.
		for _, alias := range seedNodeValidator.aliases {

			if *alias == *seed {
				nv = seedNodeValidator
			} else {
				nv, err = NewNodeValidator(alias, this.connectionTimeout)
				if err != nil {
					Logger.Warn("Seed %s failed: %s", seed.String(), err.Error())
				}
			}

			if !this.FindNodeName(list, nv.name) {
				node := this.createNode(nv)
				this.addAliases(node)
				list = append(list, node)
			}
		}
	}

	if len(list) > 0 {
		this.addNodesCopy(list)
	}
}

// FIXIT: This function is not well desined while it is expoted.
// Finds a node by name in a list of nodes
func (this *Cluster) FindNodeName(list []*Node, name string) bool {
	for _, node := range list {
		if node.GetName() == name {
			return true
		}
	}
	return false
}

func (this *Cluster) addAlias(host *Host, node *Node) error {
	if host == nil || node == nil {
		return errors.New("To add an alias, both host and node must be non-nil")
	} else {
		this.mutex.Lock()
		defer this.mutex.Unlock()
		this.aliases[host] = node
		return nil
	}
}

func (this *Cluster) removeAlias(alias *Host) error {
	if alias == nil {
		return errors.New("alias pointer cannot be nil")
	} else {
		this.mutex.Lock()
		defer this.mutex.Unlock()
		delete(this.aliases, alias)
		return nil
	}
}

func (this *Cluster) findNodesToAdd(hosts []*Host) []*Node {
	list := make([]*Node, len(hosts))

	for _, host := range hosts {
		if nv, err := NewNodeValidator(host, this.connectionTimeout); err != nil {
			Logger.Warn("Add node %s failed: %s", err.Error())
		} else {
			node := this.findNodeByName(nv.name)

			if node != nil {
				// Duplicate node name found.  This usually occurs when the server
				// services list contains both internal and external IP addresses
				// for the same node.  Add new host to list of alias filters
				// and do not add new node.
				// TODO: Sync?
				node.referenceCount.IncrementAndGet()
				node.AddAlias(host)
				this.addAlias(host, node)
				continue
			}
			node = this.createNode(nv)
			list = append(list, node)
		}
	}
	return list
}

func (this *Cluster) createNode(nv *NodeValidator) *Node {
	return NewNode(this, nv)
}

func (this *Cluster) findNodesToRemove(refreshCount int) []*Node {
	nodes := this.GetNodes()

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
			break

		case 2:
			// Two node clusters require at least one successful refresh before removing.
			if refreshCount == 1 && node.referenceCount.Get() == 0 && !node.responded.Get() {
				// Node is not referenced nor did it respond.
				removeList = append(removeList, node)
			}
			break

		default:
			// Multi-node clusters require two successful node refreshes before removing.
			if refreshCount >= 2 && node.referenceCount.Get() == 0 {
				// Node is not referenced by other nodes.
				// Check if node responded to info request.
				if node.responded.Get() {
					// Node is alive, but not referenced by other nodes.  Check if mapped.
					if !this.findNodeInPartitionMap(node) {
						// Node doesn't have any partitions mapped to it.
						// There is not point in keeping it in the cluster.
						removeList = append(removeList, node)
					}
				} else {
					// Node not responding. Remove it.
					removeList = append(removeList, node)
				}
			}
			break
		}
	}
	return removeList
}

func (this *Cluster) findNodeInPartitionMap(filter *Node) bool {
	partitions := this.getPartitions()

	for _, nodeArray := range partitions {
		max := nodeArray.Length()

		for i := 0; i < max; i++ {
			node := nodeArray.Get(i)
			// Use reference equality for performance.
			if node == filter {
				return true
			}
		}
	}
	return false
}

func (this *Cluster) addNodes(nodesToAdd []*Node) {
	// Add all nodes at once to avoid copying entire array multiple times.
	for _, node := range nodesToAdd {
		this.addAliases(node)
	}
	this.addNodesCopy(nodesToAdd)
}

func (this *Cluster) addAliases(node *Node) {
	// Add node's aliases to global alias set.
	// Aliases are only used in tend thread, so synchronization is not necessary.
	for _, alias := range node.GetAliases() {
		this.aliases[alias] = node
	}
}

func (this *Cluster) addNodesCopy(nodesToAdd []*Node) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.nodes = append(this.nodes, nodesToAdd...)
}

func (this *Cluster) removeNodes(nodesToRemove []*Node) {
	// TODO: Why not? This at least counts as a mem leak;
	//    especially if too many nodes are queried, or equality is not correct
	//    and duplicate nodes are added
	// There is no need to delete nodes from partitionWriteMap because the nodes
	// have already been set to inactive. Further connection requests will result
	// in an exception and a different node will be tried.

	// Cleanup node resources.
	for _, node := range nodesToRemove {
		// Remove node's aliases from cluster alias set.
		// Aliases are only used in tend thread, so synchronization is not necessary.
		for _, alias := range node.GetAliases() {
			Logger.Debug("Removing alias ", alias)
			this.removeAlias(alias)
		}
		go node.Close()
	}

	// Remove all nodes at once to avoid copying entire array multiple times.
	this.removeNodesCopy(nodesToRemove)
}

func (this *Cluster) setNodes(nodes []*Node) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	// Replace nodes with copy.
	this.nodes = nodes
}

func (this *Cluster) removeNodesCopy(nodesToRemove []*Node) {
	// Create temporary nodes array.
	// Since nodes are only marked for deletion using node references in the nodes array,
	// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
	// in nodesToRemove exist.  Therefore, we know the final array size.
	// TODO: no check on final size; no guarantees it will be > 0 ?
	nodes := this.GetNodes()
	nodeArray := make([]*Node, len(nodes)-len(nodesToRemove))
	count := 0

	// Add nodes that are not in remove list.
	for _, node := range nodes {
		if this.findNode(node, nodesToRemove) {
			Logger.Info("Removed node `%s`", node)
		} else {
			count++
			nodeArray[count] = node
		}
	}

	// TODO: mismatch probably due to lack of synchronization; this should never happen
	// // Do sanity check to make sure assumptions are correct.
	// if count < len(nodeArray) {
	// 	Logger.Warn(fmt.Sprintf("Node remove mismatch. Expected %s, Received %s", len(nodeArray), count))

	// 	// Resize array.
	// 	nodeArray2 := make([]*Node, count)
	// 	copy(nodeArray2, nodeArray)
	// 	nodeArray = nodeArray2
	// }

	this.setNodes(nodeArray)
}

// TODO: name doesn't imply returned value
func (this *Cluster) findNode(search *Node, nodeList []*Node) bool {
	for _, node := range nodeList {
		if node.Equals(search) {
			return true
		}
	}
	return false
}

func (this *Cluster) IsConnected() bool {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := this.GetNodes()
	return (len(nodeArray) > 0) && !this.closed.Get()
}

func (this *Cluster) GetNode(partition *Partition) (*Node, error) {
	// Must copy hashmap reference for copy on write semantics to work.
	nmap := this.getPartitions()
	if nodeArray, exists := nmap[partition.Namespace]; exists {
		node := nodeArray.Get(partition.PartitionId).(*Node)

		if node != nil && node.IsActive() {
			return node, nil
		}
	}
	return this.GetRandomNode()
}

// Returns a random node on the cluster
func (this *Cluster) GetRandomNode() (*Node, error) {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := this.GetNodes()

	for i := 0; i < len(nodeArray); i++ {
		// Must handle concurrency with other non-tending threads, so nodeIndex is consistent.
		index := int(math.Abs(float64(this.nodeIndex.GetAndIncrement() % len(nodeArray))))
		node := nodeArray[index]

		if node.IsActive() {
			Logger.Debug("Node `%s` is active. index=%d", node, index)
			return node, nil
		}
	}
	return nil, InvalidNodeErr()
}

// Returns a list of all nodes in the cluster
func (this *Cluster) GetNodes() []*Node {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	// Must copy array reference for copy on write semantics to work.
	nodeArray := this.nodes
	return nodeArray
}

// Find a node by name and returns an error if not found
func (this *Cluster) GetNodeByName(nodeName string) (*Node, error) {
	node := this.findNodeByName(nodeName)

	if node == nil {
		return nil, errors.New("Invalid node")
	}
	return node, nil
}

func (this *Cluster) findNodeByName(nodeName string) *Node {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := this.GetNodes()

	for _, node := range nodeArray {
		if node.GetName() == nodeName {
			return node
		}
	}
	return nil
}

// Closes all cached connections to the cluster nodes and stops the tend goroutine
func (this *Cluster) Close() {
	if !this.closed.Get() {
		// send close signal to maintenance channel
		this.tendChannel <- _TEND_CMD_CLOSE

		// wait until tendChannel returns
		<-this.tendChannel
	}
}

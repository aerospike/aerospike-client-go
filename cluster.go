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
	"fmt"
	"maps"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	sm "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
	"github.com/aerospike/aerospike-client-go/v8/internal/seq"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const aesModule = "github.com/aerospike/aerospike-client-go/v8"

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
type Cluster struct {
	// Initial host nodes specified by user.
	seeds iatomic.SyncVal[[]*Host]

	// All aliases for all nodes in cluster.
	// Only accessed within cluster tend goroutine.
	aliases sm.Map[Host, *Node]

	// Map of active nodes in cluster.
	// Only accessed within cluster tend goroutine.
	nodesMap sm.Map[string, *Node]

	// Active nodes in cluster.
	nodes     iatomic.SyncVal[[]*Node]
	stats     map[string]*nodeStats //host => stats
	statsLock sync.Mutex

	// enable performance metrics
	metricsEnabled atomic.Bool // bool
	metricsPolicy  iatomic.TypedVal[*MetricsPolicy]

	// Hints for best node for a partition
	partitionWriteMap iatomic.TypedVal[partitionMap] //partitionMap

	clientPolicy        *atomic.Pointer[ClientPolicy]
	infoPolicy          InfoPolicy
	connectionThreshold iatomic.Int // number of parallel opening connections

	maxRetriesExceededCount   iatomic.Int // number of times the commands on this cluster were exceeded the specifiedmax retries
	totalTimeoutExceededCount iatomic.Int // number of times the commands on this cluster were exceeded the specified total timeout

	nodeIndex    iatomic.Int // only used via atomic operations
	replicaIndex iatomic.Int // only used via atomic operations

	wgTend      sync.WaitGroup
	tendChannel chan struct{}
	closed      iatomic.Bool
	tendCount   int

	supportsPartitionQuery iatomic.Bool // whether all nodes in the cluster support query by partition.

	// User name in UTF-8 encoded bytes.
	user string

	// Password in hashed format in bytes.
	password iatomic.SyncVal[[]byte]

	// Cluster max error count for the nodes
	maxErrorCount iatomic.Int

	// User agent id
	// Leaving this at cluster level since cluster is visible to nodes
	// which will need this information when sending user-agent to the server.
	clientModuleVersion string // e.g. v8.0.0, v8.1.0, etc."
}

// NewCluster generates a Cluster instance.
func NewCluster(policy *atomic.Pointer[ClientPolicy], hosts []*Host) (*Cluster, Error) {
	loadedPolicy := policy.Load()
	// Validate the policy params
	if loadedPolicy.MinConnectionsPerNode > loadedPolicy.ConnectionQueueSize {
		panic("minimum number of connections specified in the ClientPolicy is bigger than total connection pool size")
	}

	// Default TLS names when TLS enabled.
	newHosts := make([]*Host, 0, len(hosts))
	if loadedPolicy.TlsConfig != nil && !loadedPolicy.TlsConfig.InsecureSkipVerify {
		useClusterName := len(loadedPolicy.ClusterName) > 0

		for _, host := range hosts {
			nh := *host
			if nh.TLSName == "" {
				if useClusterName {
					nh.TLSName = loadedPolicy.ClusterName
				} else {
					nh.TLSName = host.Name
				}
			}
			newHosts = append(newHosts, &nh)
		}
		hosts = newHosts
	}

	// Set a default Idle Timeout for the connection
	if loadedPolicy.IdleTimeout <= 0 {
		loadedPolicy.IdleTimeout = 55 * time.Second
	}

	newCluster := &Cluster{
		infoPolicy:   InfoPolicy{Timeout: loadedPolicy.Timeout},
		tendChannel:  make(chan struct{}),
		clientPolicy: policy,

		seeds:    *iatomic.NewSyncVal(hosts),
		aliases:  *sm.New[Host, *Node](16),
		nodesMap: *sm.New[string, *Node](16),
		nodes:    *iatomic.NewSyncVal([]*Node{}),
		stats:    map[string]*nodeStats{},

		password: *iatomic.NewSyncVal[[]byte](nil),

		supportsPartitionQuery: *iatomic.NewBool(false),
		clientModuleVersion:    getLibraryVersion(aesModule),
	}
	newCluster.maxErrorCount.Set(loadedPolicy.MaxErrorRate)

	newCluster.partitionWriteMap.Set(make(partitionMap))

	// setup auth info for cluster
	if loadedPolicy.RequiresAuthentication() {
		if loadedPolicy.AuthMode == AuthModeExternal && loadedPolicy.TlsConfig == nil {
			return nil, newError(types.PARAMETER_ERROR, "External Authentication requires TLS configuration to be set, because it sends clear password on the wire.")
		}

		// If PKI authentication is used and user is attempting to set password, return an error
		if loadedPolicy.AuthMode == AuthModePKI && (loadedPolicy.User != "" || loadedPolicy.Password != "") {
			return nil, newError(types.FORBIDDEN_PASSWORD, "Password authentication is disabled for PKI-only users. Please authenticate using your certificate.")
		}

		newCluster.user = loadedPolicy.User
		hashedPass, err := hashPassword(loadedPolicy.Password)
		if err != nil {
			return nil, err
		}
		newCluster.password = *iatomic.NewSyncVal(hashedPass)
	}

	// try to seed connections for first use
	err := newCluster.waitTillStabilized()

	// apply policy rules
	if loadedPolicy.FailIfNotConnected && !newCluster.IsConnected() {
		if err != nil {
			return nil, err
		}
		return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("Failed to connect to host(s): %v. The network connection(s) to cluster nodes may have timed out, or the cluster may be in a state of flux.", hosts))
	}

	// start up cluster maintenance go routine
	newCluster.wgTend.Add(1)
	go newCluster.clusterBoss()

	if err == nil {
		logger.Logger.Debug("New cluster initialized and ready to be used...")
	} else {
		logger.Logger.Error("New cluster was not initialized successfully, but the client will keep trying to connect to the database. Error: %s", err.Error())
	}

	return newCluster, err
}

// String implements the stringer interface
func (clstr *Cluster) String() string {
	return fmt.Sprintf("%v", clstr.GetNodes())
}

// Maintains the cluster on intervals.
// All clean up code for cluster is here as well.
func (clstr *Cluster) clusterBoss() {
	policy := clstr.clientPolicy.Load()
	logger.Logger.Info("Starting the cluster tend goroutine...")

	defer func() {
		if r := recover(); r != nil {
			logger.Logger.Error("Cluster tend goroutine crashed: %s", debug.Stack())
			go clstr.clusterBoss()
		}
	}()

	defer clstr.wgTend.Done()

	tendInterval := clstr.clientPolicy.Load().TendInterval
	if tendInterval <= 10*time.Millisecond {
		tendInterval = 10 * time.Millisecond
	}

Loop:
	for {
		select {
		case <-clstr.tendChannel:
			// tend channel closed
			logger.Logger.Debug("Tend channel closed. Shutting down the cluster...")
			break Loop
		case <-time.After(tendInterval):
			tm := time.Now()
			if err := clstr.tend(); err != nil {
				logger.Logger.Warn("%s", err.Error())
			}

			// clientPolicy := clstr.clientPolicy.Load()
			// Tending took longer than requested tend interval.
			// Tending is too slow for the cluster, and may be falling behind schedule.
			if tendDuration := time.Since(tm); tendDuration > policy.TendInterval {
				logger.Logger.Warn("Tending took %s, while your requested ClientPolicy.TendInterval is %s. Tends are slower than the interval, and may be falling behind the changes in the cluster.", tendDuration, policy.TendInterval)
			}
		}
	}

	// cleanup code goes here
	// close the nodes
	nodeArray := clstr.GetNodes()
	for _, node := range nodeArray {
		node.Close()
	}
}

// AddSeeds adds new hosts to the cluster.
// They will be added to the cluster on next tend call.
func (clstr *Cluster) AddSeeds(hosts []*Host) {
	clstr.seeds.Update(func(seeds []*Host) ([]*Host, error) {
		seeds = append(seeds, hosts...)
		return seeds, nil
	})
}

// Healthy returns an error if the cluster is not healthy.
func (clstr *Cluster) Healthy() Error {
	p := clstr.getPartitions()
	if p == nil {
		return ErrInvalidPartitionMap.err()
	}
	return p.validate()
}

// Updates cluster state
func (clstr *Cluster) tend() Error {

	nodes := clstr.GetNodes()
	nodeCountBeforeTend := len(nodes)

	// All node additions/deletions are performed in tend goroutine.
	// If active nodes don't exist, seed cluster.
	if clientPolicy := clstr.clientPolicy.Load(); len(nodes) == 0 || (clientPolicy != nil && clientPolicy.SeedOnlyCluster && len(nodes) < clstr.GetSeedCount()) {
		logger.Logger.Info("No nodes available; seeding...")
		if newNodesFound, err := clstr.seedNodes(); !newNodesFound {
			return err
		}

		// refresh nodes list after seeding
		nodes = clstr.GetNodes()
	}

	peers := newPeers(len(nodes)+16, 16)

	seq.ParDo(nodes, func(node *Node) {
		if err := node.Refresh(peers); err != nil {
			logger.Logger.Debug("Error occurred while refreshing node: %s", node.String())
		}
	})

	// Refresh peers when necessary.
	if peers.genChanged.Get() || len(peers.peers()) != nodeCountBeforeTend {
		// Refresh peers for all nodes that responded the first time even if only one node's peers changed.
		peers.refreshCount.Set(0)

		seq.ParDo(nodes, func(node *Node) {
			node.refreshPeers(peers)
		})
	}

	var partMap iatomic.Guard[partitionMap]

	// find the first host that connects
	seq.ParDo(peers.peers(), func(_peer *peer) {
		if clstr.peerExists(peers, _peer) {
			// Node already exists. Do not even try to connect to hosts.
			return
		}

		seq.Do(_peer.hosts, func(host *Host) error {
			// attempt connection to the host
			nv := nodeValidator{seedOnlyCluster: clstr.clientPolicy.Load().SeedOnlyCluster}
			if err := nv.validateNode(clstr, host); err != nil {
				logger.Logger.Warn("Add node `%s` failed: `%s`", host, err)
				return nil
			}

			// Must look for new node name in the unlikely event that node names do not agree.
			if _peer.nodeName != nv.name {
				logger.Logger.Warn("Peer node `%s` is different than actual node `%s` for host `%s`", _peer.nodeName, nv.name, host)
			}

			// Create new node.
			node := clstr.createNode(&nv)
			peers.addNode(nv.name, node)
			partMap.InitDoVal(clstr.getPartitions().clone, func(partMap partitionMap) {
				node.refreshPartitions(peers, partMap, true)
			})

			// Preventing duplicate entries.
			if _peer.replaceNode != nil && !peers.containsNodeToRemove(_peer.replaceNode) {
				peers.addNodesToRemove(_peer.replaceNode)
			}
			return seq.Break
		})
	})

	// Refresh partition map when necessary.
	seq.ParDo(nodes, func(node *Node) {
		if node.partitionChanged.Get() {
			partMap.InitDoVal(clstr.getPartitions().clone, func(partMap partitionMap) {
				node.refreshPartitions(peers, partMap, false)
			})
		}
	})

	if peers.genChanged.Get() {
		clstr.findNodesToRemove(peers)

		// Remove nodes in a batch.
		nodesToRemove := peers.getNodesToRemove()
		for i := range nodesToRemove {
			logger.Logger.Debug("The following nodes will be removed: %s", nodesToRemove[i])
		}
		clstr.removeNodes(nodesToRemove)
		clstr.aggregateNodeStats(nodesToRemove)
	}

	// Add nodes in a batch.
	clstr.addNodes(peers.nodes())

	// add to the number of successful tends
	clstr.tendCount++

	// update all partitions in one go
	updatePartitionMap := seq.Any(clstr.GetNodes(), func(node *Node) bool {
		return node.partitionChanged.Get()
	})

	if updatePartitionMap {
		clstr.setPartitions(*partMap.Release())
	}

	if err := clstr.getPartitions().validate(); err != nil {
		logger.Logger.Error("Error validating the cluster partition map after tend: %s", err.Error())
	}

	// only log if node count is changed
	if nodeCountBeforeTend != len(clstr.GetNodes()) {
		logger.Logger.Info("Tend finished. Live node count changes from %d to %d", nodeCountBeforeTend, len(clstr.GetNodes()))
	}

	clstr.aggregateNodeStats(clstr.GetNodes())

	clusterClientPolicy := *clstr.clientPolicy.Load()
	// Reset connection error window for all nodes every connErrorWindow tend iterations.
	if clusterClientPolicy.MaxErrorRate > 0 && clstr.tendCount%clusterClientPolicy.ErrorRateWindow == 0 {
		for _, node := range clstr.GetNodes() {
			node.resetErrorCount()
		}
	}

	return nil
}

func (clstr *Cluster) aggregateNodeStats(nodeList []*Node) {
	// update stats
	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()

	for _, node := range nodeList {
		h := node.host.String()
		if stats, exists := clstr.stats[h]; exists {
			nr := node.stats.getAndReset()
			stats.aggregate(nr)
		} else {
			nr := node.stats.getAndReset()
			clstr.stats[h] = nr
		}
	}
}

func (clstr *Cluster) statsCopy() map[string]nodeStats {
	// update the stats on the cluster object
	clstr.aggregateNodeStats(clstr.GetNodes())

	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()

	res := make(map[string]nodeStats, len(clstr.stats))
	for _, node := range clstr.GetNodes() {
		h := node.host.String()
		if stats, exists := clstr.stats[h]; exists {
			statsCopy := stats.clone()
			statsCopy.ConnectionsOpen.Set(node.connectionCount.Get())
			res[h] = statsCopy
		}
	}

	// stats for nodes which do not exist anymore
	for h, stats := range clstr.stats {
		if _, exists := res[h]; !exists {
			stats.ConnectionsOpen.Set(0)
			res[h] = stats.clone()
		}
	}

	return res
}

func (clstr *Cluster) peerExists(peers *peers, peer *peer) bool {
	node := clstr.findNodeByName(peer.nodeName)

	if node != nil {
		if node.failures.Get() <= 0 || node.host.IsLocalhost() {
			// If the node does not have cluster tend errors or is localhost,
			// reject new peer as the IP address does not need to change.
			node.referenceCount.IncrementAndGet()
			return true
		}

		for _, host := range peer.hosts {
			if host.Port == node.host.Port {
				if host.Name == node.host.Name || (node.hostName != "" && host.Name == node.hostName) {
					// Main node host is also the same as one of the peer hosts.
					// Peer should not be added.
					if host.IsLocalhost() {
						node.hostName = host.Name
					}
					node.referenceCount.IncrementAndGet()

					logger.Logger.Debug("Peer node `%s` matches existing node `%s` by host `%s`.", peer.nodeName, node.name, host)
					return true
				}
			}

		}
		peer.replaceNode = node
	}

	node = peers.nodeByName(peer.nodeName)
	if node != nil {
		node.referenceCount.IncrementAndGet()
		peer.replaceNode = nil

		return true
	}

	return false
}

// Tend the cluster until it has stabilized and return control.
// This helps avoid initial database request timeout issues when
// a large number of goroutines are initiated at client startup.
//
// If the cluster has not stabilized by the timeout, return
// control as well.  Do not return an error since future
// database requests may still succeed.
func (clstr *Cluster) waitTillStabilized() Error {
	count := -1

	doneCh := make(chan Error, 10)

	// will run until the cluster is stabilized
	go func() {
		var err Error
		for {
			if err = clstr.tend(); err != nil {
				if err.Matches(types.NOT_AUTHENTICATED, types.CLUSTER_NAME_MISMATCH_ERROR) {
					select {
					case doneCh <- err:
					default:
					}
				}
				logger.Logger.Warn("%s", err.Error())
			}

			// Check to see if cluster has changed since the last Tend().
			// If not, assume cluster has stabilized and return.
			if count == len(clstr.GetNodes()) {
				break
			}

			time.Sleep(time.Millisecond)

			count = len(clstr.GetNodes())
		}
		doneCh <- err
	}()

	clusterClientPolicy := clstr.clientPolicy.Load()
	select {
	case <-time.After(clusterClientPolicy.Timeout):
		if clusterClientPolicy.FailIfNotConnected {
			clstr.Close()
		}
		return ErrTimeout.err()
	case err := <-doneCh:
		if err != nil && clusterClientPolicy.FailIfNotConnected {
			clstr.Close()
		}
		return err
	}
}

// TODO: Not used anywhere. Consider removing
func (clstr *Cluster) findAlias(alias *Host) *Node {
	return clstr.aliases.Get(*alias)
}

func (clstr *Cluster) setPartitions(partMap partitionMap) {
	if err := partMap.validate(); err != nil {
		logger.Logger.Error("Partition map error: %s.", err.Error())
	}

	clstr.partitionWriteMap.Set(partMap)
}

func (clstr *Cluster) getPartitions() partitionMap {
	return clstr.partitionWriteMap.Get()
}

// discoverSeeds will lookup the seed hosts and convert seed hosts
// to IP addresses.
func discoverSeedIPs(seeds []*Host) (res []*Host) {
	for _, seed := range seeds {
		addresses, err := net.LookupHost(seed.Name)
		if err != nil {
			continue
		}

		for i := range addresses {
			h := *seed
			h.Name = addresses[i]
			res = append(res, &h)
		}
	}

	return res
}

// Adds seeds to the cluster
func (clstr *Cluster) seedNodes() (newSeedsFound bool, errChain Error) {
	// Must copy array reference for copy on write semantics to work.
	seedArrayCopy, _ := clstr.seeds.GetSyncedVia(func(seeds []*Host) ([]*Host, error) {
		seedsCopy := make([]*Host, len(seeds))
		copy(seedsCopy, seeds)

		return seedsCopy, nil
	})

	// discover seed IPs from DNS or Load Balancers
	seedArray := discoverSeedIPs(seedArrayCopy)

	successChan := make(chan struct{}, len(seedArray))
	errChan := make(chan Error, len(seedArray))

	logger.Logger.Info("Seeding the cluster. Seeds count: %d", len(seedArray))

	clusterClientPolicy := clstr.clientPolicy.Load()
	// Add all nodes at once to avoid copying entire array multiple times.
	for i, seed := range seedArray {
		go func(index int, seed *Host) {
			nodesToAdd := make(nodesToAddT, 128)
			nv := nodeValidator{seedOnlyCluster: clusterClientPolicy.SeedOnlyCluster}
			err := nv.seedNodes(clstr, seed, nodesToAdd)
			if err != nil {
				logger.Logger.Warn("Seed %s failed: %s", seed.String(), err.Error())
				errChan <- err
				return
			}
			clstr.addNodes(nodesToAdd)
			successChan <- struct{}{}
		}(i, seed)
	}

	seedCount := len(seedArray)
L:
	for {
		select {
		case err := <-errChan:
			errChain = chainErrors(err, errChain)
			seedCount--
			if seedCount <= 0 {
				break L
			}
		case <-successChan:
			seedCount--
			newSeedsFound = true
			if seedCount <= 0 {
				break L
			}
		case <-time.After(clusterClientPolicy.Timeout):
			// time is up, no seeds found
			break L
		}
	}

	if errChain != nil {
		errChain = chainErrors(newError(types.INVALID_NODE_ERROR, fmt.Sprintf("Failed to connect to hosts: %v", seedArray)), errChain)
	}

	return newSeedsFound, errChain
}

func (clstr *Cluster) createNode(nv *nodeValidator) *Node {
	return newNode(clstr, nv)
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

// TODO: Not used anywhere. Consider removing
func (clstr *Cluster) addAlias(host *Host, node *Node) {
	if host != nil && node != nil {
		clstr.aliases.Set(*host, node)
	}
}

func (clstr *Cluster) findNodesToRemove(peers *peers) {
	refreshCount := peers.refreshCount.Get()

	if clstr.clientPolicy.Load().SeedOnlyCluster {
		// Don't remove any node even if its bad or inactive.
		return
	}

	nodes := clstr.GetNodes()
	var numberOfOrphans int64
	for _, node := range nodes {
		if node.isOrphan.Get() {
			numberOfOrphans++
		}
		if !node.IsActive() {
			// Inactive nodes must be removed.
			if !peers.containsNodeToRemove(node) {
				peers.addNodesToRemove(node)
			}
			continue
		}

		// Single node clusters rely on whether it responded to info requests.
		if refreshCount == 0 && node.failures.Get() >= 5 {
			// All node info requests failed and this node had 5 consecutive failures.
			// Remove node.  If no nodes are left, seeds will be tried in next cluster
			// tend iteration.
			if !peers.containsNodeToRemove(node) {
				peers.addNodesToRemove(node)
			}
			continue
		}

		// Two node clusters require at least one successful refresh before removing.
		if len(nodes) > 1 && refreshCount >= 1 && node.referenceCount.Get() == 0 {
			// Node is not referenced by other nodes.
			// Check if node responded to info request.
			if node.failures.Get() == 0 {
				// Node is alive, but not referenced by other nodes.  Check if mapped.
				if !clstr.findNodeInPartitionMap(node) {
					// Node doesn't have any partitions mapped to it.
					// There is no point in keeping it in the cluster.
					if !peers.containsNodeToRemove(node) {
						peers.addNodesToRemove(node)
					}
				}
				// If node is orphan, it can be removed if it has no references and no peers.
				if node.referenceCount.Get() == 0 && node.peersCount.Get() == 0 && node.isOrphan.Get() {
					peers.addNodesToRemove(node)
				}
			} else {
				// Node not responding. Remove it.
				if !peers.containsNodeToRemove(node) {
					peers.addNodesToRemove(node)
				}
			}
		}
	}
}

func (clstr *Cluster) findNodeInPartitionMap(filter *Node) bool {
	partMap := clstr.getPartitions()

	for _, partitions := range partMap {
		for _, nodeArray := range partitions.Replicas {
			for _, node := range nodeArray {
				// Use reference equality for performance.
				if node == filter {
					return true
				}
			}
		}
	}
	return false
}

func (clstr *Cluster) updateClusterFeatures() {
	pQuery := true
	for _, n := range clstr.GetNodes() {
		if !n.SupportsPartitionQuery() {
			pQuery = false
			break
		}
	}
	clstr.supportsPartitionQuery.Set(pQuery)
}

func (clstr *Cluster) addNodes(nodesToAdd map[string]*Node) {
	if len(nodesToAdd) == 0 {
		return
	}

	// update features for all nodes
	defer clstr.updateClusterFeatures()

	clstr.nodes.Update(func(nodes []*Node) ([]*Node, error) {
		if clientPolicy := clstr.clientPolicy.Load(); clientPolicy != nil && clientPolicy.SeedOnlyCluster && clstr.GetSeedCount() == len(nodes) {
			// Don't add new nodes.
			return nodes, nil
		}

		for _, node := range nodesToAdd {
			if node != nil && !clstr.findNodeName(nodes, node.name) {
				logger.Logger.Debug("Adding node %s (%s) to the cluster.", node.name, node.host.String())
				nodes = append(nodes, node)
			}
		}

		nodesMap := make(map[string]*Node, len(nodes))
		nodesAliases := make(map[Host]*Node, len(nodes))
		for i := range nodes {
			nodesMap[nodes[i].name] = nodes[i]

			for _, alias := range nodes[i].GetAliases() {
				nodesAliases[*alias] = nodes[i]
			}
		}

		clstr.nodesMap.Replace(nodesMap)
		clstr.aliases.Replace(nodesAliases)

		return nodes, nil
	})
}

func (clstr *Cluster) removeNodes(nodesToRemove []*Node) {
	if len(nodesToRemove) == 0 {
		return
	}

	// update features for all nodes
	defer clstr.updateClusterFeatures()

	// There is no need to delete nodes from partitionWriteMap because the nodes
	// have already been set to inactive.

	// Cleanup node resources.
	for _, node := range nodesToRemove {
		// Remove node's aliases from cluster alias set.
		// Aliases are only used in tend goroutine, so synchronization is not necessary.
		clstr.aliases.DeleteAllDeref(node.GetAliases()...)
		clstr.nodesMap.Delete(node.name)
		node.Close()
	}

	// Remove all nodes at once to avoid copying entire array multiple times.
	clstr.nodes.Update(func(nodes []*Node) ([]*Node, error) {
		nlist := make([]*Node, 0, len(nodes))
		nlist = append(nlist, nodes...)
		for i, n := range nlist {
			for _, ntr := range nodesToRemove {
				if ntr.Equals(n) {
					nlist[i] = nil
				}
			}
		}

		newNodes := make([]*Node, 0, len(nlist))
		for i := range nlist {
			if nlist[i] != nil {
				newNodes = append(newNodes, nlist[i])
			}
		}

		return newNodes, nil
	})

}

// IsConnected returns true if cluster has nodes and is not already closed.
func (clstr *Cluster) IsConnected() bool {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	return (len(nodeArray) > 0) && !clstr.closed.Get()
}

// GetRandomNode returns a random node on the cluster
func (clstr *Cluster) GetRandomNode() (*Node, Error) {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	length := len(nodeArray)

	// prevent division by zero
	if length > 0 {
		for i := 0; i < length; i++ {
			// Must handle concurrency with other non-tending goroutines, so nodeIndex is consistent.
			index := clstr.nodeIndex.IncrementAndGet() % length
			node := nodeArray[index]

			if node != nil && node.IsActive() {
				//logger.Logger.Debug("Node `%s` is active. index=%d", node, index)
				return node, nil
			}
		}
	}

	return nil, ErrClusterIsEmpty.err()
}

// GetNodes returns a list of all nodes in the cluster
func (clstr *Cluster) GetNodes() []*Node {
	// Must copy array reference for copy on write semantics to work.
	return clstr.nodes.Get()
}

// GetSeedCount is the count of seed nodes
func (clstr *Cluster) GetSeedCount() int {
	res, _ := iatomic.MapSyncValue(&clstr.seeds, func(seeds []*Host) (int, error) {
		return len(seeds), nil
	})

	return res
}

// GetSeeds returns a list of all seed nodes in the cluster
func (clstr *Cluster) GetSeeds() []Host {
	res, _ := iatomic.MapSyncValue(&clstr.seeds, func(seeds []*Host) ([]Host, error) {
		res := make([]Host, 0, len(seeds))
		for _, seed := range seeds {
			res = append(res, *seed)
		}

		return res, nil
	})

	return res
}

// GetAliases returns a list of all node aliases in the cluster
func (clstr *Cluster) GetAliases() map[Host]*Node {
	return clstr.aliases.Clone()
}

// GetNodeByName finds a node by name and returns an
// error if the node is not found.
func (clstr *Cluster) GetNodeByName(nodeName string) (*Node, Error) {
	node := clstr.findNodeByName(nodeName)

	if node == nil {
		return nil, newError(types.INVALID_NODE_ERROR, "Invalid node name"+nodeName)
	}
	return node, nil
}

func (clstr *Cluster) findNodeByName(nodeName string) *Node {
	// Must copy array reference for copy on write semantics to work.
	for _, node := range clstr.GetNodes() {
		if node.GetName() == nodeName {
			return node
		}
	}
	return nil
}

// Close closes all cached connections to the cluster nodes
// and stops the tend goroutine.
func (clstr *Cluster) Close() {
	if clstr.closed.CompareAndToggle(false) {
		// send close signal to maintenance channel
		close(clstr.tendChannel)

		// wait until tend is over
		clstr.wgTend.Wait()

		// remove node references from the partition table
		// to allow GC to work its magic. Leaks otherwise.
		clstr.getPartitions().cleanup()
	}
}

// MigrationInProgress determines if any node in the cluster
// is participating in a data migration
func (clstr *Cluster) MigrationInProgress(timeout time.Duration) (res bool, err Error) {
	if timeout <= 0 {
		timeout = _DEFAULT_TIMEOUT
	}

	done := make(chan bool, 1)

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

	deadLine := time.After(timeout)
	for {
		select {
		case <-deadLine:
			return false, ErrTimeout.err()
		case <-done:
			return res, err
		}
	}
}

// WaitUntilMigrationIsFinished will block until all
// migration operations in the cluster all finished.
func (clstr *Cluster) WaitUntilMigrationIsFinished(timeout time.Duration) Error {
	if timeout <= 0 {
		timeout = _NO_TIMEOUT
	}
	done := make(chan Error, 1)

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

	deadLine := time.After(timeout)
	select {
	case <-deadLine:
		return ErrTimeout.err()
	case err := <-done:
		return err
	}
}

// Password returns the password that is currently used with the cluster.
func (clstr *Cluster) Password() (res []byte) {
	pass := clstr.password.Get()
	if pass != nil {
		return pass
	}
	return nil
}

func (clstr *Cluster) changePassword(user string, password string, hash []byte) {
	// change password ONLY if the user is the same
	if clstr.user == user {
		clientPolicy := *clstr.clientPolicy.Load()
		clientPolicy.Password = password
		clstr.clientPolicy.Store(&clientPolicy)
		clstr.password.Set(hash)
	}
}

// ClientPolicy returns the client policy that is currently used with the cluster.
func (clstr *Cluster) ClientPolicy() (res ClientPolicy) {
	returnPolicy := *clstr.clientPolicy.Load()
	return returnPolicy
}

// WarmUp fills the connection pool with connections for all nodes.
// This is necessary on startup for high traffic programs.
// If the count is <= 0, the connection queue will be filled.
// If the count is more than the size of the pool, the pool will be filled.
// Note: One connection per node is reserved for tend operations and is not used for transactions.
func (clstr *Cluster) WarmUp(count int) (int, Error) {
	var g errgroup.Group
	cnt := iatomic.NewInt(0)
	nodes := clstr.GetNodes()
	for i := range nodes {
		node := nodes[i]
		g.Go(func() error {
			n, err := node.WarmUp(count)
			cnt.AddAndGet(n)

			return err
		})
	}

	err := g.Wait()
	if err != nil {
		return cnt.Get(), err.(Error)
	}
	return cnt.Get(), nil
}

// MetricsEnabled returns true if metrics are enabled for the cluster.
func (clstr *Cluster) MetricsPolicy() *MetricsPolicy {
	return clstr.metricsPolicy.Get()
}

// MetricsEnabled returns true if metrics are enabled for the cluster.
func (clstr *Cluster) MetricsEnabled() bool {
	return clstr.metricsEnabled.Load()
}

// EnableMetrics enables the cluster command metrics gathering.
// If the parameters for the histogram in the policy are the different from the one already
// on the cluster, the metrics will be reset.
func (clstr *Cluster) EnableMetrics(policy *MetricsPolicy) {
	if policy == nil {
		policy = DefaultMetricsPolicy()
	}

	clstr.metricsPolicy.Set(policy)
	clstr.metricsEnabled.Store(true)

	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()

	// reshape the histogram in case it has changed
	for _, stat := range clstr.stats {
		stat.reshape(policy)
	}

	for _, node := range clstr.GetNodes() {
		node.stats.reshape(policy)
	}
}

func (clstr *Cluster) getNodeLabels() *Labels {
	var userLabels *Labels
	if clstr.metricsPolicy.Get() != nil && clstr.metricsPolicy.Get().Labels != nil {
		userLabels = clstr.metricsPolicy.Get().Labels
	} else {
		userLabels = NewLabels()
	}

	nodes := clstr.GetNodes()
	labels := make([]map[string]string, 0)
	clientPolicy := clstr.clientPolicy.Load()
	// Add node labels
	for node := range nodes {
		entries := make(map[string]string)
		var app_id string

		if clientPolicy.ApplicationId != "" {
			app_id = clientPolicy.ApplicationId
		} else {
			app_id = clstr.user
		}

		// Merging user labels with node labels
		for _, labelMap := range *userLabels {
			maps.Copy(entries, labelMap)
		}

		// Reserved label names for the client
		entries["node"] = nodes[node].GetName()
		entries["host"] = nodes[node].host.String()
		entries["cluster"] = clientPolicy.ClusterName

		// Users are allowed to override app-id if they want to. Default is the user name.
		// Users need to set the application id int the client policy.
		entries["app-id"] = app_id

		labels = append(labels, entries)
	}

	return NewLabels(labels...)
}

// DisableMetrics disables the cluster command metrics gathering.
func (clstr *Cluster) DisableMetrics() {
	clstr.metricsEnabled.Store(false)
}

//-------------------------------------------------------
// Utility Functions
//-------------------------------------------------------

// getLibraryVersion returns the version of the aerospike client library.
func getLibraryVersion(modulePath string) string {
	// golang idiomatic module version is "(devel)" if not set.
	defaultVersion := "(devel)"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return defaultVersion
	}

	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			return dep.Version
		}
	}

	return defaultVersion
}

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
	"errors"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	iatomic "github.com/aerospike/aerospike-client-go/v7/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/aerospike/aerospike-client-go/v7/types"
)

const (
	_PARTITIONS = 4096
)

const (
	_SUPPORTS_PARTITION_SCAN = 1 << iota
	_SUPPORTS_QUERY_SHOW
	_SUPPORTS_BATCH_ANY
	_SUPPORTS_PARTITION_QUERY
)

// Node represents an Aerospike Database Server Node
type Node struct {
	cluster     *Cluster
	name        string
	host        *Host
	aliases     iatomic.TypedVal[[]*Host]
	stats       nodeStats
	sessionInfo iatomic.TypedVal[*sessionInfo]

	racks iatomic.TypedVal[map[string]int]

	// tendConn reserves a connection for tend so that it won't have to
	// wait in queue for connections, since that will cause starvation
	// and the node being dropped under load.
	tendConn iatomic.Guard[Connection]

	peersGeneration iatomic.Int
	peersCount      iatomic.Int

	connections     connectionHeap
	connectionCount iatomic.Int

	partitionGeneration iatomic.Int
	referenceCount      iatomic.Int
	failures            iatomic.Int
	partitionChanged    iatomic.Bool
	errorCount          iatomic.Int
	rebalanceGeneration iatomic.Int

	features int

	active iatomic.Bool
}

// NewNode initializes a server node with connection parameters.
func newNode(cluster *Cluster, nv *nodeValidator) *Node {
	newNode := &Node{
		cluster: cluster,
		name:    nv.name,
		host:    nv.primaryHost,

		features: nv.features,

		stats: *newNodeStats(cluster.MetricsPolicy()),

		// Assign host to first IP alias because the server identifies nodes
		// by IP address (not hostname).
		connections:         *newConnectionHeap(cluster.clientPolicy.MinConnectionsPerNode, cluster.clientPolicy.ConnectionQueueSize),
		connectionCount:     *iatomic.NewInt(0),
		peersGeneration:     *iatomic.NewInt(-1),
		partitionGeneration: *iatomic.NewInt(-2),
		referenceCount:      *iatomic.NewInt(0),
		failures:            *iatomic.NewInt(0),
		active:              *iatomic.NewBool(true),
		partitionChanged:    *iatomic.NewBool(false),
		errorCount:          *iatomic.NewInt(0),
		rebalanceGeneration: *iatomic.NewInt(-1),
	}

	newNode.aliases.Set(nv.aliases)
	newNode.sessionInfo.Set(nv.sessionInfo)
	newNode.racks.Set(make(map[string]int))

	// this will reset to zero on first aggregation on the cluster,
	// therefore will only be counted once.
	newNode.stats.NodeAdded.IncrementAndGet()

	return newNode
}

// SupportsBatchAny returns true if the node supports the feature.
func (nd *Node) SupportsBatchAny() bool {
	return (nd.features & _SUPPORTS_BATCH_ANY) != 0
}

// SupportsQueryShow returns true if the node supports the feature.
func (nd *Node) SupportsQueryShow() bool {
	return (nd.features & _SUPPORTS_QUERY_SHOW) != 0
}

// SupportsPartitionQuery returns true if the node supports the feature.
func (nd *Node) SupportsPartitionQuery() bool {
	return (nd.features & _SUPPORTS_PARTITION_QUERY) != 0
}

// Refresh requests current status from server node, and updates node with the result.
func (nd *Node) Refresh(peers *peers) Error {
	if !nd.active.Get() {
		return nil
	}

	nd.stats.TendsTotal.IncrementAndGet()

	// Close idleConnections
	defer nd.dropIdleConnections()

	// Clear node reference counts.
	nd.referenceCount.Set(0)
	nd.partitionChanged.Set(false)

	var infoMap map[string]string
	commands := []string{"node", "peers-generation", "partition-generation"}
	if nd.cluster.clientPolicy.RackAware {
		commands = append(commands, "rack-ids")
	}

	infoMap, err := nd.RequestInfo(&nd.cluster.infoPolicy, commands...)
	if err != nil {
		nd.refreshFailed(err)
		return err
	}

	if err = nd.verifyNodeName(infoMap); err != nil {
		nd.refreshFailed(err)
		return err
	}

	if err = nd.verifyPeersGeneration(infoMap, peers); err != nil {
		nd.refreshFailed(err)
		return err
	}

	if err = nd.verifyPartitionGeneration(infoMap); err != nil {
		nd.refreshFailed(err)
		return err
	}

	if err = nd.updateRackInfo(infoMap); err != nil {
		// Update rack info should fail if the feature is not supported on the server
		if err.Matches(types.UNSUPPORTED_FEATURE) {
			nd.refreshFailed(err)
			return err
		}
		// Should not fail in other cases
		logger.Logger.Warn("Updating node rack info failed with error: %s (rack-ids: `%s`)", err, infoMap["rack-ids"])
	}

	nd.failures.Set(0)
	peers.refreshCount.IncrementAndGet()
	nd.referenceCount.IncrementAndGet()
	nd.stats.TendsSuccessful.IncrementAndGet()

	if err = nd.refreshSessionToken(); err != nil {
		logger.Logger.Error("Error refreshing session token: %s", err.Error())
	}

	if _, err = nd.fillMinConns(); err != nil {
		logger.Logger.Error("Error filling up the connection queue to the minimum required")
	}

	return nil
}

// refreshSessionToken refreshes the session token if it has been expired
func (nd *Node) refreshSessionToken() (err Error) {
	// no session token to refresh
	if !nd.cluster.clientPolicy.RequiresAuthentication() {
		return nil
	}

	st := nd.sessionInfo.Get()

	// Consider when the next tend will be in this calculation. If the next tend will be too late,
	// refresh the sessionInfo now.
	if st.expiration.IsZero() || time.Now().Before(st.expiration.Add(-nd.cluster.clientPolicy.TendInterval)) {
		return nil
	}

	nd.usingTendConn(nd.cluster.clientPolicy.LoginTimeout, func(conn *Connection) {
		command := newLoginCommand(conn.dataBuffer)
		if err = command.login(&nd.cluster.clientPolicy, conn, nd.cluster.Password()); err != nil {
			// force new connections to use default creds until a new valid session token is acquired
			nd.resetSessionInfo()
			// Socket not authenticated. Do not put back into pool.
			conn.Close()
		} else {
			nd.sessionInfo.Set(command.sessionInfo())
		}
	})

	return err
}

func (nd *Node) updateRackInfo(infoMap map[string]string) Error {
	if !nd.cluster.clientPolicy.RackAware {
		return nil
	}

	// Receive format: <ns1>:<rack1>;<ns2>:<rack2>...
	rackIds := infoMap["rack-ids"]

	// Do not panic if the server does not support rackaware
	if len(rackIds) == 0 || strings.HasPrefix(strings.ToUpper(rackIds), "ERROR") {
		return newError(types.UNSUPPORTED_FEATURE, "You have set the ClientPolicy.RackAware = true, but the server does not support this feature.")
	}

	rackPairs := strings.Split(rackIds, ";")
	racks := make(map[string]int)
	for i := range rackPairs {
		// split at the last occurrence of `:` to ensure rack will be an integer
		lastIdx := strings.LastIndex(rackPairs[i], ":")
		if lastIdx < 0 {
			return newError(types.PARSE_ERROR, "invalid rack value `%s`", rackPairs[i])
		}

		ns := rackPairs[i][:lastIdx]
		rack := rackPairs[i][lastIdx+1:]

		rackNo, err := strconv.Atoi(rack)
		if err != nil {
			return newErrorAndWrap(err, types.PARSE_ERROR)
		}
		racks[ns] = rackNo
	}

	nd.racks.Set(racks)

	return nil
}

func (nd *Node) verifyNodeName(infoMap map[string]string) Error {
	infoName, exists := infoMap["node"]

	if !exists || len(infoName) == 0 {
		return newError(types.INVALID_NODE_ERROR, "Node name is empty")
	}

	if !(nd.name == infoName) {
		// Set node to inactive immediately.
		nd.active.Set(false)
		return newError(types.INVALID_NODE_ERROR, "Node name has changed. Old="+nd.name+" New="+infoName)
	}
	return nil
}

func (nd *Node) verifyPeersGeneration(infoMap map[string]string, peers *peers) Error {
	genString := infoMap["peers-generation"]
	if len(genString) == 0 {
		return newError(types.PARSE_ERROR, "peers-generation is empty")
	}

	gen, err := strconv.Atoi(genString)
	if err != nil {
		return newError(types.PARSE_ERROR, "peers-generation is not a number: "+genString)
	}

	peers.genChanged.Or(nd.peersGeneration.Get() != gen)
	return nil
}

func (nd *Node) verifyPartitionGeneration(infoMap map[string]string) Error {
	genString := infoMap["partition-generation"]

	if len(genString) == 0 {
		return newError(types.PARSE_ERROR, "partition-generation is empty")
	}

	gen, err := strconv.Atoi(genString)
	if err != nil {
		return newError(types.PARSE_ERROR, "partition-generation is not a number:"+genString)
	}

	if nd.partitionGeneration.Get() != gen {
		nd.partitionChanged.Set(true)
	}
	return nil
}

func (nd *Node) refreshPeers(peers *peers) {
	// Do not refresh peers when node connection has already failed during this cluster tend iteration.
	if nd.failures.Get() > 0 || !nd.active.Get() {
		return
	}

	peerParser, err := parsePeers(nd.cluster, nd)
	if err != nil {
		logger.Logger.Debug("Parsing peers failed: %s", err)
		nd.refreshFailed(err)
		return
	}

	peers.appendPeers(peerParser.peers)
	nd.peersGeneration.Set(int(peerParser.generation()))
	nd.peersCount.Set(len(peers.peers()))
	peers.refreshCount.IncrementAndGet()
}

func (nd *Node) refreshPartitions(peers *peers, partitions partitionMap, freshlyAdded bool) {
	// Do not refresh peers when node connection has already failed during this cluster tend iteration.
	// Also, avoid "split cluster" case where this node thinks it's a 1-node cluster.
	// Unchecked, such a node can dominate the partition map and cause all other
	// nodes to be dropped.
	if !freshlyAdded {
		if nd.failures.Get() > 0 || !nd.active.Get() || (nd.peersCount.Get() == 0 && peers.refreshCount.Get() > 1) {
			return
		}
	}

	parser, err := newPartitionParser(nd, partitions, _PARTITIONS)
	if err != nil {
		nd.refreshFailed(err)
		return
	}

	if parser.generation != nd.partitionGeneration.Get() {
		logger.Logger.Info("Node %s partition generation changed from %d to %d", nd.host.String(), nd.partitionGeneration.Get(), parser.getGeneration())
		nd.partitionChanged.Set(true)
		nd.partitionGeneration.Set(parser.getGeneration())
		nd.stats.PartitionMapUpdates.IncrementAndGet()
	}
}

func (nd *Node) refreshFailed(e Error) {
	nd.peersGeneration.Set(-1)
	nd.partitionGeneration.Set(-1)

	if nd.cluster.clientPolicy.RackAware {
		nd.rebalanceGeneration.Set(-1)
	}

	nd.failures.IncrementAndGet()
	nd.stats.TendsFailed.IncrementAndGet()

	// Only log message if cluster is still active.
	if nd.cluster.IsConnected() {
		logger.Logger.Warn("Node `%s` refresh failed: `%s`", nd, e)
	}
}

// dropIdleConnections picks a connection from the head of the connection pool queue
// if that connection is idle, it drops it and takes the next one until it picks
// a fresh connection or exhaust the queue.
func (nd *Node) dropIdleConnections() {
	nd.connections.DropIdle()
}

// GetConnection gets a connection to the node.
// If no pooled connection is available, a new connection will be created, unless
// ClientPolicy.MaxQueueSize number of connections are already created.
// This method will retry to retrieve a connection in case the connection pool
// is empty, until timeout is reached.
func (nd *Node) GetConnection(timeout time.Duration) (conn *Connection, err Error) {
	if timeout <= 0 {
		timeout = _DEFAULT_TIMEOUT
	}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		conn, err = nd.getConnection(timeout, timeout)
		if err == nil && conn != nil {
			return conn, nil
		}

		if errors.Is(err, ErrServerNotAvailable) {
			return nil, err
		}

		time.Sleep(5 * time.Millisecond)
	}

	// in case the block didn't run at all
	if err == nil {
		err = ErrConnectionPoolEmpty.err()
	}

	return nil, err
}

// getConnection gets a connection to the node.
// If no pooled connection is available, a new connection will be created.
func (nd *Node) getConnection(deadline, timeout time.Duration) (conn *Connection, err Error) {
	return nd.getConnectionWithHint(deadline, timeout, 0)
}

// newConnectionAllowed will tentatively check if the client is allowed to make a new connection
// based on the ClientPolicy passed to it.
// This is more or less a copy of the logic in the beginning of newConnection function.
func (nd *Node) newConnectionAllowed() Error {
	if !nd.active.Get() {
		return ErrServerNotAvailable.err()
	}

	// if connection count is limited and enough connections are already created, don't create a new one
	cc := nd.connectionCount.IncrementAndGet()
	defer nd.connectionCount.DecrementAndGet()
	if nd.cluster.clientPolicy.LimitConnectionsToQueueSize && cc > nd.cluster.clientPolicy.ConnectionQueueSize {
		return ErrTooManyConnectionsForNode.err()
	}

	// Check for opening connection threshold
	if nd.cluster.clientPolicy.OpeningConnectionThreshold > 0 {
		ct := nd.cluster.connectionThreshold.IncrementAndGet()
		defer nd.cluster.connectionThreshold.DecrementAndGet()
		if ct > nd.cluster.clientPolicy.OpeningConnectionThreshold {
			return ErrTooManyOpeningConnections.err()
		}
	}

	return nil
}

// newConnection will make a new connection for the node.
func (nd *Node) newConnection(overrideThreshold bool) (*Connection, Error) {
	if !nd.active.Get() {
		return nil, ErrServerNotAvailable.err()
	}

	// if connection count is limited and enough connections are already created, don't create a new one
	cc := nd.connectionCount.IncrementAndGet()
	if nd.cluster.clientPolicy.LimitConnectionsToQueueSize && cc > nd.cluster.clientPolicy.ConnectionQueueSize {
		nd.connectionCount.DecrementAndGet()
		nd.stats.ConnectionsPoolEmpty.IncrementAndGet()

		return nil, ErrTooManyConnectionsForNode.err()
	}

	// Check for opening connection threshold
	if !overrideThreshold && nd.cluster.clientPolicy.OpeningConnectionThreshold > 0 {
		ct := nd.cluster.connectionThreshold.IncrementAndGet()
		if ct > nd.cluster.clientPolicy.OpeningConnectionThreshold {
			nd.cluster.connectionThreshold.DecrementAndGet()
			nd.connectionCount.DecrementAndGet()

			return nil, ErrTooManyOpeningConnections.err()
		}

		defer nd.cluster.connectionThreshold.DecrementAndGet()
	}

	nd.stats.ConnectionsAttempts.IncrementAndGet()
	conn, err := NewConnection(&nd.cluster.clientPolicy, nd.host)
	if err != nil {
		nd.incrErrorCount()
		nd.connectionCount.DecrementAndGet()
		nd.stats.ConnectionsFailed.IncrementAndGet()
		return nil, err
	}
	conn.node = nd

	sessionInfo := nd.sessionInfo.Get()
	// need to authenticate
	if err = conn.login(&nd.cluster.clientPolicy, nd.cluster.Password(), sessionInfo); err != nil {
		// increment node errors if authentication hit a network error
		if networkError(err) {
			nd.incrErrorCount()
		}
		nd.stats.ConnectionsFailed.IncrementAndGet()

		// Socket not authenticated. Do not put back into pool.
		conn.Close()
		return nil, err
	}

	nd.stats.ConnectionsSuccessful.IncrementAndGet()
	conn.setIdleTimeout(nd.cluster.clientPolicy.IdleTimeout)

	return conn, nil
}

// makeConnectionForPool will try to open a connection until deadline.
// if no deadline is defined, it will only try for _DEFAULT_TIMEOUT.
func (nd *Node) makeConnectionForPool(hint byte) {
	conn, err := nd.newConnection(false)
	if err != nil {
		logger.Logger.Debug("Error trying to make a connection to the node %s: %s", nd.String(), err.Error())
		return
	}

	nd.putConnectionWithHint(conn, hint)
}

// getConnectionWithHint gets a connection to the node.
// If no pooled connection is available, a new connection will be created.
func (nd *Node) getConnectionWithHint(totalTimeout, socketTimeout time.Duration, hint byte) (conn *Connection, err Error) {
	if !nd.active.Get() {
		return nil, ErrServerNotAvailable.err()
	}

	// try to get a valid connection from the connection pool
	for conn = nd.connections.Poll(hint); conn != nil; conn = nd.connections.Poll(hint) {
		if conn.IsConnected() {
			break
		}
		conn.Close()
		conn = nil
	}

	if conn == nil {
		// tentatively check if a connection is allowed to avoid launching too many goroutines.
		err = nd.newConnectionAllowed()
		if err == nil {
			go nd.makeConnectionForPool(hint)
		} else if errors.Is(err, ErrTooManyConnectionsForNode) {
			return nil, ErrConnectionPoolExhausted.err()
		}
		return nil, ErrConnectionPoolEmpty.err()
	}

	if err = conn.setTimeout(totalTimeout, socketTimeout); err != nil {
		nd.stats.ConnectionsFailed.IncrementAndGet()

		// Do not put back into pool.
		conn.Close()
		return nil, err
	}

	conn.refresh()

	return conn, nil
}

// PutConnection puts back a connection to the pool.
// If connection pool is full, the connection will be
// closed and discarded.
func (nd *Node) putConnectionWithHint(conn *Connection, hint byte) bool {
	conn.refresh()
	if !nd.active.Get() || !nd.connections.Offer(conn, hint) {
		nd.stats.ConnectionsPoolOverflow.IncrementAndGet()
		conn.Close()
		return false
	}
	return true
}

// PutConnection puts back a connection to the pool.
// If connection pool is full, the connection will be
// closed and discarded.
func (nd *Node) PutConnection(conn *Connection) {
	nd.putConnectionWithHint(conn, 0)
}

// InvalidateConnection closes and discards a connection from the pool.
func (nd *Node) InvalidateConnection(conn *Connection) {
	conn.Close()
}

// GetHost retrieves host for the node.
func (nd *Node) GetHost() *Host {
	return nd.host
}

// IsActive Checks if the node is active.
func (nd *Node) IsActive() bool {
	return nd != nil && nd.active.Get() && nd.partitionGeneration.Get() >= -1
}

// GetName returns node name.
func (nd *Node) GetName() string {
	return nd.name
}

// GetAliases returns node aliases.
func (nd *Node) GetAliases() []*Host {
	return nd.aliases.Get()
}

// Sets node aliases
func (nd *Node) setAliases(aliases []*Host) {
	nd.aliases.Set(aliases)
}

// AddAlias adds an alias for the node
func (nd *Node) addAlias(aliasToAdd *Host) {
	// Aliases are only referenced in the cluster tend goroutine,
	// so synchronization is not necessary.
	aliases := nd.GetAliases()
	if aliases == nil {
		aliases = []*Host{}
	}

	aliases = append(aliases, aliasToAdd)
	nd.setAliases(aliases)
}

// Close marks node as inactive and closes all of its pooled connections.
func (nd *Node) Close() {
	if nd.active.Get() {
		nd.active.Set(false)
		nd.stats.NodeRemoved.IncrementAndGet()
	}
	nd.closeConnections()
	nd.connections.cleanup()
}

// String implements stringer interface
func (nd *Node) String() string {
	if nd != nil {
		return nd.name + " " + nd.host.String()
	}
	return "<nil>"
}

func (nd *Node) closeConnections() {
	for conn := nd.connections.Poll(0); conn != nil; conn = nd.connections.Poll(0) {
		conn.Close()
	}

	// close the tend connection
	nd.tendConn.Do(func(conn *Connection) {
		if conn != nil {
			conn.Close()
		}
	})
}

// Equals compares equality of two nodes based on their names.
func (nd *Node) Equals(other *Node) bool {
	return nd != nil && other != nil && (nd == other || nd.name == other.name)
}

// MigrationInProgress determines if the node is participating in a data migration
func (nd *Node) MigrationInProgress() (bool, Error) {
	values, err := nd.RequestStats(&nd.cluster.infoPolicy)
	if err != nil {
		return false, err
	}

	// if the migrate_partitions_remaining exists and is not `0`, then migration is in progress
	if migration, exists := values["migrate_partitions_remaining"]; exists && migration != "0" {
		return true, nil
	}

	// migration not in progress
	return false, nil
}

// WaitUntillMigrationIsFinished will block until migration operations are finished.
func (nd *Node) WaitUntillMigrationIsFinished(timeout time.Duration) Error {
	if timeout <= 0 {
		timeout = _NO_TIMEOUT
	}
	done := make(chan Error)

	go func() {
		// this function is guaranteed to return after timeout
		// no go routines will be leaked
		for {
			if res, err := nd.MigrationInProgress(); err != nil || !res {
				done <- err
				return
			}
		}
	}()

	dealine := time.After(timeout)
	select {
	case <-dealine:
		return newError(types.TIMEOUT)
	case err := <-done:
		return err
	}
}

// usingTendConn allows the tend connection to be used in a monitor without race conditions.
// If the connection is not valid, it establishes a valid connection first.
func (nd *Node) usingTendConn(timeout time.Duration, f func(conn *Connection)) (err Error) {
	nd.tendConn.Update(func(conn **Connection) {
		if timeout <= 0 {
			timeout = _DEFAULT_TIMEOUT
		}

		// if the tend connection is invalid, establish a new connection first
		if *conn == nil || !(*conn).IsConnected() {
			if nd.connectionCount.Get() == 0 {
				// if there are no connections in the pool, create a new connection synchronously.
				// this will make sure the initial tend will get a connection without multiple retries.
				*conn, err = nd.newConnection(true)
			} else {
				*conn, err = nd.GetConnection(timeout)
			}

			// if no connection could be established, exit fast
			if err != nil {
				return
			}
		}

		// Set timeout for tend conn
		if err = (*conn).setTimeout(timeout, timeout); err != nil {
			return
		}

		// if all went well, call the closure
		f(*conn)
	})
	return err
}

// requestInfoWithRetry gets info values by name from the specified database server node.
// It will try at least N times before returning an error.
func (nd *Node) requestInfoWithRetry(policy *InfoPolicy, n int, name ...string) (res map[string]string, err Error) {
	for i := 0; i < n; i++ {
		if res, err = nd.requestInfo(policy.Timeout, name...); err == nil {
			return res, nil
		}

		logger.Logger.Error("Error occurred while fetching info from the server node %s: %s", nd.host.String(), err.Error())
		time.Sleep(100 * time.Millisecond)
	}

	// return the last error
	return nil, err
}

// RequestInfo gets info values by name from the specified database server node.
func (nd *Node) RequestInfo(policy *InfoPolicy, name ...string) (map[string]string, Error) {
	return nd.requestInfo(policy.Timeout, name...)
}

// RequestInfo gets info values by name from the specified database server node.
func (nd *Node) requestInfo(timeout time.Duration, name ...string) (response map[string]string, err Error) {
	nd.usingTendConn(timeout, func(conn *Connection) {
		response, err = conn.RequestInfo(name...)
		if err != nil {
			conn.Close()
		}
	})

	return response, err
}

// requestRawInfo gets info values by name from the specified database server node.
// It won't parse the results.
func (nd *Node) requestRawInfo(policy *InfoPolicy, name ...string) (response *info, err Error) {
	nd.usingTendConn(policy.Timeout, func(conn *Connection) {
		response, err = newInfo(conn, name...)
		if err != nil {
			conn.Close()
		}
	})
	return response, err
}

// RequestStats returns statistics for the specified node as a map
func (nd *Node) RequestStats(policy *InfoPolicy) (map[string]string, Error) {
	infoMap, err := nd.RequestInfo(policy, "statistics")
	if err != nil {
		return nil, err
	}

	res := map[string]string{}

	v, exists := infoMap["statistics"]
	if !exists {
		return res, nil
	}

	values := strings.Split(v, ";")
	for i := range values {
		kv := strings.Split(values[i], "=")
		if len(kv) > 1 {
			res[kv[0]] = kv[1]
		}
	}

	return res, nil
}

// resetSessionInfo resets the sessionInfo after an
// unsuccessful authentication with token
func (nd *Node) resetSessionInfo() {
	si := &sessionInfo{}
	nd.sessionInfo.Set(si)
}

// sessionToken returns the session token for the node.
// It will return nil if the session has expired.
func (nd *Node) sessionToken() []byte {
	si := nd.sessionInfo.Get()
	if !si.isValid() {
		return nil
	}

	return si.token
}

// Rack returns the rack number for the namespace.
func (nd *Node) Rack(namespace string) (int, Error) {
	racks := nd.racks.Get()
	v, exists := racks[namespace]

	if exists {
		return v, nil
	}

	return -1, newCustomNodeError(nd, types.RACK_NOT_DEFINED)
}

// Rack returns the rack number for the namespace.
func (nd *Node) hasRack(namespace string, rack int) bool {
	racks := nd.racks.Get()
	v, exists := racks[namespace]

	if !exists {
		return false
	}

	return v == rack
}

// WarmUp fills the node's connection pool with connections.
// This is necessary on startup for high traffic programs.
// If the count is <= 0, the connection queue will be filled.
// If the count is more than the size of the pool, the pool will be filled.
// Note: One connection per node is reserved for tend operations and is not used for transactions.
func (nd *Node) WarmUp(count int) (int, Error) {
	var g errgroup.Group
	cnt := iatomic.NewInt(0)

	toAlloc := nd.connections.Cap() - nd.connectionCount.Get()
	if count < toAlloc && count > 0 {
		toAlloc = count
	}

	for i := 0; i < toAlloc; i++ {
		g.Go(func() error {
			conn, err := nd.newConnection(true)
			if err != nil {
				if errors.Is(err, ErrTooManyConnectionsForNode) {
					return nil
				}
				return err
			}

			if nd.putConnectionWithHint(conn, 0) {
				cnt.IncrementAndGet()
			} else {
				conn.Close()
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return cnt.Get(), err.(Error)
	}
	return cnt.Get(), nil
}

// fillMinCounts will fill the connection pool to the minimum required
// by the ClientPolicy.MinConnectionsPerNode
func (nd *Node) fillMinConns() (int, Error) {
	if nd.cluster.clientPolicy.MinConnectionsPerNode > 0 {
		toFill := nd.cluster.clientPolicy.MinConnectionsPerNode - nd.connectionCount.Get()
		if toFill > 0 {
			return nd.WarmUp(toFill)
		}
	}
	return 0, nil
}

// Increments error count for the node. If errorCount goes above the threshold,
// the node will not accept any more requests until the next window.
func (nd *Node) incrErrorCount() {
	if nd.cluster.clientPolicy.MaxErrorRate > 0 {
		nd.errorCount.GetAndIncrement()
	}
}

// Resets the error count
func (nd *Node) resetErrorCount() {
	nd.errorCount.Set(0)
}

// checks if the errorCount is within set limits
func (nd *Node) errorCountWithinLimit() bool {
	return nd.cluster.clientPolicy.MaxErrorRate <= 0 || nd.errorCount.Get() <= nd.cluster.clientPolicy.MaxErrorRate
}

// returns error if errorCount has gone above the threshold set in the policy
func (nd *Node) validateErrorCount() Error {
	if !nd.errorCountWithinLimit() {
		nd.stats.CircuitBreakerHits.IncrementAndGet()
		return newError(types.MAX_ERROR_RATE)
	}
	return nil
}

// PeersGeneration returns node's Peers Generation
func (nd *Node) PeersGeneration() int {
	return nd.peersGeneration.Get()
}

// PartitionGeneration returns node's Partition Generation
func (nd *Node) PartitionGeneration() int {
	return nd.partitionGeneration.Get()
}

// RebalanceGeneration returns node's Rebalance Generation
func (nd *Node) RebalanceGeneration() int {
	return nd.rebalanceGeneration.Get()
}

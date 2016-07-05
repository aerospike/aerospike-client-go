// Copyright 2013-2016 Aerospike, Inc.
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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

const (
	_PARTITIONS = 4096
)

// Node represents an Aerospike Database Server Node
type Node struct {
	cluster *Cluster
	name    string
	host    *Host
	aliases atomic.Value //[]*Host
	address string

	// tendConn reserves a connection for tend so that it won't have to
	// wait in queue for connections, since that will cause starvation
	// and the node being dropped under load.
	tendConn *Connection

	connections     *AtomicQueue //ArrayBlockingQueue<*Connection>
	connectionCount *AtomicInt
	health          *AtomicInt //AtomicInteger

	partitionGeneration *AtomicInt
	referenceCount      *AtomicInt
	failures            *AtomicInt

	useNewInfo bool
	active     *AtomicBool

	supportsFloat, supportsBatchIndex, supportsReplicasAll, supportsGeo *AtomicBool
}

// NewNode initializes a server node with connection parameters.
func newNode(cluster *Cluster, nv *nodeValidator) *Node {
	newNode := &Node{
		cluster: cluster,
		name:    nv.name,
		// aliases:    nv.aliases,
		address:    nv.address,
		useNewInfo: nv.useNewInfo,

		// Assign host to first IP alias because the server identifies nodes
		// by IP address (not hostname).
		host:                nv.aliases[0],
		connections:         NewAtomicQueue(cluster.clientPolicy.ConnectionQueueSize),
		connectionCount:     NewAtomicInt(0),
		partitionGeneration: NewAtomicInt(-1),
		referenceCount:      NewAtomicInt(0),
		failures:            NewAtomicInt(0),
		active:              NewAtomicBool(true),

		supportsFloat:       NewAtomicBool(nv.supportsFloat),
		supportsBatchIndex:  NewAtomicBool(nv.supportsBatchIndex),
		supportsReplicasAll: NewAtomicBool(nv.supportsReplicasAll),
		supportsGeo:         NewAtomicBool(nv.supportsGeo),
	}

	newNode.aliases.Store(nv.aliases)

	return newNode
}

// Refresh requests current status from server node, and updates node with the result.
func (nd *Node) Refresh(friends []*Host) ([]*Host, error) {
	// Close idleConnections
	defer nd.dropIdleConnections()

	nd.referenceCount.Set(0)

	if nd.tendConn == nil {
		// Tend connection required a long timeout
		tendConn, err := nd.GetConnection(15 * time.Second)
		if err != nil {
			return nil, err
		}

		nd.tendConn = tendConn
	}

	// Set timeout for tend conn
	nd.tendConn.SetTimeout(15 * time.Second)

	commands := []string{"node", "partition-generation", nd.cluster.clientPolicy.serviceString()}

	infoMap, err := RequestInfo(nd.tendConn, commands...)
	if err != nil {
		nd.InvalidateConnection(nd.tendConn)
		nd.tendConn = nil
		return nil, err
	}

	if err := nd.verifyNodeName(infoMap); err != nil {
		return nil, err
	}

	if friends, err = nd.addFriends(infoMap, friends); err != nil {
		return nil, err
	}

	if err := nd.updatePartitions(nd.tendConn, infoMap); err != nil {
		nd.InvalidateConnection(nd.tendConn)
		nd.tendConn = nil
		return nil, err
	}

	return friends, nil
}

func (nd *Node) verifyNodeName(infoMap map[string]string) error {
	infoName, exists := infoMap["node"]

	if !exists || len(infoName) == 0 {
		return NewAerospikeError(INVALID_NODE_ERROR, "Node name is empty")
	}

	if !(nd.name == infoName) {
		// Set node to inactive immediately.
		nd.active.Set(false)
		return NewAerospikeError(INVALID_NODE_ERROR, "Node name has changed. Old="+nd.name+" New="+infoName)
	}
	return nil
}

func (nd *Node) addFriends(infoMap map[string]string, friends []*Host) ([]*Host, error) {
	friendString, exists := infoMap[nd.cluster.clientPolicy.serviceString()]

	if !exists || len(friendString) == 0 {
		return friends, nil
	}

	friendNames := strings.Split(friendString, ";")

	for _, friend := range friendNames {
		friendInfo := strings.Split(friend, ":")

		if len(friendInfo) != 2 {
			Logger.Error("Node info from asinfo:services is malformed. Expected HOST:PORT, but got `%s`", friend)
			continue
		}

		host := friendInfo[0]
		port, _ := strconv.Atoi(friendInfo[1])
		alias := NewHost(host, port)

		if nd.cluster.clientPolicy.IpMap != nil {
			if alternativeHost, ok := nd.cluster.clientPolicy.IpMap[host]; ok {
				alias = NewHost(alternativeHost, port)
			}
		}

		node := nd.cluster.findAlias(alias)

		if node != nil {
			node.referenceCount.IncrementAndGet()
		} else {
			if !nd.findAlias(friends, alias) {
				if friends == nil {
					friends = make([]*Host, 0, 16)
				}

				friends = append(friends, alias)
			}
		}
	}

	return friends, nil
}

func (nd *Node) findAlias(friends []*Host, alias *Host) bool {
	for _, host := range friends {
		if *host == *alias {
			return true
		}
	}
	return false
}

func (nd *Node) updatePartitions(conn *Connection, infoMap map[string]string) error {
	genString, exists := infoMap["partition-generation"]

	if !exists || len(genString) == 0 {
		return NewAerospikeError(PARSE_ERROR, "partition-generation is empty")
	}

	generation, _ := strconv.Atoi(genString)

	if nd.partitionGeneration.Get() != generation {
		Logger.Info("Node %s partition generation %d changed", nd.GetName(), generation)
		if err := nd.cluster.updatePartitions(conn, nd); err != nil {
			return err
		}
		nd.partitionGeneration.Set(generation)
	}

	return nil
}

// dropIdleConnections picks a connection from the head of the connection pool queue
// if that connection is idle, it drops it and takes the next one until it picks
// a fresh connection or exhaust the queue.
func (nd *Node) dropIdleConnections() {
	for {
		if t := nd.connections.Poll(); t != nil {
			conn := t.(*Connection)
			if conn.IsConnected() && !conn.isIdle() {
				// put it back: this connection is the oldest, and is still fresh
				// so the ones after it are likely also fresh
				if !nd.connections.Offer(conn) {
					nd.InvalidateConnection(conn)
				}
				return
			}
			nd.InvalidateConnection(conn)
		} else {
			// the queue is exhaused
			break
		}
	}
}

// GetConnection gets a connection to the node.
// If no pooled connection is available, a new connection will be created, unless
// ClientPolicy.MaxQueueSize number of connections are already created.
// This method will retry to retrieve a connection in case the connection pool
// is empty, until timeout is reached.
func (nd *Node) GetConnection(timeout time.Duration) (conn *Connection, err error) {
	deadline := time.Now().Add(timeout)
	if timeout == 0 {
		deadline = time.Now().Add(time.Second)
	}

CL:
	// try to acquire a connection; if the connection pool is empty, retry until
	// timeout occures. If no timeout is set, will retry indefinitely.
	conn, err = nd.getConnection(timeout)
	if err != nil {
		if err == ErrConnectionPoolEmpty && nd.IsActive() && time.Now().Before(deadline) {
			// give the scheduler time to breath; affects latency minimally, but throughput drastically
			time.Sleep(time.Microsecond)
			goto CL
		}

		return nil, err
	}

	return conn, nil
}

// getConnection gets a connection to the node.
// If no pooled connection is available, a new connection will be created.
// This method does not include logic to retry in case the connection pool is empty
func (nd *Node) getConnection(timeout time.Duration) (conn *Connection, err error) {
	// try to get a valid connection from the connection pool
	for t := nd.connections.Poll(); t != nil; t = nd.connections.Poll() {
		conn = t.(*Connection)
		if conn.IsConnected() {
			break
		}
		nd.InvalidateConnection(conn)
		conn = nil
	}

	if conn == nil {
		// if connection count is limited and enough connections are already created, don't create a new one
		if nd.cluster.clientPolicy.LimitConnectionsToQueueSize && nd.connectionCount.IncrementAndGet() > nd.cluster.clientPolicy.ConnectionQueueSize {
			nd.connectionCount.DecrementAndGet()
			return nil, ErrConnectionPoolEmpty
		}

		if conn, err = NewConnection(nd.address, nd.cluster.clientPolicy.Timeout); err != nil {
			nd.connectionCount.DecrementAndGet()
			return nil, err
		}

		// need to authenticate
		if err = conn.Authenticate(nd.cluster.user, nd.cluster.Password()); err != nil {
			// Socket not authenticated. Do not put back into pool.
			nd.InvalidateConnection(conn)
			return nil, err
		}
	}

	if err = conn.SetTimeout(timeout); err != nil {
		// Do not put back into pool.
		nd.InvalidateConnection(conn)
		return nil, err
	}

	conn.setIdleTimeout(nd.cluster.clientPolicy.IdleTimeout)
	conn.refresh()

	return conn, nil
}

// PutConnection puts back a connection to the pool.
// If connection pool is full, the connection will be
// closed and discarded.
func (nd *Node) PutConnection(conn *Connection) {
	conn.refresh()
	if !nd.active.Get() || !nd.connections.Offer(conn) {
		nd.InvalidateConnection(conn)
	}
}

// InvalidateConnection closes and discards a connection from the pool.
func (nd *Node) InvalidateConnection(conn *Connection) {
	nd.connectionCount.DecrementAndGet()
	conn.Close()
}

// GetHost retrieves host for the node.
func (nd *Node) GetHost() *Host {
	return nd.host
}

// IsActive Checks if the node is active.
func (nd *Node) IsActive() bool {
	return nd.active.Get()
}

// GetName returns node name.
func (nd *Node) GetName() string {
	return nd.name
}

// GetAliases returns node aliases.
func (nd *Node) GetAliases() []*Host {
	return nd.aliases.Load().([]*Host)
}

// Sets node aliases
func (nd *Node) setAliases(aliases []*Host) {
	nd.aliases.Store(aliases)
}

// AddAlias adds an alias for the node
func (nd *Node) AddAlias(aliasToAdd *Host) {
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
	nd.active.Set(false)
	nd.closeConnections()
}

// String implements stringer interface
func (nd *Node) String() string {
	return nd.name + " " + nd.host.String()
}

func (nd *Node) closeConnections() {
	for conn := nd.connections.Poll(); conn != nil; conn = nd.connections.Poll() {
		conn.(*Connection).Close()
	}
}

// Equals compares equality of two nodes based on their names.
func (nd *Node) Equals(other *Node) bool {
	return nd.name == other.name
}

// MigrationInProgress determines if the node is participating in a data migration
func (nd *Node) MigrationInProgress() (bool, error) {
	values, err := RequestNodeStats(nd)
	if err != nil {
		return false, err
	}

	// if the migration_progress_send exists and is not `0`, then migration is in progress
	if migration, exists := values["migrate_progress_send"]; exists && migration != "0" {
		return true, nil
	}

	// migration not in progress
	return false, nil
}

// WaitUntillMigrationIsFinished will block until migration operations are finished.
func (nd *Node) WaitUntillMigrationIsFinished(timeout time.Duration) (err error) {
	if timeout <= 0 {
		timeout = _NO_TIMEOUT
	}
	done := make(chan error)

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
		return NewAerospikeError(TIMEOUT)
	case err = <-done:
		return err
	}
}

// RequestInfo gets info values by name from the specified database server node.
func (nd *Node) RequestInfo(name ...string) (map[string]string, error) {
	conn, err := nd.GetConnection(_DEFAULT_TIMEOUT)
	if err != nil {
		return nil, err
	}

	response, err := RequestInfo(conn, name...)
	if err != nil {
		nd.InvalidateConnection(conn)
		return nil, err
	}
	nd.PutConnection(conn)
	return response, nil
}

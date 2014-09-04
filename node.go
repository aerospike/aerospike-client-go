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

	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

const (
	_PARTITIONS  = 4096
	_FULL_HEALTH = 100
)

// Node represents an Aerospike Database Server Node
type Node struct {
	cluster *Cluster
	name    string
	host    *Host
	aliases []*Host
	address string // socket?

	connections *AtomicQueue //ArrayBlockingQueue<*Connection>
	health      *AtomicInt   //AtomicInteger

	partitionGeneration *AtomicInt
	referenceCount      *AtomicInt
	responded           *AtomicBool
	useNewInfo          *AtomicBool
	active              *AtomicBool
	mutex               sync.RWMutex
}

// Initialize server node with connection parameters.
func newNode(cluster *Cluster, nv *nodeValidator) *Node {
	return &Node{
		cluster:    cluster,
		name:       nv.name,
		aliases:    nv.aliases,
		address:    nv.address,
		useNewInfo: NewAtomicBool(nv.useNewInfo),

		// Assign host to first IP alias because the server identifies nodes
		// by IP address (not hostname).
		host:                nv.aliases[0],
		connections:         NewAtomicQueue(cluster.connectionQueueSize),
		health:              NewAtomicInt(_FULL_HEALTH),
		partitionGeneration: NewAtomicInt(-1),
		referenceCount:      NewAtomicInt(0),
		responded:           NewAtomicBool(false),
		active:              NewAtomicBool(true),
	}
}

// Request current status from server node, and update node with the result
func (nd *Node) Refresh() ([]*Host, error) {
	var friends []*Host

	conn, err := nd.GetConnection(1 * time.Second)
	if err != nil {
		return nil, err
	}

	infoMap, err := RequestInfo(conn, "node", "partition-generation", "services")
	if err != nil {
		conn.Close()
		nd.DecreaseHealth()
		return nil, err
	}

	if err := nd.verifyNodeName(infoMap); err != nil {
		return nil, err
	}
	nd.RestoreHealth()
	nd.responded.Set(true)

	if friends, err = nd.addFriends(infoMap); err != nil {
		return nil, err
	}

	if err := nd.updatePartitions(conn, infoMap); err != nil {
		return nil, err
	}
	nd.PutConnection(conn)
	return friends, nil
}

func (nd *Node) verifyNodeName(infoMap map[string]string) error {
	infoName, exists := infoMap["node"]

	if !exists || len(infoName) == 0 {
		nd.DecreaseHealth()
		return errors.New("Node name is empty")
	}

	if !(nd.name == infoName) {
		// Set node to inactive immediately.
		nd.active.Set(false)
		return errors.New("Node name has changed. Old=" + nd.name + " New=" + infoName)
	}
	return nil
}

func (nd *Node) getUseNewInfo() bool {
	return nd.useNewInfo.Get()
}

func (nd *Node) addFriends(infoMap map[string]string) ([]*Host, error) {
	friendString, exists := infoMap["services"]
	var friends []*Host

	if !exists || len(friendString) == 0 {
		return friends, nil
	}

	friendNames := strings.Split(friendString, ";")

	for _, friend := range friendNames {
		friendInfo := strings.Split(friend, ":")
		host := friendInfo[0]
		port, _ := strconv.Atoi(friendInfo[1])
		alias := NewHost(host, port)
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
		return errors.New("partition-generation is empty")
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

// Get a connection to the node. If no cached connection is not available,
// a new connection will be created
func (nd *Node) GetConnection(timeout time.Duration) (conn *Connection, err error) {
	for t := nd.connections.Poll(); t != nil; t = nd.connections.Poll() {
		conn = t.(*Connection)
		if conn.IsConnected() {
			if err := conn.SetTimeout(timeout); err == nil {
				return conn, nil
			}
		}
		conn.Close()
	}

	if conn, err = NewConnection(nd.address, nd.cluster.connectionTimeout); err != nil {
		return nil, err
	}
	if conn.SetTimeout(timeout) != nil {
		return nil, err
	}
	return conn, nil
}

// Put back a connection to the cache. If cache is full, the connection will be
// closed and discarded
func (nd *Node) PutConnection(conn *Connection) {
	if !nd.active.Get() || !nd.connections.Offer(conn) {
		conn.Close()
	}
}

// Mark the node as healthy
func (nd *Node) RestoreHealth() {
	// There can be cases where health is full, but active is false.
	// Once a node has been marked inactive, it stays inactive.
	nd.health.Set(_FULL_HEALTH)
}

// Decrease node Health as a result of bad connection or communication
func (nd *Node) DecreaseHealth() {
	nd.health.DecrementAndGet()
}

// Check if the node is unhealthy
func (nd *Node) IsUnhealthy() bool {
	return nd.health.Get() <= 0
}

// Retrieves host for the node
func (nd *Node) GetHost() *Host {
	return nd.host
}

// Checks if the node is active
func (nd *Node) IsActive() bool {
	return nd.active.Get()
}

// Returns node name
func (nd *Node) GetName() string {
	return nd.name
}

// Returns node aliases
func (nd *Node) GetAliases() []*Host {
	nd.mutex.RLock()
	defer nd.mutex.RUnlock()
	aliases := nd.aliases
	return aliases
}

// Sets node aliases
func (nd *Node) setAliases(aliases []*Host) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	nd.aliases = aliases
}

// Adds an alias for the node
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

// Marks node as inactice and closes all cached connections
func (nd *Node) Close() {
	nd.active.Set(false)
	nd.closeConnections()
}

// Implements stringer interface
func (nd *Node) String() string {
	return nd.name + " " + nd.host.String()
}

func (nd *Node) closeConnections() {
	for conn := nd.connections.Poll(); conn != nil; conn = nd.connections.Poll() {
		conn.(*Connection).Close()
	}
}

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

func (nd *Node) WaitUntillMigrationIsFinished(timeout time.Duration) (err error) {
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

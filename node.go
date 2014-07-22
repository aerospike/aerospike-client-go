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

	. "github.com/citrusleaf/go-client/logger"
	. "github.com/citrusleaf/go-client/types/atomic"
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
	mutex               *sync.RWMutex
}

// Initialize server node with connection parameters.
func NewNode(cluster *Cluster, nv *NodeValidator) *Node {
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
		mutex:               new(sync.RWMutex),
	}
}

// Request current status from server node, and update node with the result
func (this *Node) Refresh(friends []*Host) error {
	conn, err := this.GetConnection(1 * time.Second)
	if err != nil {
		return err
	}

	if infoMap, err := RequestInfo(conn, "node", "partition-generation", "services"); err != nil {
		conn.Close()
		this.DecreaseHealth()
		return err
	} else {
		this.verifyNodeName(infoMap)
		this.RestoreHealth()
		this.responded.Set(true)
		this.addFriends(infoMap, friends)
		this.updatePartitions(conn, infoMap)
		this.PutConnection(conn)
	}
	return nil
}

func (this *Node) verifyNodeName(infoMap map[string]string) error {
	infoName, exists := infoMap["node"]

	if !exists || len(infoName) == 0 {
		this.DecreaseHealth()
		return errors.New("Node name is empty")
	}

	if !(this.name == infoName) {
		// Set node to inactive immediately.
		this.active.Set(false)
		return errors.New("Node name has changed. Old=" + this.name + " New=" + infoName)
	}
	return nil
}

func (this *Node) getUseNewInfo() bool {
	return this.useNewInfo.Get()
}

func (this *Node) addFriends(infoMap map[string]string, friends []*Host) error {
	friendString, exists := infoMap["services"]

	if !exists || len(friendString) == 0 {
		return nil
	}

	friendNames := strings.Split(friendString, ";")

	for _, friend := range friendNames {
		friendInfo := strings.Split(friend, ":")
		host := friendInfo[0]
		port, _ := strconv.Atoi(friendInfo[1])
		alias := NewHost(host, port)
		node := this.cluster.findAlias(alias)

		if node != nil {
			node.referenceCount.IncrementAndGet()
		} else {
			if !this.findAlias(friends, alias) {
				// TODO: wrong abstraction; should return friend list to caller
				friends = append(friends, alias)
			}
		}
	}

	return nil
}

func (this *Node) findAlias(friends []*Host, alias *Host) bool {
	for _, host := range friends {
		if *host == *alias {
			return true
		}
	}
	return false
}

func (this *Node) updatePartitions(conn *Connection, infoMap map[string]string) error {
	genString, exists := infoMap["partition-generation"]

	if !exists || len(genString) == 0 {
		return errors.New("partition-generation is empty")
	}

	generation, _ := strconv.Atoi(genString)

	if this.partitionGeneration.Get() != generation {
		Logger.Info("Node %s partition generation %d changed", this.GetName(), generation)
		this.cluster.updatePartitions(conn, this)
		this.partitionGeneration.Set(generation)
	}

	return nil
}

// Get a connection to the node. If no cached connection is not available,
// a new connection will be created
func (this *Node) GetConnection(timeout time.Duration) (*Connection, error) {
	var conn *Connection
	t := this.connections.Poll()
	for ; t != nil; t = this.connections.Poll() {
		conn = t.(*Connection)
		if conn.IsConnected() {
			conn.SetTimeout(timeout)
			return conn, nil
		}
		conn.Close()
	}
	return NewConnection(this.address, timeout)
}

// Put back a connection to the cache. If cache is full, the connection will be
// closed and discarded
func (this *Node) PutConnection(conn *Connection) {
	if !this.active.Get() || !this.connections.Offer(conn) {
		go conn.Close()
	}
}

// Mark the node as healthy
func (this *Node) RestoreHealth() {
	// There can be cases where health is full, but active is false.
	// Once a node has been marked inactive, it stays inactive.
	this.health.Set(_FULL_HEALTH)
}

// Decrease node Health as a result of bad connection or communication
func (this *Node) DecreaseHealth() {
	this.health.DecrementAndGet()
}

// Check if the node is unhealthy
func (this *Node) IsUnhealthy() bool {
	return this.health.Get() <= 0
}

// Retrieves host for the node
func (this *Node) GetHost() *Host {
	return this.host
}

// Checks if the node is active
func (this *Node) IsActive() bool {
	return this.active.Get()
}

// Returns node name
func (this *Node) GetName() string {
	return this.name
}

// Returns node aliases
func (this *Node) GetAliases() []*Host {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	aliases := this.aliases
	return aliases
}

// Sets node aliases
func (this *Node) setAliases(aliases []*Host) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.aliases = aliases
}

// Adds an alias for the node
func (this *Node) AddAlias(aliasToAdd *Host) {
	// Aliases are only referenced in the cluster tend thread,
	// so synchronization is not necessary.
	aliases := this.GetAliases()
	count := len(aliases) + 1
	tmpAliases := make([]*Host, count)

	for idx, host := range aliases {
		tmpAliases[idx] = host
	}
	tmpAliases[count] = aliasToAdd

	this.setAliases(tmpAliases)
}

// Marks node as inactice and closes all cached connections
func (this *Node) Close() {
	this.active.Set(false)
	go this.closeConnections()
}

// Implements stringer interface
func (this *Node) String() string {
	return this.name + " " + this.host.String()
}

func (this *Node) closeConnections() {
	for conn := this.connections.Poll(); conn != nil; {
		conn.(*Connection).Close()
	}
}

func (this *Node) Equals(other *Node) bool {
	return this.name == other.name
}

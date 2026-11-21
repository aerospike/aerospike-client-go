// Copyright 2014-2026 Aerospike, Inc.
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

package main

import (
	"context"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	partitionCount     = 4096
	routingPollEvery   = 10 * time.Millisecond
	heartbeatEvery     = 100 * time.Millisecond
	reachabilityEvery  = 500 * time.Millisecond
	reachabilityBudget = time.Second
)

// nodeView is what a snapshot reports for one node, apart from reachability.
type nodeView struct {
	node   *as.Node
	name   string
	host   string
	port   int
	active bool
}

// heldNodes is every node the client holds, sorted by name.
func heldNodes(client *as.Client) []nodeView {
	nodes := client.GetNodes()
	views := make([]nodeView, 0, len(nodes))
	for _, node := range nodes {
		host := node.GetHost()
		views = append(views, nodeView{node: node, name: node.GetName(), host: host.Name, port: host.Port, active: node.IsActive()})
	}
	sort.Slice(views, func(left, right int) bool { return views[left].name < views[right].name })
	return views
}

// ownership is the partition map of the namespace as the client holds it.
type ownership struct {
	unowned int
	owners  map[string]int
}

// masterMap reads the master of every partition of one namespace from the
// client's partition map.
type masterMap struct {
	client       *as.Client
	namespaceKey *as.Key
	masterPolicy *as.BasePolicy
}

func newMasterMap(client *as.Client, namespace string) (*masterMap, error) {
	namespaceKey, err := as.NewKey(namespace, "", "l5-owner")
	if err != nil {
		return nil, err
	}
	masterPolicy := as.NewPolicy()
	masterPolicy.ReplicaPolicy = as.MASTER
	return &masterMap{client: client, namespaceKey: namespaceKey, masterPolicy: masterPolicy}, nil
}

// ownership counts the partitions whose master is missing, not active, or
// not the node the client holds under that name, reading every partition
// from one copy of the map.
func (masters *masterMap) ownership(views []nodeView) ownership {
	result := ownership{owners: make(map[string]int, len(views))}
	cluster := masters.client.Cluster()
	partition, err := as.PartitionForWrite(cluster, masters.masterPolicy, masters.namespaceKey)
	if err != nil {
		result.unowned = partitionCount
		return result
	}

	held := make(map[string]*as.Node, len(views))
	for _, view := range views {
		held[view.name] = view.node
	}
	for partitionID := 0; partitionID < partitionCount; partitionID++ {
		partition.PartitionId = partitionID
		master, err := partition.GetNodeWrite(cluster)
		if err != nil || master == nil || held[master.GetName()] != master {
			result.unowned++
			continue
		}
		result.owners[master.GetName()]++
	}
	return result
}

// routingKey changes when a node is added or removed, a node's host, port or
// active changes, or the unowned count changes.
func routingKey(views []nodeView, unowned int) string {
	key := make([]byte, 0, 48*len(views)+8)
	for _, view := range views {
		key = append(key, view.name...)
		key = append(key, '@')
		key = append(key, view.host...)
		key = append(key, ':')
		key = strconv.AppendInt(key, int64(view.port), 10)
		key = append(key, '/')
		key = strconv.AppendBool(key, view.active)
		key = append(key, ';')
	}
	return string(strconv.AppendInt(key, int64(unowned), 10))
}

// reachability is the latest check of one node.
type reachability struct {
	reachable bool
	address   *string
}

// routingMonitor polls the client's routing, sends snapshots, checks every
// node's reachability and answers whether a node is usable.
type routingMonitor struct {
	client  *as.Client
	output  *driverOutput
	stats   *loadStats
	masters *masterMap

	// Read and written only under the output lock.
	polled           bool
	lastRouting      string
	nextHeartbeat    int64
	unownedSince     *int64
	longestUnownedMs int64

	checkLock sync.Mutex
	checks    map[*as.Node]reachability
	checking  map[*as.Node]bool
}

func newRoutingMonitor(client *as.Client, output *driverOutput, stats *loadStats, namespace string) (*routingMonitor, error) {
	masters, err := newMasterMap(client, namespace)
	if err != nil {
		return nil, err
	}
	return &routingMonitor{
		client:   client,
		output:   output,
		stats:    stats,
		masters:  masters,
		checks:   map[*as.Node]reachability{},
		checking: map[*as.Node]bool{},
	}, nil
}

// poll reads the routing once, sends a change snapshot when it differs from
// the previous poll, and a heartbeat when one is due.
func (monitor *routingMonitor) poll() {
	views := heldNodes(monitor.client)
	current := monitor.masters.ownership(views)
	key := routingKey(views, current.unowned)

	monitor.output.atomically(func(t int64, emit func(string, line)) {
		longest := monitor.trackUnowned(t, current.unowned)
		if !monitor.polled {
			monitor.polled = true
			monitor.nextHeartbeat = t
		} else if key != monitor.lastRouting {
			emit("snapshot", monitor.snapshot("change", views, current, longest))
		}
		monitor.lastRouting = key

		if t >= monitor.nextHeartbeat {
			emit("snapshot", monitor.snapshot("heartbeat", views, current, longest))
			monitor.nextHeartbeat = max(monitor.nextHeartbeat+heartbeatEvery.Milliseconds(), t+1)
		}
	})
}

// trackUnowned returns the longest stretch since the last reset in which
// partitions were unowned, including a stretch still open at t.
func (monitor *routingMonitor) trackUnowned(t int64, unowned int) int64 {
	if unowned > 0 && monitor.unownedSince == nil {
		monitor.unownedSince = &t
	} else if unowned == 0 && monitor.unownedSince != nil {
		monitor.longestUnownedMs = max(monitor.longestUnownedMs, t-*monitor.unownedSince)
		monitor.unownedSince = nil
	}
	if monitor.unownedSince != nil {
		return max(monitor.longestUnownedMs, t-*monitor.unownedSince)
	}
	return monitor.longestUnownedMs
}

// reset restarts the longest unowned stretch from t. The caller holds the
// output lock.
func (monitor *routingMonitor) reset(t int64) {
	monitor.longestUnownedMs = 0
	if monitor.unownedSince != nil {
		monitor.unownedSince = &t
	}
}

func (monitor *routingMonitor) snapshot(reason string, views []nodeView, current ownership, longestUnownedMs int64) line {
	monitor.checkLock.Lock()
	nodes := make([]line, 0, len(views))
	for _, view := range views {
		object := line{"name": view.name, "host": view.host, "port": view.port, "active": view.active,
			"address": nil, "reachable": nil}
		if net.ParseIP(view.host) != nil {
			object["address"] = view.host
		}
		if check, checked := monitor.checks[view.node]; checked {
			object["reachable"] = check.reachable
			if check.address != nil {
				object["address"] = *check.address
			}
		}
		nodes = append(nodes, object)
	}
	monitor.checkLock.Unlock()

	object := line{
		"reason":           reason,
		"nodes":            nodes,
		"unowned":          current.unowned,
		"owners":           current.owners,
		"longestUnownedMs": longestUnownedMs,
	}
	monitor.stats.addTo(object)
	return object
}

// usable reports whether the client holds the node, it is active, and no
// partition is unowned. It leaves the poll's state alone.
func (monitor *routingMonitor) usable(nodeName string) bool {
	views := heldNodes(monitor.client)
	for _, view := range views {
		if view.name == nodeName {
			return view.active && monitor.masters.ownership(views).unowned == 0
		}
	}
	return false
}

// startChecks starts a check of every node, each on its own goroutine,
// skipping a node whose previous check is still running.
func (monitor *routingMonitor) startChecks() {
	nodes := monitor.client.GetNodes()
	held := make(map[*as.Node]bool, len(nodes))

	monitor.checkLock.Lock()
	defer monitor.checkLock.Unlock()
	for _, node := range nodes {
		held[node] = true
		if monitor.checking[node] {
			continue
		}
		monitor.checking[node] = true
		go func(node *as.Node) {
			result := checkNode(node)
			monitor.checkLock.Lock()
			monitor.checks[node] = result
			delete(monitor.checking, node)
			monitor.checkLock.Unlock()
		}(node)
	}
	for node := range monitor.checks {
		if !held[node] {
			delete(monitor.checks, node)
		}
	}
}

// checkNode asks the node for its name over a pooled connection, as a
// command would, and resolves the host it connects to.
func checkNode(node *as.Node) reachability {
	result := reachability{reachable: answersWithName(node)}
	host := node.GetHost().Name
	if net.ParseIP(host) != nil {
		result.address = &host
		return result
	}
	ctx, cancel := context.WithTimeout(context.Background(), reachabilityBudget)
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	cancel()
	if err == nil && len(addresses) > 0 {
		result.address = &addresses[0]
	}
	return result
}

// answersWithName reports whether an info request for node answers with the
// node's name within the reachability budget.
func answersWithName(node *as.Node) bool {
	deadline := time.Now().Add(reachabilityBudget)
	conn, err := node.GetConnection(reachabilityBudget)
	if err != nil {
		return false
	}
	_ = conn.SetTimeout(deadline, 0)
	info, err := conn.RequestInfo("node")
	if err != nil {
		node.InvalidateConnection(conn)
		return false
	}
	node.PutConnection(conn)
	return info["node"] == node.GetName()
}

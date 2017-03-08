// +build !as_performance

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
	"reflect"

	. "github.com/aerospike/aerospike-client-go/types"
	xornd "github.com/aerospike/aerospike-client-go/types/rand"
)

// PutObject writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) PutObject(policy *WritePolicy, key *Key, obj interface{}) (err error) {
	policy = clnt.getUsableWritePolicy(policy)

	bins := marshal(obj, clnt.cluster.supportsFloat.Get())
	command := newWriteCommand(clnt.cluster, policy, key, bins, nil, WRITE)
	res := command.Execute()
	binPool.Put(bins)
	return res
}

// GetObject reads a record for specified key and puts the result into the provided object.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) GetObject(policy *BasePolicy, key *Key, obj interface{}) error {
	policy = clnt.getUsablePolicy(policy)

	rval := reflect.ValueOf(obj)
	binNames := objectMappings.getFields(rval.Type())

	command := newReadCommand(clnt.cluster, policy, key, binNames)
	command.object = &rval
	return command.Execute()
}

// ScanAllObjects reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanAllObjects(apolicy *ScanPolicy, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// result recordset
	taskId := uint64(xornd.Int64())
	os := newObjectset(reflect.ValueOf(objChan), len(nodes), taskId)
	res := &Recordset{
		objectset: *os,
	}

	// the whole call should be wrapped in a goroutine
	if policy.ConcurrentNodes {
		for _, node := range nodes {
			go func(node *Node) {
				// Errors are handled inside the command itself
				clnt.scanNodeObjects(&policy, node, res, namespace, setName, taskId, binNames...)
			}(node)
		}
	} else {
		// scan nodes one by one
		go func() {
			for _, node := range nodes {
				// Errors are handled inside the command itself
				clnt.scanNodeObjects(&policy, node, res, namespace, setName, taskId, binNames...)
			}
		}()
	}

	return res, nil
}

// scanNodeObjects reads all records in specified namespace and set for one node only,
// and marshalls the results into the objects of the provided channel in Recordset.
// If the policy is nil, the default relevant policy will be used.
// The resulting records will be marshalled into the objChan.
// objChan will be closed after all the records are read.
func (clnt *Client) ScanNodeObjects(apolicy *ScanPolicy, node *Node, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	// results channel must be async for performance
	taskId := uint64(xornd.Int64())
	os := newObjectset(reflect.ValueOf(objChan), 1, taskId)
	res := &Recordset{
		objectset: *os,
	}

	go clnt.scanNodeObjects(&policy, node, res, namespace, setName, taskId, binNames...)
	return res, nil
}

// scanNodeObjects reads all records in specified namespace and set for one node only,
// and marshalls the results into the objects of the provided channel in Recordset.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) scanNodeObjects(policy *ScanPolicy, node *Node, recordset *Recordset, namespace string, setName string, taskId uint64, binNames ...string) error {
	if policy.WaitUntilMigrationsAreOver {
		// wait until migrations on node are finished
		if err := node.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			recordset.signalEnd()
			return err
		}
	}

	command := newScanObjectsCommand(node, policy, namespace, setName, binNames, recordset, taskId)
	return command.Execute()
}

// QueryNodeObjects executes a query on all nodes in the cluster and marshals the records into the given channel.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop objects.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryObjects(policy *QueryPolicy, statement *Statement, objChan interface{}) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	os := newObjectset(reflect.ValueOf(objChan), len(nodes), statement.TaskId)
	recSet := &Recordset{
		objectset: *os,
	}

	// the whole call sho
	// results channel must be async for performance
	for _, node := range nodes {
		// copy policies to avoid race conditions
		newPolicy := *policy
		command := newQueryObjectsCommand(node, &newPolicy, statement, recSet)
		go func() {
			// Do not send the error to the channel; it is already handled in the Execute method
			command.Execute()
		}()
	}

	return recSet, nil
}

// QueryNodeObjects executes a query on a specific node and marshals the records into the given channel.
// The caller can concurrently pop records off the channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryNodeObjects(policy *QueryPolicy, node *Node, statement *Statement, objChan interface{}) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	os := newObjectset(reflect.ValueOf(objChan), 1, statement.TaskId)
	recSet := &Recordset{
		objectset: *os,
	}

	// copy policies to avoid race conditions
	newPolicy := *policy
	command := newQueryRecordCommand(node, &newPolicy, statement, recSet)
	go func() {
		command.Execute()
	}()

	return recSet, nil
}

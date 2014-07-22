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

	// . "github.com/citrusleaf/go-client/logger"
)

type Client struct {
	cluster *Cluster
}

//-------------------------------------------------------
// Constructors
//-------------------------------------------------------

// NewClient generates a new Client
func NewClient(hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(NewClientPolicy(), NewHost(hostname, port))
}

// NewClientWithPolicy generates a new Client and sets the ClientPolicy
func NewClientWithPolicy(policy *ClientPolicy, hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
}

// NewClientWithPolicyAndHost generates a new Client and sets the ClientPolicy and sets up the cluster
func NewClientWithPolicyAndHost(policy *ClientPolicy, hosts ...*Host) (*Client, error) {
	if policy == nil {
		policy = NewClientPolicy()
	}

	if cluster, err := NewCluster(policy, hosts); err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect to host(s): %v", hosts))
	} else {
		return &Client{cluster: cluster}, nil
	}

	// TODO: Leaked abstraction; if cluster didn't adhere to the policy,
	// it should say so itself
	// if policy.FailIfNotConnected && !newClient.cluster.IsConnected() {
	// 	return nil, errors.New(fmt.Sprintf("Failed to connect to host(s): %v", hosts))
	// }
}

//-------------------------------------------------------
// Cluster Connection Management
//-------------------------------------------------------

//  Close all client connections to database server nodes.
func (this *Client) Close() {
	this.cluster.Close()
}

//  Determine if we are ready to talk to the database server cluster.
func (this *Client) IsConnected() bool {
	return this.cluster.IsConnected()
}

//  Return array of active server nodes in the cluster.
func (this *Client) GetNodes() []*Node {
	return this.cluster.GetNodes()
}

//  Return list of active server node names in the cluster.
func (this *Client) GetNodeNames() []string {
	nodes := this.cluster.GetNodes()
	names := make([]string, len(nodes))

	for _, node := range nodes {
		names = append(names, node.GetName())
	}
	return names
}

//-------------------------------------------------------
// Write Record Operations
//-------------------------------------------------------

//  Write record bin(s).
//  The policy specifies the transaction timeout, record expiration and how the transaction is
//  handled when the record already exists.
func (this *Client) Put(policy *WritePolicy, key *Key, bins BinMap) error {
	return this.PutBins(policy, key, mapToBins(bins)...)
}

func (this *Client) PutBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewWriteCommand(this.cluster, policy, key, bins, WRITE)
	return command.Execute()
}

//-------------------------------------------------------
// Operations string
//-------------------------------------------------------

//  Append bin values string to existing record bin values.
//  The policy specifies the transaction timeout, record expiration and how the transaction is
//  handled when the record already exists.
//  This call only works for string values.
func (this *Client) Append(policy *WritePolicy, key *Key, bins BinMap) error {
	return this.AppendBins(policy, key, mapToBins(bins)...)
}

func (this *Client) AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewWriteCommand(this.cluster, policy, key, bins, APPEND)
	return command.Execute()
}

//  Prepend bin values string to existing record bin values.
//  The policy specifies the transaction timeout, record expiration and how the transaction is
//  handled when the record already exists.
//  This call works only for string values.
func (this *Client) Prepend(policy *WritePolicy, key *Key, bins BinMap) error {
	return this.PrependBins(policy, key, mapToBins(bins)...)
}

func (this *Client) PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewWriteCommand(this.cluster, policy, key, bins, PREPEND)
	return command.Execute()
}

//-------------------------------------------------------
// Arithmetic Operations
//-------------------------------------------------------

//  Add integer bin values to existing record bin values.
//  The policy specifies the transaction timeout, record expiration and how the transaction is
//  handled when the record already exists.
//  This call only works for integer values.
func (this *Client) Add(policy *WritePolicy, key *Key, bins BinMap) error {
	return this.AddBins(policy, key, mapToBins(bins)...)
}

func (this *Client) AddBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewWriteCommand(this.cluster, policy, key, bins, ADD)
	return command.Execute()
}

//-------------------------------------------------------
// Delete Operations
//-------------------------------------------------------

//  Delete record for specified key.
//  The policy specifies the transaction timeout.
func (this *Client) Delete(policy *WritePolicy, key *Key) (bool, error) {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewDeleteCommand(this.cluster, policy, key)
	err := command.Execute()
	return command.Existed(), err
}

//-------------------------------------------------------
// Touch Operations
//-------------------------------------------------------

//  Create record if it does not already exist.  If the record exists, the record's
//  time to expiration will be reset to the policy's expiration.
func (this *Client) Touch(policy *WritePolicy, key *Key) error {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewTouchCommand(this.cluster, policy, key)
	return command.Execute()
}

//-------------------------------------------------------
// Existence-Check Operations
//-------------------------------------------------------

//  Determine if a record key exists.
//  The policy can be used to specify timeouts.
func (this *Client) Exists(policy *BasePolicy, key *Key) (bool, error) {
	if policy == nil {
		policy = NewPolicy()
	}
	command := NewExistsCommand(this.cluster, policy, key)
	err := command.Execute()
	return command.Exists(), err
}

// TODO: INCLUDE this function in the above

// //  Check if multiple record keys exist in one batch call.
// //  The returned array bool is in positional order with the original key array order.
// //  The policy can be used to specify timeouts.
// func (this *Client) Exists2(policy *BasePolicy, keys []Key) ([]bool, error) {
// }

//-------------------------------------------------------
// Read Record Operations
//-------------------------------------------------------

//  Read record header and bins for specified key.
//  The policy can be used to specify timeouts.
func (this *Client) Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, error) {
	if policy == nil {
		policy = NewPolicy()
	}
	command := NewReadCommand(this.cluster, policy, key, binNames)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//  Read record generation and expiration only for specified key.  Bins are not read.
//  The policy can be used to specify timeouts.
func (this *Client) GetHeader(policy *BasePolicy, key *Key) (*Record, error) {
	if policy == nil {
		policy = NewPolicy()
	}
	command := NewReadHeaderCommand(this.cluster, policy, key)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

// //-------------------------------------------------------
// // Batch Read Operations
// //-------------------------------------------------------

// //  Read multiple records for specified keys in one batch call.
// //  The returned records are in positional order with the original key array order.
// //  If a key is not found, the positional record will be null.
// //  The policy can be used to specify timeouts.
// func (this *Client) Get(policy Policy, keys []Key) ([]Record, error) {
// }

// //  Read multiple record headers and bins for specified keys in one batch call.
// //  The returned records are in positional order with the original key array order.
// //  If a key is not found, the positional record will be null.
// //  The policy can be used to specify timeouts.
// func (this *Client) Get(policy Policy, keys []Key, binNames ...string) []Record {
// }

// //  Read multiple record header data for specified keys in one batch call.
// //  The returned records are in positional order with the original key array order.
// //  If a key is not found, the positional record will be null.
// //  The policy can be used to specify timeouts.
// func (this *Client) GetHeader(policy Policy, keys []Key) ([]Record, error) {
// }

// //-------------------------------------------------------
// // Generic Database Operations
// //-------------------------------------------------------

// //  Perform multiple read/write operations on a single key in one batch call.
// //  An example would be to add an integer value to an existing record and then
// //  read the result, all in one database call.
// //  <p>
// //  Write operations are always performed first, regardless of operation order
// //  relative to read operations.
// func (this *Client) Operate(policy WritePolicy, key Key, operations ...Operation) (Record, error) {
// }

// //-------------------------------------------------------
// // Scan Operations
// //-------------------------------------------------------

// //  Read all records in specified namespace and set.  If the policy's
// //  <code>concurrent*Nodes</code> is specified, each server node will be read in
// //  parallel.  Otherwise, server nodes are read in series.
// //  <p>
// //  This call will block until the scan is complete - callbacks are made
// //  within the scope of this call.
// func (this *Client) ScanAll(policy ScanPolicy, namespace string, setName string, callback ScanCallback, binNames ...string) {
// }

// //  Read all records in specified namespace and set for one node only.
// //  The node is specified by name.
// //  <p>
// //  This call will block until the scan is complete - callbacks are made
// //  within the scope of this call.
// func (this *Client) ScanNode(policy ScanPolicy, nodeName string, namespace string, setName string, callback ScanCallback, binNames ...string) {
// }

// //  Read all records in specified namespace and set for one node only.
// //  <p>
// //  This call will block until the scan is complete - callbacks are made
// //  within the scope of this call.
// func (this *Client) ScanNode(policy ScanPolicy, node Node, namespace string, setName string, callback ScanCallback, binNames ...string) {
// }

// //-------------------------------------------------------------------
// // Large collection functions (Supported by Aerospike 3 servers only)
// //-------------------------------------------------------------------

// //  Initialize large list operator.  This operator can be used to create and manage a list
// //  within a single bin.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) GetLargeList(policy Policy, key Key, binName string, userModule string) LargeList {
// }

// //  Initialize large map operator.  This operator can be used to create and manage a map
// //  within a single bin.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) GetLargeMap(policy Policy, key Key, binName string, userModule string) LargeMap {
// }

// //  Initialize large set operator.  This operator can be used to create and manage a set
// //  within a single bin.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) GetLargeSet(policy Policy, key Key, binName string, userModule string) LargeSet {
// }

// //  Initialize large stack operator.  This operator can be used to create and manage a stack
// //  within a single bin.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) GetLargeStack(policy Policy, key Key, binName string, userModule string) LargeStack {
// }

// //---------------------------------------------------------------
// // User defined functions (Supported by Aerospike 3 servers only)
// //---------------------------------------------------------------

// //  Register package containing user defined functions with server.
// //  This asynchronous server call will return before command is complete.
// //  The user can optionally wait for command completion by using the returned
// //  RegisterTask instance.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) Register(policy Policy, clientPath string, serverPath string, language Language) RegisterTask {
// }

// //  Execute user defined function on server and return results.
// //  The function operates on a single record.
// //  The package name is used to locate the udf file location:
// //  <p>
// //  udf file = <server udf dir>/<package name>.lua
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) Execute(policy Policy, key Key, packageName string, functionName string, args ...Value) Object {
// }

// //----------------------------------------------------------
// // Query/Execute UDF (Supported by Aerospike 3 servers only)
// //----------------------------------------------------------

// //  Apply user defined function on records that match the statement filter.
// //  Records are not returned to the client.
// //  This asynchronous server call will return before command is complete.
// //  The user can optionally wait for command completion by using the returned
// //  ExecuteTask instance.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) Execute2(policy Policy,
// 	statement Statement,
// 	packageName string,
// 	functionName string,
// 	functionArgs ...Value,
// ) (ExecuteTask, error) {
// }

// //--------------------------------------------------------
// // Query functions (Supported by Aerospike 3 servers only)
// //--------------------------------------------------------

// //  Execute query and return record iterator.  The query executor puts records on a queue in
// //  separate threads.  The calling thread concurrently pops records off the queue through the
// //  record iterator.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) Query(policy QueryPolicy, statement Statement) (RecordSet, error) {
// }

// //  Execute query, apply statement's aggregation function, and return result iterator. The query
// //  executor puts results on a queue in separate threads.  The calling thread concurrently pops
// //  results off the queue through the result iterator.
// //  <p>
// //  The aggregation function is called on both server and client (reduce).  Therefore,
// //  the Lua script files must also reside on both server and client.
// //  The package name is used to locate the udf file location:
// //  <p>
// //  udf file = <udf dir>/<package name>.lua
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) QueryAggregate(
// 	policy QueryPolicy,
// 	statement Statement,
// 	packageName string,
// 	functionName string,
// 	functionArgs ...Value,
// ) (ResultSet, error) {
// }

// //  Create secondary index.
// //  This asynchronous server call will return before command is complete.
// //  The user can optionally wait for command completion by using the returned
// //  IndexTask instance.
// //  <p>
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) CreateIndex(
// 	policy Policy,
// 	namespace string,
// 	setName string,
// 	indexName string,
// 	binName string,
// 	indexType IndexType,
// ) (IndexTask, error) {

// }

// //  Delete secondary index.
// //  This method is only supported by Aerospike 3 servers.
// func (this *Client) DropIndex(
// 	policy Policy,
// 	namespace string,
// 	setName string,
// 	indexName string,
// ) error {
// }

// //-------------------------------------------------------
// // Internal Methods
// //-------------------------------------------------------
// func (this *Client) binNamesToHashSet(binNames []string) BinMap {
// }

// func (this *Client) sendInfoCommand(policy Policy, command string) (string, error) {
// }

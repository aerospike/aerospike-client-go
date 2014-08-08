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
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	"github.com/aerospike/aerospike-client-go/utils"
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

//  Check if multiple record keys exist in one batch call.
//  The returned array bool is in positional order with the original key array order.
//  The policy can be used to specify timeouts.
func (this *Client) BatchExists(policy *BasePolicy, keys []*Key) ([]bool, error) {
	if policy == nil {
		policy = NewPolicy()
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be marked true
	// TODO: Investigate CPU cache invalidation semantics
	existsArray := make([]bool, len(keys))

	keyMap := NewBatchItemList(keys)

	if err := this.batchExecute(keys, func(node *Node, bns *batchNamespace) Command {
		return NewBatchCommandExists(node, bns, policy, keyMap, existsArray)
	}); err != nil {
		return nil, err
	}

	return existsArray, nil
}

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

//-------------------------------------------------------
// Batch Read Operations
//-------------------------------------------------------

//  Read multiple record headers and bins for specified keys in one batch call.
//  The returned records are in positional order with the original key array order.
//  If a key is not found, the positional record will be null.
//  The policy can be used to specify timeouts.
func (this *Client) BatchGet(policy *BasePolicy, keys []*Key, binNames ...string) ([]*Record, error) {
	if policy == nil {
		policy = NewPolicy()
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be set to record
	// TODO: Investigate CPU cache invalidation semantics
	records := make([]*Record, len(keys))

	keyMap := NewBatchItemList(keys)
	binSet := map[string]struct{}{}
	for idx := range binNames {
		binSet[binNames[idx]] = struct{}{}
	}

	err := this.batchExecute(keys, func(node *Node, bns *batchNamespace) Command {
		return NewBatchCommandGet(node, bns, policy, keyMap, binSet, records, INFO1_READ)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

//  Read multiple record header data for specified keys in one batch call.
//  The returned records are in positional order with the original key array order.
//  If a key is not found, the positional record will be null.
//  The policy can be used to specify timeouts.
func (this *Client) BatchGetHeader(policy *BasePolicy, keys []*Key) ([]*Record, error) {
	if policy == nil {
		policy = NewPolicy()
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be set to record
	// TODO: Investigate CPU cache invalidation semantics
	records := make([]*Record, len(keys))

	keyMap := NewBatchItemList(keys)
	err := this.batchExecute(keys, func(node *Node, bns *batchNamespace) Command {
		return NewBatchCommandGet(node, bns, policy, keyMap, nil, records, INFO1_READ|INFO1_NOBINDATA)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

//-------------------------------------------------------
// Generic Database Operations
//-------------------------------------------------------

//  Perform multiple read/write operations on a single key in one batch call.
//  An example would be to add an integer value to an existing record and then
//  read the result, all in one database call.
//
//  Write operations are always performed first, regardless of operation order
//  relative to read operations.
func (this *Client) Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, error) {
	command := NewOperateCommand(this.cluster, policy, key, operations)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//-------------------------------------------------------
// Scan Operations
//-------------------------------------------------------

//  Read all records in specified namespace and set.  If the policy's
//  <code>concurrent*Nodes</code> is specified, each server node will be read in
//  parallel.  Otherwise, server nodes are read in series.
//
//  This call will block until the scan is complete - callbacks are made
//  within the scope of this call.
func (this *Client) ScanAll(policy *ScanPolicy, namespace string, setName string, binNames ...string) (chan *Record, error) {
	if policy == nil {
		policy = NewScanPolicy()
	}

	// Retry policy must be one-shot for scans.
	policy.MaxRetries = 0

	nodes := this.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
	}

	// results channel must be async for performance
	var resChan chan *Record

	// the whole call should be wrapped in a goroutine
	if policy.ConcurrentNodes {
		// results channel must be async for performance
		recChans := []chan *Record{}
		for _, node := range nodes {
			if recChan, err := this.ScanNode(policy, node, namespace, setName, binNames...); err != nil {
				return nil, err
			} else {
				recChans = append(recChans, recChan)
			}
		}
		resChan = this.mergeResultChannels(recChans...)
	} else {
		resChan = make(chan *Record, 1024)
		go func() {
			defer close(resChan)
			for _, node := range nodes {
				if tempChan, err := this.ScanNode(policy, node, namespace, setName, binNames...); err != nil {
					return
				} else {
					for rec := range tempChan {
						resChan <- rec
					}
				}
			}
		}()
	}

	return resChan, nil
}

//  Read all records in specified namespace and set for one node only.
//  The node is specified by name.
//
//  This call will block until the scan is complete - callbacks are made
//  within the scope of this call.
func (this *Client) ScanNode(policy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (chan *Record, error) {
	// results channel must be async for performance
	recChan := make(chan *Record, 1024)

	go this.scanNode(policy, node, namespace, setName, recChan, binNames...)

	return recChan, nil
}

func (this *Client) scanNode(policy *ScanPolicy, node *Node, namespace string, setName string, recChan chan *Record, binNames ...string) error {
	if policy == nil {
		policy = NewScanPolicy()
	}

	// Retry policy must be one-shot for scans.
	policy.MaxRetries = 0

	command := NewScanCommand(node, policy, namespace, setName, binNames, recChan)
	return command.Execute()
}

//-------------------------------------------------------------------
// Large collection functions (Supported by Aerospike 3 servers only)
//-------------------------------------------------------------------

//  Initialize large list operator.  This operator can be used to create and manage a list
//  within a single bin.
//
//  This method is only supported by Aerospike 3 servers.
// func (this *Client) GetLargeList(policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
// 	return NewLargeList(this, policy, key, binName, userModule)
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

//---------------------------------------------------------------
// User defined functions (Supported by Aerospike 3 servers only)
//---------------------------------------------------------------

//  Register package containing user defined functions with server.
//  This asynchronous server call will return before command is complete.
//  The user can optionally wait for command completion by using the returned
//  RegisterTask instance.
//
//  This method is only supported by Aerospike 3 servers.
func (this *Client) Register(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, error) {
	content, err := utils.ReadFileEncodeBase64(clientPath)
	if err != nil {
		return nil, err
	}

	var strCmd bytes.Buffer
	strCmd.WriteString("udf-put:filename=")
	strCmd.WriteString(serverPath)
	strCmd.WriteString(";content=")
	strCmd.WriteString(content)
	strCmd.WriteString(";content-len=")
	strCmd.WriteString(strconv.Itoa(len(content)))
	strCmd.WriteString(";udf-type=")
	strCmd.WriteString(string(language))
	strCmd.WriteString(";")

	// Send UDF to one node. That node will distribute the UDF to other nodes.
	node, err := this.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(0)
	if policy == nil {
		timeout = policy.Timeout
	}
	conn, err := node.GetConnection(timeout)
	if err != nil {
		return nil, err
	}

	//
	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		conn.Close()
		return nil, err
	}

	var response string
	for _, v := range responseMap {
		response = v
	}

	res := make(map[string]string)
	vals := strings.Split(response, ";")
	for _, pair := range vals {
		t := strings.SplitN(pair, "=", 2)
		res[t[0]] = t[1]
	}

	if _, exists := res["error"]; exists {
		return nil, NewAerospikeError(COMMAND_REJECTED, "Registration failed: %s\nFile: %s\nLine: %s\nMessage: %s",
			res["error"], res["file"], res["line"], res["message"])
	}

	node.PutConnection(conn)
	return NewRegisterTask(this.cluster, serverPath), nil
}

//  Execute user defined function on server and return results.
//  The function operates on a single record.
//  The package name is used to locate the udf file location:
//
//  udf file = <server udf dir>/<package name>.lua
//
//  This method is only supported by Aerospike 3 servers.
func (this *Client) Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (interface{}, error) {
	if policy == nil {
		policy = NewWritePolicy(0, 0)
	}
	command := NewExecuteCommand(this.cluster, policy, key, packageName, functionName, args)
	command.Execute()

	record := command.GetRecord()
	// fmt.Printf("%v\n", record)

	if record == nil || len(record.Bins) == 0 {
		return nil, nil
	}

	resultMap := record.Bins

	// User defined functions don't have to return a value.
	if exists, obj := mapContainsKeyPartial(resultMap, "SUCCESS"); exists {
		return obj, nil
	}

	if _, obj := mapContainsKeyPartial(resultMap, "FAILURE"); obj != nil {
		return nil, errors.New(fmt.Sprintf("%v", obj))
	}

	return nil, errors.New("Invalid UDF return value")
}

func mapContainsKeyPartial(theMap map[string]interface{}, key string) (bool, interface{}) {
	for k, v := range theMap {
		if strings.Index(k, key) >= 0 {
			return true, v
		}
	}
	return false, nil
}

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

//  Create secondary index.
//  This asynchronous server call will return before command is complete.
//  The user can optionally wait for command completion by using the returned
//  IndexTask instance.
//  <p>
//  This method is only supported by Aerospike 3 servers.
func (this *Client) CreateIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
) (*IndexTask, error) {
	var strCmd bytes.Buffer
	strCmd.WriteString("sindex-create:ns=")
	strCmd.WriteString(namespace)

	if len(setName) > 0 {
		strCmd.WriteString(";set=")
		strCmd.WriteString(setName)
	}

	strCmd.WriteString(";indexname=")
	strCmd.WriteString(indexName)
	strCmd.WriteString(";numbins=1")
	strCmd.WriteString(";indexdata=")
	strCmd.WriteString(binName)
	strCmd.WriteString(",")
	strCmd.WriteString(string(indexType))
	strCmd.WriteString(";priority=normal")

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := this.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := ""
	for _, v := range responseMap {
		response = v
	}

	if strings.ToUpper(response) == "OK" {
		// Return task that could optionally be polled for completion.
		return NewIndexTask(this.cluster, namespace, indexName), nil
	}

	if strings.HasPrefix(response, "FAIL:200") {
		// Index has already been created.  Do not need to poll for completion.
		return nil, NewAerospikeError(INDEX_FOUND)
	}

	return nil, errors.New("Create index failed: " + response)
}

//  Delete secondary index.
//  This method is only supported by Aerospike 3 servers.
func (this *Client) DropIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
) error {
	var strCmd bytes.Buffer
	strCmd.WriteString("sindex-delete:ns=")
	strCmd.WriteString(namespace)

	if len(setName) > 0 {
		strCmd.WriteString(";set=")
		strCmd.WriteString(setName)
	}
	strCmd.WriteString(";indexname=")
	strCmd.WriteString(indexName)

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := this.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return err
	}

	response := ""
	for _, v := range responseMap {
		response = v
	}

	if strings.ToUpper(response) == "OK" {
		return nil
	}

	if strings.HasPrefix(response, "FAIL:201") {
		// Index did not previously exist. Return without error.
		return nil
	}

	return errors.New("Drop index failed: " + response)
}

//-------------------------------------------------------
// Internal Methods
//-------------------------------------------------------
// func (this *Client) binNamesToHashSet(binNames []string) BinMap {
// }

func (this *Client) sendInfoCommand(policy *WritePolicy, command string) (map[string]string, error) {
	node, err := this.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(0)
	if policy != nil {
		timeout = policy.Timeout
	}

	conn, err := node.GetConnection(timeout)
	if err != nil {
		return nil, err
	}

	info, err := NewInfo(conn, command)
	if err != nil {
		conn.Close()
		return nil, err
	}
	node.PutConnection(conn)

	results, err := info.parseMultiResponse()
	if err != nil {
		return nil, err
	}

	return results, nil
}

//-------------------------------------------------------
// Utility Functions
//-------------------------------------------------------
// batchExecute Uses sync.WaitGroup to run commands using multiple goroutines,
// and waits for their return
func (this *Client) batchExecute(keys []*Key, cmdGen func(node *Node, bns *batchNamespace) Command) error {

	batchNodes, err := NewBatchNodeList(this.cluster, keys)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Use a goroutine per namespace per node
	for _, batchNode := range batchNodes {
		for _, batchNamespace := range batchNode.BatchNamespaces {
			wg.Add(1)
			go func() {
				defer wg.Done()
				command := cmdGen(batchNode.Node, batchNamespace)
				command.Execute()
			}()
		}
	}

	wg.Wait()
	return nil
}

func (this *Client) mergeResultChannels(cs ...chan *Record) chan *Record {
	var wg sync.WaitGroup
	out := make(chan *Record, 1024)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c chan *Record) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

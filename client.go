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
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/aerospike/aerospike-client-go/types"
)

// Client encapsulates an Aerospike cluster.
// All database operations are available against this object.
type Client struct {
	cluster *Cluster

	// DefaultPolicy is used for all read commands without a specific policy.
	DefaultPolicy *BasePolicy
	// DefaultWritePolicy is used for all write commands without a specific policy.
	DefaultWritePolicy *WritePolicy
	// DefaultScanPolicy is used for all quey commands without a specific policy.
	DefaultScanPolicy *ScanPolicy
	// DefaultQueryPolicy is used for all scan commands without a specific policy.
	DefaultQueryPolicy *QueryPolicy
}

//-------------------------------------------------------
// Constructors
//-------------------------------------------------------

// NewClient generates a new Client instance.
func NewClient(hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(NewClientPolicy(), NewHost(hostname, port))
}

// NewClientWithPolicy generates a new Client using the specified ClientPolicy.
// If the policy is nil, a default policy will be generated.
func NewClientWithPolicy(policy *ClientPolicy, hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
}

// NewClientWithPolicyAndHost generates a new Client the specified ClientPolicy and
// sets up the cluster using the provided hosts.
// If the policy is nil, a default policy will be generated.
func NewClientWithPolicyAndHost(policy *ClientPolicy, hosts ...*Host) (*Client, error) {
	if policy == nil {
		policy = NewClientPolicy()
	}

	cluster, err := NewCluster(policy, hosts)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to host(s): %v", hosts)
	}
	return &Client{
		cluster:            cluster,
		DefaultPolicy:      NewPolicy(),
		DefaultWritePolicy: NewWritePolicy(0, 0),
		DefaultScanPolicy:  NewScanPolicy(),
		DefaultQueryPolicy: NewQueryPolicy(),
	}, nil

}

//-------------------------------------------------------
// Cluster Connection Management
//-------------------------------------------------------

// Close closes all client connections to database server nodes.
func (clnt *Client) Close() {
	clnt.cluster.Close()
}

// IsConnected determines if the client is ready to talk to the database server cluster.
func (clnt *Client) IsConnected() bool {
	return clnt.cluster.IsConnected()
}

// GetNodes returns an array of active server nodes in the cluster.
func (clnt *Client) GetNodes() []*Node {
	return clnt.cluster.GetNodes()
}

// GetNodeNames returns a list of active server node names in the cluster.
func (clnt *Client) GetNodeNames() []string {
	nodes := clnt.cluster.GetNodes()
	names := make([]string, 0, len(nodes))

	for _, node := range nodes {
		names = append(names, node.GetName())
	}
	return names
}

//-------------------------------------------------------
// Write Record Operations
//-------------------------------------------------------

// Put writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Put(policy *WritePolicy, key *Key, bins BinMap) error {
	return clnt.PutBins(policy, key, binMapToBins(bins)...)
}

// PutBins writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This method avoids using the BinMap allocation and iteration and is lighter on GC.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) PutBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newWriteCommand(clnt.cluster, policy, key, bins, WRITE)
	return command.Execute()
}

//-------------------------------------------------------
// Operations string
//-------------------------------------------------------

// Append appends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for string and []byte values.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Append(policy *WritePolicy, key *Key, bins BinMap) error {
	return clnt.AppendBins(policy, key, binMapToBins(bins)...)
}

// AppendBins works the same as Append, but avoids BinMap allocation and iteration.
func (clnt *Client) AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newWriteCommand(clnt.cluster, policy, key, bins, APPEND)
	return command.Execute()
}

// Prepend prepends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call works only for string and []byte values.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Prepend(policy *WritePolicy, key *Key, bins BinMap) error {
	return clnt.PrependBins(policy, key, binMapToBins(bins)...)
}

// PrependBins works the same as Prepend, but avoids BinMap allocation and iteration.
func (clnt *Client) PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newWriteCommand(clnt.cluster, policy, key, bins, PREPEND)
	return command.Execute()
}

//-------------------------------------------------------
// Arithmetic Operations
//-------------------------------------------------------

// Add adds integer bin values to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for integer values.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Add(policy *WritePolicy, key *Key, bins BinMap) error {
	return clnt.AddBins(policy, key, binMapToBins(bins)...)
}

// AddBins works the same as Add, but avoids BinMap allocation and iteration.
func (clnt *Client) AddBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newWriteCommand(clnt.cluster, policy, key, bins, ADD)
	return command.Execute()
}

//-------------------------------------------------------
// Delete Operations
//-------------------------------------------------------

// Delete deletes a record for specified key.
// The policy specifies the transaction timeout.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Delete(policy *WritePolicy, key *Key) (bool, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newDeleteCommand(clnt.cluster, policy, key)
	err := command.Execute()
	return command.Existed(), err
}

//-------------------------------------------------------
// Touch Operations
//-------------------------------------------------------

// Touch updates a record's metadata.
// If the record exists, the record's TTL will be reset to the
// policy's expiration.
// If the record doesn't exist, it will return an error.
func (clnt *Client) Touch(policy *WritePolicy, key *Key) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newTouchCommand(clnt.cluster, policy, key)
	return command.Execute()
}

//-------------------------------------------------------
// Existence-Check Operations
//-------------------------------------------------------

// Exists determine if a record key exists.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Exists(policy *BasePolicy, key *Key) (bool, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}
	command := newExistsCommand(clnt.cluster, policy, key)
	err := command.Execute()
	return command.Exists(), err
}

// BatchExists determines if multiple record keys exist in one batch request.
// The returned array bool is in positional order with the original key array order.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) BatchExists(policy *BasePolicy, keys []*Key) ([]bool, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be marked true
	existsArray := make([]bool, len(keys))

	keyMap := newBatchItemList(keys)

	if err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandExists(node, bns, policy, keyMap, existsArray)
	}); err != nil {
		return nil, err
	}

	return existsArray, nil
}

//-------------------------------------------------------
// Read Record Operations
//-------------------------------------------------------

// Get reads a record header and bins for specified key.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}
	command := newReadCommand(clnt.cluster, policy, key, binNames)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

// GetHeader reads a record generation and expiration only for specified key.
// Bins are not read.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) GetHeader(policy *BasePolicy, key *Key) (*Record, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}
	command := newReadHeaderCommand(clnt.cluster, policy, key)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//-------------------------------------------------------
// Batch Read Operations
//-------------------------------------------------------

// BatchGet reads multiple record headers and bins for specified keys in one batch request.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) BatchGet(policy *BasePolicy, keys []*Key, binNames ...string) ([]*Record, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	keyMap := newBatchItemList(keys)
	binSet := map[string]struct{}{}
	for idx := range binNames {
		binSet[binNames[idx]] = struct{}{}
	}

	err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandGet(node, bns, policy, keyMap, binSet, records, _INFO1_READ)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

// BatchGetHeader reads multiple record header data for specified keys in one batch request.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
// The policy can be used to specify timeouts.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) BatchGetHeader(policy *BasePolicy, keys []*Key) ([]*Record, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}

	// same array can be used without sychronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	keyMap := newBatchItemList(keys)
	err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandGet(node, bns, policy, keyMap, nil, records, _INFO1_READ|_INFO1_NOBINDATA)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

//-------------------------------------------------------
// Generic Database Operations
//-------------------------------------------------------

// Operate performs multiple read/write operations on a single key in one batch request.
// An example would be to add an integer value to an existing record and then
// read the result, all in one database call.
//
// Write operations are always performed first, regardless of operation order
// relative to read operations.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newOperateCommand(clnt.cluster, policy, key, operations)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//-------------------------------------------------------
// Scan Operations
//-------------------------------------------------------

// ScanAll reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) ScanAll(policy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, error) {
	if policy == nil {
		if clnt.DefaultScanPolicy != nil {
			policy = clnt.DefaultScanPolicy
		} else {
			policy = NewScanPolicy()
		}
	}

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
	res := NewRecordset(policy.RecordQueueSize)

	// the whole call should be wrapped in a goroutine
	if policy.ConcurrentNodes {
		// results channel must be async for performance
		recChans := []chan *Record{}
		errChans := []chan error{}
		recCmds := []multiCommand{}
		for _, node := range nodes {
			res, err := clnt.ScanNode(policy, node, namespace, setName, binNames...)
			if err != nil {
				return nil, err
			}
			recChans = append(recChans, res.Records)
			errChans = append(errChans, res.Errors)
			recCmds = append(recCmds, res.commands...)
		}

		res.chans = recChans
		res.errs = errChans
		res.commands = recCmds
		res.Records, res.Errors = clnt.mergeResultChannels(policy.RecordQueueSize, recChans, errChans)
	} else {
		// drain nodes one by one
		go func() {
			defer close(res.Records)
			defer close(res.Errors)

			for _, node := range nodes {
				if recSet, err := clnt.ScanNode(policy, node, namespace, setName, binNames...); err != nil {
					res.Errors <- err
					continue
				} else {
					// Here be concurrent dragons
					// Don't wait for err channels to close; only record chans
				L:
					for {
						select {
						case err := <-recSet.Errors:
							res.drainRecords(recSet.Records)
							res.Errors <- err

							// this break will move on to the next node
							break L
						case rec, open := <-recSet.Records:
							if open && rec != nil {
								res.Records <- rec
							} else if !open {
								// channel has been closed
								res.drainErrors(recSet.Errors)

								// this break will move on to the next node
								break L
							}
						}
					}
				}
			}
		}()
	}

	return res, nil
}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) ScanNode(policy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, error) {
	if policy == nil {
		if clnt.DefaultScanPolicy != nil {
			policy = clnt.DefaultScanPolicy
		} else {
			policy = NewScanPolicy()
		}
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until migrations on node are finished
		if err := node.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	res := NewRecordset(policy.RecordQueueSize)

	// Retry policy must be one-shot for scans.
	// copy on write for policy
	newPolicy := *policy

	command := newScanCommand(node, &newPolicy, namespace, setName, binNames, res.Records, res.Errors)
	res.commands = append(res.commands, command)
	go command.Execute()

	return res, nil
}

//-------------------------------------------------------------------
// Large collection functions (Supported by Aerospike 3 servers only)
//-------------------------------------------------------------------

// GetLargeList initializes large list operator.
// This operator can be used to create and manage a list
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) GetLargeList(policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	return NewLargeList(clnt, policy, key, binName, userModule)
}

// GetLargeMap initializes a large map operator.
// This operator can be used to create and manage a map
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) GetLargeMap(policy *WritePolicy, key *Key, binName string, userModule string) *LargeMap {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	return NewLargeMap(clnt, policy, key, binName, userModule)
}

// GetLargeSet initializes large set operator.
// This operator can be used to create and manage a set
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) GetLargeSet(policy *WritePolicy, key *Key, binName string, userModule string) *LargeSet {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	return NewLargeSet(clnt, policy, key, binName, userModule)
}

// GetLargeStack initializes large stack operator.
// This operator can be used to create and manage a stack
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) GetLargeStack(policy *WritePolicy, key *Key, binName string, userModule string) *LargeStack {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	return NewLargeStack(clnt, policy, key, binName, userModule)
}

//---------------------------------------------------------------
// User defined functions (Supported by Aerospike 3 servers only)
//---------------------------------------------------------------

// RegisterUDFFromFile reads a file from file system and registers
// the containing a package user defined functions with the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	udfBody, err := ioutil.ReadFile(clientPath)
	if err != nil {
		return nil, err
	}

	return clnt.RegisterUDF(policy, udfBody, serverPath, language)
}

// RegisterUDF registers a package containing user defined functions with server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	content := base64.StdEncoding.EncodeToString(udfBody)

	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-put:filename=")
	_, err = strCmd.WriteString(serverPath)
	_, err = strCmd.WriteString(";content=")
	_, err = strCmd.WriteString(content)
	_, err = strCmd.WriteString(";content-len=")
	_, err = strCmd.WriteString(strconv.Itoa(len(content)))
	_, err = strCmd.WriteString(";udf-type=")
	_, err = strCmd.WriteString(string(language))
	_, err = strCmd.WriteString(";")

	// Send UDF to one node. That node will distribute the UDF to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(clnt.cluster.connectionTimeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		conn.Close()
		return nil, err
	}

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	res := make(map[string]string)
	vals := strings.Split(response, ";")
	for _, pair := range vals {
		t := strings.SplitN(pair, "=", 2)
		if len(t) == 2 {
			res[t[0]] = t[1]
		} else if len(t) == 1 {
			res[t[0]] = ""
		}
	}

	if _, exists := res["error"]; exists {
		return nil, NewAerospikeError(COMMAND_REJECTED, fmt.Sprintf("Registration failed: %s\nFile: %s\nLine: %s\nMessage: %s",
			res["error"], res["file"], res["line"], res["message"]))
	}

	node.PutConnection(conn)
	return NewRegisterTask(clnt.cluster, serverPath), nil
}

// RemoveUDF removes a package containing user defined functions in the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RemoveTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) RemoveUDF(policy *WritePolicy, udfName string) (*RemoveTask, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-remove:filename=")
	_, err = strCmd.WriteString(udfName)
	_, err = strCmd.WriteString(";")

	// Send command to one node. That node will distribute it to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(clnt.cluster.connectionTimeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		conn.Close()
		return nil, err
	}

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	if response == "ok" {
		return NewRemoveTask(clnt.cluster, udfName), nil
	}
	return nil, NewAerospikeError(SERVER_ERROR, response)
}

// ListUDF lists all packages containing user defined functions in the server.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) ListUDF(policy *BasePolicy) ([]*UDF, error) {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			policy = clnt.DefaultPolicy
		} else {
			policy = NewPolicy()
		}
	}
	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-list")

	// Send command to one node. That node will distribute it to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(clnt.cluster.connectionTimeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		conn.Close()
		return nil, err
	}

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	vals := strings.Split(response, ";")
	res := make([]*UDF, 0, len(vals))

	for _, udfInfo := range vals {
		if strings.Trim(udfInfo, " ") == "" {
			continue
		}
		udfParts := strings.Split(udfInfo, ",")

		udf := &UDF{}
		for _, values := range udfParts {
			valueParts := strings.Split(values, "=")
			if len(valueParts) == 2 {
				switch valueParts[0] {
				case "filename":
					udf.Filename = valueParts[1]
				case "hash":
					udf.Hash = valueParts[1]
				case "type":
					udf.Language = Language(valueParts[1])
				}
			}
		}
		res = append(res, udf)
	}

	node.PutConnection(conn)

	return res, nil
}

// Execute executes a user defined function on server and return results.
// The function operates on a single record.
// The package name is used to locate the udf file location:
//
// udf file = <server udf dir>/<package name>.lua
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (interface{}, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	command := newExecuteCommand(clnt.cluster, policy, key, packageName, functionName, args)
	if err := command.Execute(); err != nil {
		return nil, err
	}

	record := command.GetRecord()

	if record == nil || len(record.Bins) == 0 {
		return nil, nil
	}

	resultMap := record.Bins

	// User defined functions don't have to return a value.
	if exists, obj := mapContainsKeyPartial(resultMap, "SUCCESS"); exists {
		return obj, nil
	}

	if _, obj := mapContainsKeyPartial(resultMap, "FAILURE"); obj != nil {
		return nil, fmt.Errorf("%v", obj)
	}

	return nil, NewAerospikeError(UDF_BAD_RESPONSE, "Invalid UDF return value")
}

func mapContainsKeyPartial(theMap map[string]interface{}, key string) (bool, interface{}) {
	for k, v := range theMap {
		if strings.Index(k, key) >= 0 {
			return true, v
		}
	}
	return false, nil
}

//----------------------------------------------------------
// Query/Execute UDF (Supported by Aerospike 3 servers only)
//----------------------------------------------------------

// ExecuteUDF applies user defined function on records that match the statement filter.
// Records are not returned to the client.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) ExecuteUDF(policy *QueryPolicy,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, error) {
	if policy == nil {
		if clnt.DefaultQueryPolicy != nil {
			policy = clnt.DefaultQueryPolicy
		} else {
			policy = NewQueryPolicy()
		}
	}

	// Always set a taskId
	if statement.TaskId == 0 {
		statement.TaskId = time.Now().UnixNano()
	}

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "ExecuteUDF failed because cluster is empty.")
	}

	// wait until all migrations are finished
	if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
		return nil, err
	}

	statement.SetAggregateFunction(packageName, functionName, functionArgs, false)

	errs := []error{}
	for i := range nodes {
		command := newServerCommand(nodes[i], policy, statement)
		if err := command.Execute(); err != nil {
			errs = append(errs, err)
		}
	}

	return NewExecuteTask(clnt.cluster, statement), mergeErrors(errs)
}

//--------------------------------------------------------
// Query functions (Supported by Aerospike 3 servers only)
//--------------------------------------------------------

// Query executes a query and returns a recordset.
// The query executor puts records on a channel from separate goroutines.
// The caller can concurrently pops records off the channel through the
// record channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) Query(policy *QueryPolicy, statement *Statement) (*Recordset, error) {
	if policy == nil {
		if clnt.DefaultQueryPolicy != nil {
			policy = clnt.DefaultQueryPolicy
		} else {
			policy = NewQueryPolicy()
		}
	}

	// Always set a taskId
	if statement.TaskId == 0 {
		statement.TaskId = time.Now().UnixNano()
	}

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
	recSet := NewRecordset(policy.RecordQueueSize)

	// results channel must be async for performance
	recChans := []chan *Record{}
	errChans := []chan error{}
	recCmds := []multiCommand{}
	for _, node := range nodes {
		recChan := make(chan *Record, policy.RecordQueueSize)
		errChan := make(chan error, policy.RecordQueueSize)

		// copy policies to avoid race conditions
		newPolicy := *policy
		command := newQueryRecordCommand(node, &newPolicy, statement, recChan, errChan)
		recCmds = append(recCmds, command)
		go command.Execute()

		recChans = append(recChans, recChan)
		errChans = append(errChans, errChan)
	}

	recSet.commands = recCmds
	recSet.chans = recChans
	recSet.errs = errChans
	recSet.Records, recSet.Errors = clnt.mergeResultChannels(policy.RecordQueueSize, recChans, errChans)

	return recSet, nil
}

// // Execute query, apply statement's aggregation function, and return result iterator. The query
// // executor puts results on a channel in separate goroutines.  The calling goroutine concurrently pops
// // results off the queue through the result iterator.
// //
// // The aggregation function is called on both server and client (reduce).  Therefore,
// // the Lua script files must also reside on both server and client.
// // The package name is used to locate the udf file location:
// // <p>
// // udf file = <udf dir>/<package name>.lua
// // <p>
// // This method is only supported by Aerospike 3 servers.
// func (clnt *Client) QueryAggregate(
// 	policy QueryPolicy,
// 	statement Statement,
// 	packageName string,
// 	functionName string,
// 	functionArgs ...Value,
// ) (ResultSet, error) {
// }

// CreateIndex creates a secondary index.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) CreateIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
) (*IndexTask, error) {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}

	var strCmd bytes.Buffer
	_, err := strCmd.WriteString("sindex-create:ns=")
	_, err = strCmd.WriteString(namespace)

	if len(setName) > 0 {
		_, err = strCmd.WriteString(";set=")
		_, err = strCmd.WriteString(setName)
	}

	_, err = strCmd.WriteString(";indexname=")
	_, err = strCmd.WriteString(indexName)
	_, err = strCmd.WriteString(";numbins=1")
	_, err = strCmd.WriteString(";indexdata=")
	_, err = strCmd.WriteString(binName)
	_, err = strCmd.WriteString(",")
	_, err = strCmd.WriteString(string(indexType))
	_, err = strCmd.WriteString(";priority=normal")

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := ""
	for _, v := range responseMap {
		response = v
	}

	if strings.ToUpper(response) == "OK" {
		// Return task that could optionally be polled for completion.
		return NewIndexTask(clnt.cluster, namespace, indexName), nil
	}

	if strings.HasPrefix(response, "FAIL:200") {
		// Index has already been created.  Do not need to poll for completion.
		return nil, NewAerospikeError(INDEX_FOUND)
	}

	return nil, NewAerospikeError(INDEX_GENERIC, "Create index failed: "+response)
}

// DropIndex deletes a secondary index.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, a default policy will be generated.
func (clnt *Client) DropIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
) error {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			policy = clnt.DefaultWritePolicy
		} else {
			policy = NewWritePolicy(0, 0)
		}
	}
	var strCmd bytes.Buffer
	_, err := strCmd.WriteString("sindex-delete:ns=")
	_, err = strCmd.WriteString(namespace)

	if len(setName) > 0 {
		_, err = strCmd.WriteString(";set=")
		_, err = strCmd.WriteString(setName)
	}
	_, err = strCmd.WriteString(";indexname=")
	_, err = strCmd.WriteString(indexName)

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return err
	}

	response := ""
	for _, v := range responseMap {
		response = v

		if strings.ToUpper(response) == "OK" {
			return nil
		}

		if strings.HasPrefix(response, "FAIL:201") {
			// Index did not previously exist. Return without error.
			return nil
		}
	}

	return NewAerospikeError(INDEX_GENERIC, "Drop index failed: "+response)
}

//-------------------------------------------------------
// Internal Methods
//-------------------------------------------------------

func (clnt *Client) sendInfoCommand(policy *WritePolicy, command string) (map[string]string, error) {
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(policy.Timeout)
	if err != nil {
		return nil, err
	}

	info, err := newInfo(conn, command)
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

// mergeErrors merges several errors into one
func mergeErrors(errs []error) error {
	if errs == nil || len(errs) == 0 {
		return nil
	}
	var msg bytes.Buffer
	for _, err := range errs {
		msg.WriteString(err.Error() + "\n")
	}
	return errors.New(msg.String())
}

// batchExecute Uses sync.WaitGroup to run commands using multiple goroutines,
// and waits for their return
func (clnt *Client) batchExecute(keys []*Key, cmdGen func(node *Node, bns *batchNamespace) command) error {

	batchNodes, err := newBatchNodeList(clnt.cluster, keys)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Use a goroutine per namespace per node
	errs := []error{}
	wg.Add(len(batchNodes))
	for _, batchNode := range batchNodes {
		// copy to avoid race condition
		bn := *batchNode
		for _, bns := range bn.BatchNamespaces {
			go func(bn *Node, bns *batchNamespace) {
				defer wg.Done()
				command := cmdGen(bn, bns)
				if err := command.Execute(); err != nil {
					errs = append(errs, err)
				}
			}(bn.Node, bns)
		}
	}

	wg.Wait()
	return mergeErrors(errs)
}

func (clnt *Client) mergeResultChannels(size int, channels []chan *Record, errors []chan error) (chan *Record, chan error) {
	var wg sync.WaitGroup
	out := make(chan *Record, size)
	outErr := make(chan error, size)

	// Start an output goroutine for each input channel in channels.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	outputRecs := func(c chan *Record) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}

	outputErrors := func(c chan error) {
		for n := range c {
			outErr <- n
		}
		wg.Done()
	}

	// wait for both record and channels to close
	// otherwise we may leak goroutines and we won't know
	wg.Add(len(channels) + len(errors))
	for _, c := range channels {
		go outputRecs(c)
	}

	for _, c := range errors {
		go outputErrors(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
		close(outErr)
	}()
	return out, outErr
}

// internal random number generator instance
var rnd *rand.Rand

func init() {
	// seed the random number generator
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/aerospike/aerospike-client-go/v8/config/provider"
	internal "github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const unreachable = "UNREACHABLE"

// AEROSPIKE_CLIENT_CONFIG_URL is the environment variable that can be set to
// load the Aerospike client configuration from a URL.
var AEROSPIKE_CLIENT_CONFIG_URL = os.Getenv("AEROSPIKE_CLIENT_CONFIG_URL")

// Client encapsulates an Aerospike cluster.
// All database operations are available against this object.
type Client struct {
	cluster *Cluster

	// Dynamic configuration
	dynConfig *DynConfig

	// DefaultPolicy is used for all read commands without a specific policy.
	DefaultPolicy *BasePolicy
	// DefaultBatchPolicy is the default parent policy used in batch read commands. Base policy fields
	// include socketTimeout, totalTimeout, maxRetries, etc...
	DefaultBatchPolicy *BatchPolicy
	// DefaultBatchReadPolicy is the default read policy used in batch operate commands.
	DefaultBatchReadPolicy *BatchReadPolicy
	// DefaultBatchWritePolicy is the default write policy used in batch operate commands.
	// Write policy fields include generation, expiration, durableDelete, etc...
	DefaultBatchWritePolicy *BatchWritePolicy
	// DefaultBatchDeletePolicy is the default delete policy used in batch delete commands.
	DefaultBatchDeletePolicy *BatchDeletePolicy
	// DefaultBatchUDFPolicy is the default user defined function policy used in batch UDF execute commands.
	DefaultBatchUDFPolicy *BatchUDFPolicy
	// DefaultWritePolicy is used for all write commands without a specific policy.
	DefaultWritePolicy *WritePolicy
	// DefaultScanPolicy is used for all scan commands without a specific policy.
	DefaultScanPolicy *ScanPolicy
	// DefaultQueryPolicy is used for all query commands without a specific policy.
	DefaultQueryPolicy *QueryPolicy
	// DefaultAdminPolicy is used for all security commands without a specific policy.
	DefaultAdminPolicy *AdminPolicy
	// DefaultInfoPolicy is used for all info commands without a specific policy.
	DefaultInfoPolicy *InfoPolicy
	// Default transaction policy when verifying record versions in a batch on a commit.
	DefaultTxnVerifyPolicy *TxnVerifyPolicy
	// Default transaction policy when rolling the transaction records forward (commit)
	// or back (abort) in a batch.
	DefaultTxnRollPolicy *TxnRollPolicy

	// Policies used for dynamic configuration updates.
	// ClientPolicy is used to update the client configuration.
	dynDefaultClientPolicy *atomic.Pointer[ClientPolicy]
	// DefaultPolicy is used for all read commands without a specific policy.
	dynDefaultPolicy atomic.Pointer[BasePolicy]
	// DynamicScanPolicy is used for all scan commands without a specific policy.
	dynDefaultScanPolicy atomic.Pointer[ScanPolicy]
	// DynamicQueryPolicy is used for all query commands without a specific policy.
	dynDefaultQueryPolicy atomic.Pointer[QueryPolicy]
	// DynamicBatchPolicy is the default parent policy used in batch read commands. Base policy fields]
	// include socketTimeout, totalTimeout, maxRetries, etc...
	dynDefaultBatchPolicy atomic.Pointer[BatchPolicy]
	// Write policy fields include generation, expiration, durableDelete, etc...
	dynDefaultBatchWritePolicy atomic.Pointer[BatchWritePolicy]
	// include socketTimeout, totalTimeout, maxRetries, etc...
	// DynamicBatchReadPolicy is the default read policy used in batch operate commands.
	dynDefaultBatchReadPolicy atomic.Pointer[BatchReadPolicy]
	// DynamicBatchDeletePolicy is the default delete policy used in batch delete commands.
	dynDefaultBatchDeletePolicy atomic.Pointer[BatchDeletePolicy]
	// DynamicBatchUDFPolicy is the default user defined function policy used in batch UDF execute commands.
	dynDefaultBatchUDFPolicy atomic.Pointer[BatchUDFPolicy]
	// DynamicWritePolicy is used for all write commands without a specific policy.
	dynDefaultWritePolicy atomic.Pointer[WritePolicy]
	// Dynamic transaction policy when verifying record versions in a batch on a commit.
	dynDefaultTxnVerifyPolicy atomic.Pointer[TxnVerifyPolicy]
	// Dynamic transaction policy when rolling the transaction records forward (commit)
	// or back (abort) in a batch.
	dynDefaultTxnRollPolicy atomic.Pointer[TxnRollPolicy]
	// DynamicMetricsPolicy is used for all metrics commands without a specific policy.
	dynDefaultMetricsPolicy atomic.Pointer[MetricsPolicy]
	// DynamicBasePolicy is used for all commands without a specific policy when running batch operations.
	dynDefaultBatchReadBasePolicy atomic.Pointer[BasePolicy]
	// DynamicBasePolicy is used for all commands without a specific policy when running batch operations.
	dynDefaultBatchWriteBasePolicy atomic.Pointer[BasePolicy]
}

func clientFinalizer(f *Client) {
	f.Close()
}

//-------------------------------------------------------
// Constructors
//-------------------------------------------------------

// NewClient generates a new Client instance.
// The connection pool after connecting to the database is initially empty,
// and connections are established on a per need basis, which can be slow and
// time out some initial commands.
// It is recommended to call the client.WarmUp() method right after connecting to the database
// to fill up the connection pool to the required service level.
func NewClient(hostname string, port int) (*Client, Error) {
	return NewClientWithPolicyAndHost(NewClientPolicy(), NewHost(hostname, port))
}

// NewClientWithPolicy generates a new Client using the specified ClientPolicy.
// If the policy is nil, the default relevant policy will be used.
// The connection pool after connecting to the database is initially empty,
// and connections are established on a per need basis, which can be slow and
// time out some initial commands.
// It is recommended to call the client.WarmUp() method right after connecting to the database
// to fill up the connection pool to the required service level.
func NewClientWithPolicy(policy *ClientPolicy, hostname string, port int) (*Client, Error) {
	return NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
}

// NewClientWithPolicyAndHost generates a new Client with the specified ClientPolicy and
// sets up the cluster using the provided hosts.
// If the policy is nil, the default relevant policy will be used.
// The connection pool after connecting to the database is initially empty,
// and connections are established on a per need basis, which can be slow and
// time out some initial commands.
// It is recommended to call the client.WarmUp() method right after connecting to the database
// to fill up the connection pool to the required service level.
func NewClientWithPolicyAndHost(policy *ClientPolicy, hosts ...*Host) (*Client, Error) {
	// Start dynamic configuration watcher
	dynConfig := newDynConfigWithCallBack(policy, metricsSyncCallBack)

	// Get updated client policy with dynamic configuration and store atomically
	// Need this since cluster needs to have updated client policy during creation.
	clientPolicy := &atomic.Pointer[ClientPolicy]{}
	clientPolicy.Store(getUsableClientPolicy(policy, dynConfig))

	cluster, err := NewCluster(clientPolicy, hosts)
	if err != nil && clientPolicy.Load().FailIfNotConnected {
		logger.Logger.Debug("Failed to connect to host(s): %v; error: %s", hosts, err)
		return nil, err
	}

	client := &Client{
		cluster:                  cluster,
		dynConfig:                dynConfig,
		DefaultPolicy:            NewPolicy(),
		DefaultBatchPolicy:       NewBatchPolicy(),
		DefaultBatchReadPolicy:   NewBatchReadPolicy(),
		DefaultBatchWritePolicy:  NewBatchWritePolicy(),
		DefaultBatchDeletePolicy: NewBatchDeletePolicy(),
		DefaultBatchUDFPolicy:    NewBatchUDFPolicy(),
		DefaultWritePolicy:       NewWritePolicy(0, 0),
		DefaultScanPolicy:        NewScanPolicy(),
		DefaultQueryPolicy:       NewQueryPolicy(),
		DefaultAdminPolicy:       NewAdminPolicy(),
		DefaultInfoPolicy:        NewInfoPolicy(),
		DefaultTxnVerifyPolicy:   NewTxnVerifyPolicy(),
		DefaultTxnRollPolicy:     NewTxnRollPolicy(),
	}

	if dynConfig != nil {
		// Running the callback function to load functionalities dependent on
		// the instance of client.
		dynConfig.client = client
		client.dynDefaultClientPolicy = clientPolicy
		dynConfig.updateCachedPolicies()
		dynConfig.runCallBack()
	}

	runtime.SetFinalizer(client, clientFinalizer)
	return client, err
}

//-------------------------------------------------------
// Policy methods
//-------------------------------------------------------

// GetDefaultPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultPolicy() *BasePolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultPolicy
	} else {
		response := *clnt.dynDefaultPolicy.Load()
		return &response
	}
}

// GetDefaultBatchPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultBatchPolicy() *BatchPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultBatchPolicy
	} else {
		response := *clnt.dynDefaultBatchPolicy.Load()
		return &response
	}
}

// GetDefaultBatchWritePolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultBatchWritePolicy() *BatchWritePolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultBatchWritePolicy
	} else {
		response := *clnt.dynDefaultBatchWritePolicy.Load()
		return &response
	}
}

// GetDefaultBatchReadPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultBatchReadPolicy() *BatchReadPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultBatchReadPolicy
	} else {
		response := *clnt.dynDefaultBatchReadPolicy.Load()
		return &response
	}
}

// GetDefaultBatchDeletePolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultBatchDeletePolicy() *BatchDeletePolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultBatchDeletePolicy
	} else {
		response := *clnt.dynDefaultBatchDeletePolicy.Load()
		return &response
	}
}

// GetDefaultBatchUDFPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultBatchUDFPolicy() *BatchUDFPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultBatchUDFPolicy
	} else {
		response := *clnt.dynDefaultBatchUDFPolicy.Load()
		return &response
	}
}

// GetDefaultWritePolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultWritePolicy() *WritePolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultWritePolicy
	} else {
		response := *clnt.dynDefaultWritePolicy.Load()
		return &response
	}
}

// GetDefaultScanPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultScanPolicy() *ScanPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultScanPolicy
	} else {
		response := *clnt.dynDefaultScanPolicy.Load()
		return &response
	}
}

// GetDefaultQueryPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultQueryPolicy() *QueryPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultQueryPolicy
	} else {
		response := *clnt.dynDefaultQueryPolicy.Load()
		return &response
	}
}

// GetDefaultAdminPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultAdminPolicy() *AdminPolicy {
	return clnt.DefaultAdminPolicy
}

// GetDefaultInfoPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultInfoPolicy() *InfoPolicy {
	return clnt.DefaultInfoPolicy
}

// GetDefaultTxnVerifyPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultTxnVerifyPolicy() *TxnVerifyPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultTxnVerifyPolicy
	} else {
		response := *clnt.dynDefaultTxnVerifyPolicy.Load()
		return &response
	}
}

// GetDefaultTxnRollPolicy returns corresponding default policy from the client
func (clnt *Client) GetDefaultTxnRollPolicy() *TxnRollPolicy {
	if clnt.dynConfig == nil {
		return clnt.DefaultTxnRollPolicy
	} else {
		response := *clnt.dynDefaultTxnRollPolicy.Load()
		return &response
	}
}

// SetDefaultPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultPolicy(policy *BasePolicy) {
	clnt.DefaultPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultBatchPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultBatchPolicy(policy *BatchPolicy) {
	clnt.DefaultBatchPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultBatchWritePolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultBatchWritePolicy(policy *BatchWritePolicy) {
	clnt.DefaultBatchWritePolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultBatchReadPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultBatchReadPolicy(policy *BatchReadPolicy) {
	clnt.DefaultBatchReadPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultBatchDeletePolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultBatchDeletePolicy(policy *BatchDeletePolicy) {
	clnt.DefaultBatchDeletePolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultBatchUDFPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultBatchUDFPolicy(policy *BatchUDFPolicy) {
	clnt.DefaultBatchUDFPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultWritePolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultWritePolicy(policy *WritePolicy) {
	clnt.DefaultWritePolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultScanPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultScanPolicy(policy *ScanPolicy) {
	clnt.DefaultScanPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultQueryPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultQueryPolicy(policy *QueryPolicy) {
	clnt.DefaultQueryPolicy = policy.pathDynamic(clnt.dynConfig)
}

// SetDefaultAdminPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultAdminPolicy(policy *AdminPolicy) {
	clnt.DefaultAdminPolicy = policy
}

// SetDefaultInfoPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultInfoPolicy(policy *InfoPolicy) {
	clnt.DefaultInfoPolicy = policy
}

// SetDefaultTxnVerifyPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultTxnVerifyPolicy(policy *TxnVerifyPolicy) {
	clnt.DefaultTxnVerifyPolicy = policy.patchDynamic(clnt.dynConfig)
}

// SetDefaultTxnRollPolicy sets corresponding default policy on the client
func (clnt *Client) SetDefaultTxnRollPolicy(policy *TxnRollPolicy) {
	clnt.DefaultTxnRollPolicy = policy.patchDynamic(clnt.dynConfig)
}

//-------------------------------------------------------
// Cluster Connection Management
//-------------------------------------------------------

// Close closes all client connections to database server nodes.
func (clnt *Client) Close() {
	clnt.cluster.Close()
	clnt.dynConfig.Close()
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

// PutPayload writes the raw write/delete payload to the server.
// The policy specifies the transaction timeout.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) PutPayload(policy *WritePolicy, key *Key, payload []byte) Error {
	policy = clnt.getUsableWritePolicy(policy)
	command, err := newWritePayloadCommand(clnt.cluster, policy, key, payload)
	if err != nil {
		return err
	}

	return command.Execute()
}

// Put writes record bin(s) to the server.
// The policy specifies the command timeout, record expiration and how the command is
// handled when the record already exists.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Put(policy *WritePolicy, key *Key, binMap BinMap) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, nil, binMap, _WRITE)
	if err != nil {
		return err
	}

	return command.Execute()
}

// PutBins writes record bin(s) to the server.
// The policy specifies the command timeout, record expiration and how the command is
// handled when the record already exists.
// This method avoids using the BinMap allocation and iteration and is lighter on GC.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) PutBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, bins, nil, _WRITE)
	if err != nil {
		return err
	}

	return command.Execute()
}

//-------------------------------------------------------
// Operations string
//-------------------------------------------------------

// Append appends bin value's string to existing record bin values.
// The policy specifies the command timeout, record expiration and how the command is
// handled when the record already exists.
// This call only works for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Append(policy *WritePolicy, key *Key, binMap BinMap) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, nil, binMap, _APPEND)
	if err != nil {
		return err
	}

	return command.Execute()
}

// AppendBins works the same as Append, but avoids BinMap allocation and iteration.
func (clnt *Client) AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, bins, nil, _APPEND)
	if err != nil {
		return err
	}

	return command.Execute()
}

// Prepend prepends bin value's string to existing record bin values.
// The policy specifies the command timeout, record expiration and how the command is
// handled when the record already exists.
// This call works only for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Prepend(policy *WritePolicy, key *Key, binMap BinMap) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, nil, binMap, _PREPEND)
	if err != nil {
		return err
	}

	return command.Execute()
}

// PrependBins works the same as Prepend, but avoids BinMap allocation and iteration.
func (clnt *Client) PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, bins, nil, _PREPEND)
	if err != nil {
		return err
	}

	return command.Execute()
}

//-------------------------------------------------------
// Arithmetic Operations
//-------------------------------------------------------

// Add adds integer bin values to existing record bin values.
// The policy specifies the command timeout, record expiration and how the command is
// handled when the record already exists.
// This call only works for integer values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Add(policy *WritePolicy, key *Key, binMap BinMap) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, nil, binMap, _ADD)
	if err != nil {
		return err
	}

	return command.Execute()
}

// AddBins works the same as Add, but avoids BinMap allocation and iteration.
func (clnt *Client) AddBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newWriteCommand(clnt.cluster, policy, key, bins, nil, _ADD)
	if err != nil {
		return err
	}

	return command.Execute()
}

//-------------------------------------------------------
// Delete Operations
//-------------------------------------------------------

// Delete deletes a record for specified key.
// The policy specifies the command timeout.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Delete(policy *WritePolicy, key *Key) (bool, Error) {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return false, err
		}
	}

	command, err := newDeleteCommand(clnt.cluster, policy, key)
	if err != nil {
		return false, err
	}

	err = command.Execute()
	return command.Existed(), err
}

//-------------------------------------------------------
// Touch Operations
//-------------------------------------------------------

// Touch updates a record's metadata.
// If the record exists, the record's TTL will be reset to the
// policy's expiration.
// If the record does not exist, it can't be created because the server deletes empty records.
// If the record doesn't exist, it will return an error.
func (clnt *Client) Touch(policy *WritePolicy, key *Key) Error {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return err
		}
	}

	command, err := newTouchCommand(clnt.cluster, policy, key)
	if err != nil {
		return err
	}

	return command.Execute()
}

//-------------------------------------------------------
// Existence-Check Operations
//-------------------------------------------------------

// Exists determine if a record key exists.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Exists(policy *BasePolicy, key *Key) (bool, Error) {
	policy = clnt.getUsablePolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareRead(key.namespace); err != nil {
			return false, err
		}
	}

	command, err := newExistsCommand(clnt.cluster, policy, key)
	if err != nil {
		return false, err
	}

	err = command.Execute()
	return command.Exists(), err
}

// BatchExists determines if multiple record keys exist in one batch request.
// The returned boolean array is in positional order with the original key array order.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchExists(policy *BatchPolicy, keys []*Key) ([]bool, Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareReadForKeys(keys); err != nil {
			return nil, err
		}
	}

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be marked true
	existsArray := make([]bool, len(keys))

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, nil, false)
	if err != nil {
		return nil, err
	}

	// pass nil to make sure it will be cloned and prepared
	cmd := newBatchCommandExists(clnt, nil, policy, keys, existsArray)
	filteredOut, err := clnt.batchExecute(policy, batchNodes, cmd)
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	if err != nil {
		return nil, err
	}

	return existsArray, err
}

//-------------------------------------------------------
// Read Record Operations
//-------------------------------------------------------

// Get reads a record header and bins for specified key.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, Error) {
	policy = clnt.getUsablePolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareRead(key.namespace); err != nil {
			return nil, err
		}
	}

	command, err := newReadCommand(clnt.cluster, policy, key, binNames)
	if err != nil {
		return nil, err
	}

	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

// GetHeader reads a record generation and expiration only for specified key.
// Bins are not read.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) GetHeader(policy *BasePolicy, key *Key) (*Record, Error) {
	policy = clnt.getUsablePolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareRead(key.namespace); err != nil {
			return nil, err
		}
	}

	command, err := newReadHeaderCommand(clnt.cluster, policy, key)
	if err != nil {
		return nil, err
	}

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
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchGet(policy *BatchPolicy, keys []*Key, binNames ...string) ([]*Record, Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareReadForKeys(keys); err != nil {
			return nil, err
		}
	}

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, nil, false)
	if err != nil {
		return nil, err
	}

	rattr := _INFO1_READ
	if len(binNames) == 0 {
		rattr = rattr | _INFO1_GET_ALL
	}

	cmd := newBatchCommandGet(clnt, nil, policy, keys, binNames, nil, records, rattr, false)
	filteredOut, err := clnt.batchExecute(policy, batchNodes, cmd)
	if err != nil && !policy.AllowPartialResults {
		return nil, err
	}

	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return records, err
}

// BatchGetOperate reads multiple records for specified keys using read operations in one batch call.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
//
// If a batch request to a node fails, the entire batch is cancelled.
func (clnt *Client) BatchGetOperate(policy *BatchPolicy, keys []*Key, ops ...*Operation) ([]*Record, Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareReadForKeys(keys); err != nil {
			return nil, err
		}
	}

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, nil, false)
	if err != nil {
		return nil, err
	}

	cmd := newBatchCommandGet(clnt, nil, policy, keys, nil, ops, records, _INFO1_READ, true)
	filteredOut, err := clnt.batchExecute(policy, batchNodes, cmd)
	if err != nil && !policy.AllowPartialResults {
		return nil, err
	}

	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return records, err
}

// BatchGetComplex reads multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins to be requested for each key in the batch.
// The returned records are located in the same list.
// If the BatchRead key field is not found, the corresponding record field will be nil.
// The policy can be used to specify timeouts and maximum concurrent goroutines.
// This method requires Aerospike Server version >= 3.6.0.
func (clnt *Client) BatchGetComplex(policy *BatchPolicy, records []*BatchRead) Error {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareBatchReads(records); err != nil {
			return err
		}
	}

	cmd := newBatchIndexCommandGet(clnt, nil, policy, records, true)

	batchNodes, err := newBatchIndexNodeList(clnt.cluster, policy, records)
	if err != nil {
		return err
	}

	filteredOut, err := clnt.batchExecute(policy, batchNodes, &cmd)
	if err != nil && !policy.AllowPartialResults {
		return err
	}

	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return err
}

// BatchGetHeader reads multiple record header data for specified keys in one batch request.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchGetHeader(policy *BatchPolicy, keys []*Key) ([]*Record, Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareReadForKeys(keys); err != nil {
			return nil, err
		}
	}

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, nil, false)
	if err != nil {
		return nil, err
	}

	cmd := newBatchCommandGet(clnt, nil, policy, keys, nil, nil, records, _INFO1_READ|_INFO1_NOBINDATA, false)
	filteredOut, err := clnt.batchExecute(policy, batchNodes, cmd)
	if err != nil && !policy.AllowPartialResults {
		return nil, err
	}

	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return records, err
}

// BatchDelete deletes records for specified keys. If a key is not found, the corresponding result
// BatchRecord.ResultCode will be types.KEY_NOT_FOUND_ERROR.
//
// Requires server version 6.0+
func (clnt *Client) BatchDelete(policy *BatchPolicy, deletePolicy *BatchDeletePolicy, keys []*Key) ([]*BatchRecord, Error) {
	policy = clnt.getUsableBatchPolicy(policy)
	deletePolicy = clnt.getUsableBatchDeletePolicy(deletePolicy)

	if policy.Txn != nil {
		if err := txnMonitor.addKeys(clnt.cluster, policy, keys); err != nil {
			return nil, err
		}
	}

	attr := &batchAttr{}
	attr.setBatchDelete(deletePolicy)

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*BatchRecord, len(keys))
	for i := range records {
		records[i] = newSimpleBatchRecord(keys[i], true)
	}

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, records, true)
	if err != nil {
		return nil, err
	}

	cmd := newBatchCommandDelete(clnt, nil, policy, deletePolicy, keys, records, attr)
	_, err = clnt.batchExecute(policy, batchNodes, cmd)
	return records, err
}

// BatchOperate will read/write multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins for each key in the batch.
// The returned records are located in the same list.
//
// BatchRecord can be *BatchRead, *BatchWrite, *BatchDelete or *BatchUDF.
//
// Requires server version 6.0+
func (clnt *Client) BatchOperate(policy *BatchPolicy, records []BatchRecordIfc) Error {
	policy = clnt.getUsableBatchPolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKeysFromRecords(clnt.cluster, policy, records); err != nil {
			return err
		}
	}

	batchNodes, err := newBatchOperateNodeListIfc(clnt.cluster, policy, records)
	if err != nil && policy.RespondAllKeys {
		return err
	}

	if len(batchNodes) == 0 {
		return newError(types.INVALID_NAMESPACE)
	}

	cmd := newBatchCommandOperate(clnt, nil, policy, records)
	_, err = clnt.batchExecute(policy, batchNodes, &cmd)
	return err
}

// BatchExecute will read/write multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins for each key in the batch.
// The returned records are located in the same list.
//
// BatchRecord can be *BatchRead, *BatchWrite, *BatchDelete or *BatchUDF.
//
// Requires server version 6.0+
func (clnt *Client) BatchExecute(policy *BatchPolicy, udfPolicy *BatchUDFPolicy, keys []*Key, packageName string, functionName string, args ...Value) ([]*BatchRecord, Error) {
	policy = clnt.getUsableBatchPolicy(policy)
	udfPolicy = clnt.getUsableBatchUDFPolicy(udfPolicy)

	if policy.Txn != nil {
		if err := txnMonitor.addKeys(clnt.cluster, policy, keys); err != nil {
			return nil, err
		}
	}

	attr := &batchAttr{}
	attr.setBatchUDF(udfPolicy)

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*BatchRecord, len(keys))
	for i := range records {
		records[i] = newSimpleBatchRecord(keys[i], attr.hasWrite)
	}

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, records, attr.hasWrite)
	if err != nil {
		return nil, err
	}

	cmd := newBatchCommandUDF(clnt, nil, policy, udfPolicy, keys, packageName, functionName, args, records, attr)
	_, err = clnt.batchExecute(policy, batchNodes, cmd)
	return records, err
}

//-------------------------------------------------------
// Generic Database Operations
//-------------------------------------------------------

// Operate performs multiple read/write operations on a single key in one batch request.
// An example would be to add an integer value to an existing record and then
// read the result, all in one database call.
//
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, Error) {
	// TODO: Remove this method in the next major release.
	policy = clnt.getUsableWritePolicy(policy)
	args, err := newOperateArgs(clnt.cluster, policy, key, operations)
	if err != nil {
		return nil, err
	}

	policy = args.writePolicy

	if args.hasWrite {
		if policy.Txn != nil {
			if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
				return nil, err
			}
		}

		command, err := newOperateCommandWrite(clnt.cluster, key, args)
		if err != nil {
			return nil, err
		}

		if err := command.Execute(); err != nil {
			return nil, err
		}
		return command.GetRecord(), nil
	} else {
		if policy.Txn != nil {
			if err := policy.Txn.prepareRead(key.namespace); err != nil {
				return nil, err
			}
		}

		command, err := newOperateCommandRead(clnt.cluster, key, args)
		if err != nil {
			return nil, err
		}

		if err := command.Execute(); err != nil {
			return nil, err
		}
		return command.GetRecord(), nil
	}
}

//-------------------------------------------------------
// Scan Operations
//-------------------------------------------------------

// ScanPartitions Read records in specified namespace, set and partition filter.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If partitionFilter is nil, all partitions will be scanned.
// If the policy is nil, the default relevant policy will be used.
// This method is only supported by Aerospike 4.9+ servers.
func (clnt *Client) ScanPartitions(apolicy *ScanPolicy, partitionFilter *PartitionFilter, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	var tracker *partitionTracker
	if partitionFilter == nil {
		tracker = newPartitionTrackerForNodes(&policy.MultiPolicy, nodes)
	} else {
		tracker = newPartitionTracker(&policy.MultiPolicy, partitionFilter, nodes)
	}

	// result recordset
	res := newRecordset(policy.RecordQueueSize, 1)
	go clnt.scanPartitions(&policy, tracker, namespace, setName, res, binNames...)

	return res, nil
}

// ScanAll reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanAll(apolicy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	return clnt.ScanPartitions(apolicy, nil, namespace, setName, binNames...)
}

// scanNodePartitions reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) scanNodePartitions(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	policy := *clnt.getUsableScanPolicy(apolicy)
	tracker := newPartitionTrackerForNode(&policy.MultiPolicy, node)

	// result recordset
	res := newRecordset(policy.RecordQueueSize, 1)
	go clnt.scanPartitions(&policy, tracker, namespace, setName, res, binNames...)

	return res, nil
}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanNode(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	return clnt.scanNodePartitions(apolicy, node, namespace, setName, binNames...)
}

//---------------------------------------------------------------
// User defined functions (Supported by Aerospike 3+ servers only)
//---------------------------------------------------------------

// RegisterUDFFromFile reads a file from file system and registers
// the containing a package user defined functions with the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	udfBody, err := os.ReadFile(clientPath)
	if err != nil {
		return nil, newCommonError(err)
	}

	return clnt.RegisterUDF(policy, udfBody, serverPath, language)
}

// RegisterUDF registers a package containing user defined functions with server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	content := base64.StdEncoding.EncodeToString(udfBody)

	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
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
	responseMap, err := clnt.sendInfoCommand(policy.TotalTimeout, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := responseMap[strCmd.String()]
	if strings.EqualFold(response, "ok") {
		return NewRegisterTask(clnt.cluster, serverPath), nil
	}

	err = parseInfoErrorCode(response)

	res := make(map[string]string)
	vals := strings.Split("error="+err.Error(), ";")
	for _, pair := range vals {
		t := strings.SplitN(pair, "=", 2)
		if len(t) == 2 {
			res[strings.ToLower(t[0])] = t[1]
		} else if len(t) == 1 {
			res[strings.ToLower(t[0])] = ""
		}
	}

	if _, exists := res["error"]; exists {
		msg, _ := base64.StdEncoding.DecodeString(res["message"])
		return nil, newError(err.resultCode(), fmt.Sprintf("Registration failed: %s\nFile: %s\nLine: %s\nMessage: %s",
			res["error"], res["file"], res["line"], msg))
	}

	// if message was not parsable
	return nil, parseInfoErrorCode(response)
}

// RemoveUDF removes a package containing user defined functions in the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RemoveTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RemoveUDF(policy *WritePolicy, udfName string) (*RemoveTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	strCmd.WriteString("udf-remove:filename=")
	strCmd.WriteString(udfName)
	strCmd.WriteString(";")

	// Send command to one node. That node will distribute it to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy.TotalTimeout, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := responseMap[strCmd.String()]
	if strings.EqualFold(response, "ok") {
		return NewRemoveTask(clnt.cluster, udfName), nil
	}
	return nil, parseInfoErrorCode(response)
}

// ListUDF lists all packages containing user defined functions in the server.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ListUDF(policy *BasePolicy) ([]*UDF, Error) {
	policy = clnt.getUsablePolicy(policy)

	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	strCmd.WriteString("udf-list")

	// Send command to one node. That node will distribute it to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy.TotalTimeout, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := responseMap[strCmd.String()]
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

	return res, nil
}

// Execute executes a user defined function on server and return results.
// The function operates on a single record.
// The package name is used to locate the udf file location:
//
// udf file = <server udf dir>/<package name>.lua
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (any, Error) {
	record, err := clnt.execute(policy, key, packageName, functionName, args...)
	if err != nil {
		return nil, err
	}

	if record == nil || len(record.Bins) == 0 {
		return nil, nil
	}

	for k, v := range record.Bins {
		if strings.Contains(k, "SUCCESS") {
			return v, nil
		} else if strings.Contains(k, "FAILURE") {
			return nil, newError(ErrUDFBadResponse.ResultCode, fmt.Sprintf("%v", v))
		}
	}

	return nil, ErrUDFBadResponse.err()
}

func (clnt *Client) execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (*Record, Error) {
	policy = clnt.getUsableWritePolicy(policy)

	if policy.Txn != nil {
		if err := txnMonitor.addKey(clnt.cluster, policy, key); err != nil {
			return nil, err
		}
	}

	command, err := newExecuteCommand(clnt.cluster, policy, key, packageName, functionName, NewValueArray(args))
	if err != nil {
		return nil, err
	}

	if err := command.Execute(); err != nil {
		return nil, err
	}

	return command.GetRecord(), nil
}

//----------------------------------------------------------
// Query/Execute (Supported by Aerospike 3+ servers only)
//----------------------------------------------------------

// QueryExecute applies operations on records that match the statement filter.
// Records are not returned to the client.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryExecute(policy *QueryPolicy,
	writePolicy *WritePolicy,
	statement *Statement,
	ops ...*Operation,
) (*ExecuteTask, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	writePolicy = clnt.getUsableWritePolicy(writePolicy)

	taskId := statement.prepareTaskId()

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	statement.prepare(false)

	var errs Error
	for i := range nodes {
		command := newServerCommand(nodes[i], policy, writePolicy, statement, ops)
		if err := command.Execute(); err != nil {
			errs = chainErrors(err, errs)
		}
	}

	return NewExecuteTask(clnt.cluster, statement, taskId), errs
}

// ExecuteUDF applies user defined function on records that match the statement filter.
// Records are not returned to the client.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ExecuteUDF(policy *QueryPolicy,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	taskId := statement.prepareTaskId()

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	statement.SetAggregateFunction(packageName, functionName, functionArgs, false)

	var errs Error
	for i := range nodes {
		command := newServerCommand(nodes[i], policy, nil, statement, nil)
		if err := command.Execute(); err != nil {
			errs = chainErrors(err, errs)
		}
	}

	return NewExecuteTask(clnt.cluster, statement, taskId), errs
}

// ExecuteUDFNode applies user defined function on records that match the statement filter on the specified node.
// Records are not returned to the client.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ExecuteUDFNode(policy *QueryPolicy,
	node *Node,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	taskId := statement.prepareTaskId()

	if node == nil {
		return nil, ErrClusterIsEmpty.err()
	}

	statement.SetAggregateFunction(packageName, functionName, functionArgs, false)

	command := newServerCommand(node, policy, nil, statement, nil)
	err := command.Execute()

	return NewExecuteTask(clnt.cluster, statement, taskId), err
}

// SetXDRFilter sets XDR filter for given datacenter name and namespace. The expression filter indicates
// which records XDR should ship to the datacenter.
// Pass nil as filter to remove the current filter on the server.
func (clnt *Client) SetXDRFilter(policy *InfoPolicy, datacenter string, namespace string, filter *Expression) Error {
	policy = clnt.getUsableInfoPolicy(policy)

	var strCmd string
	if filter == nil {
		strCmd = "xdr-set-filter:dc=" + datacenter + ";namespace=" + namespace + ";exp=null"
	} else {
		b64, err := filter.Base64()
		if err != nil {
			return newError(types.SERIALIZE_ERROR, "FilterExpression could not be serialized to Base64")
		}

		strCmd = "xdr-set-filter:dc=" + datacenter + ";namespace=" + namespace + ";exp=" + b64
	}

	// Send command to one node. That node will distribute it to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy.Timeout, strCmd)
	if err != nil {
		return err
	}

	response := responseMap[strCmd]
	if strings.EqualFold(response, "ok") {
		return nil
	}

	return parseInfoErrorCode(response)
}

var infoErrRegexp = regexp.MustCompile(`(?i)(fail|error)((:|=)(?P<code>[0-9]+))?((:|=)(?P<msg>.+))?`)

func parseInfoErrorCode(response string) Error {
	match := infoErrRegexp.FindStringSubmatch(response)

	code := types.SERVER_ERROR
	message := response

	if len(match) > 0 {
		for i, name := range infoErrRegexp.SubexpNames() {
			if i != 0 && name != "" && len(match[i]) > 0 {
				switch name {
				case "code":
					i, err := strconv.ParseInt(match[i], 10, 64)
					if err == nil {
						code = types.ResultCode(i)
						message = types.ResultCodeToString(code)
					}
				case "msg":
					message = match[i]
				}
			}
		}
	}

	return newError(code, message)
}

//--------------------------------------------------------
// Query functions (Supported by Aerospike 3+ servers only)
//--------------------------------------------------------

// QueryPartitions executes a query for specified partitions and returns a recordset.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop records off the channel through the
// Recordset.Records channel.
//
// This method is only supported by Aerospike 4.9+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryPartitions(policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter) (*Recordset, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}

	var tracker *partitionTracker
	if partitionFilter == nil {
		tracker = newPartitionTrackerForNodes(&policy.MultiPolicy, nodes)
	} else {
		tracker = newPartitionTracker(&policy.MultiPolicy, partitionFilter, nodes)
	}

	// result recordset
	res := newRecordset(policy.RecordQueueSize, 1)
	go clnt.queryPartitions(policy, tracker, statement, res)

	return res, nil
}

// Query executes a query and returns a Recordset.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop records off the channel through the
// Recordset.Records channel.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Query(policy *QueryPolicy, statement *Statement) (*Recordset, Error) {
	return clnt.QueryPartitions(policy, statement, nil)
}

// QueryNode executes a query on a specific node and returns a recordset.
// The caller can concurrently pop records off the channel through the
// record channel.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryNode(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error) {
	return clnt.queryNodePartitions(policy, node, statement)
}

func (clnt *Client) queryNodePartitions(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	tracker := newPartitionTrackerForNode(&policy.MultiPolicy, node)

	// result recordset
	res := newRecordset(policy.RecordQueueSize, 1)
	go clnt.queryPartitions(policy, tracker, statement, res)

	return res, nil
}

//-------------------------------------------------------
// Multi-Record Transactions
//-------------------------------------------------------

// Attempt to commit the given multi-record transaction. First, the expected record versions are
// sent to the server nodes for verification. If all nodes return success, the transaction is
// committed. Otherwise, the transaction is aborted.
//
// Requires server version 8.0+
func (clnt *Client) Commit(txn *Txn) (CommitStatus, Error) {
	tr := NewTxnRoll(clnt, txn)

	switch txn.State() {
	default:
		fallthrough
	case TxnStateOpen:
		if err := tr.Verify(&clnt.getUsableTxnVerifyPolicy(nil).BatchPolicy, &clnt.getUsableTxnRollPolicy(nil).BatchPolicy); err != nil {
			return CommitStatusUnverified, err
		}
		return tr.Commit(&clnt.getUsableTxnRollPolicy(nil).BatchPolicy)
	case TxnStateVerified:
		return tr.Commit(&clnt.getUsableTxnRollPolicy(nil).BatchPolicy)
	case TxnStateCommitted:
		return CommitStatusAlreadyCommitted, nil
	case TxnStateAborted:
		return CommitStatusAlreadyAborted, newError(types.TXN_ALREADY_ABORTED, "Transaction already aborted")
	}
}

// Abort and rollback the given multi-record transaction.
//
// Requires server version 8.0+
func (clnt *Client) Abort(txn *Txn) (AbortStatus, Error) {
	tr := NewTxnRoll(clnt, txn)
	switch txn.State() {
	default:
		fallthrough
	case TxnStateOpen:
		fallthrough
	case TxnStateVerified:
		return tr.Abort(&clnt.getUsableTxnRollPolicy(nil).BatchPolicy)
	case TxnStateCommitted:
		return AbortStatusAlreadyCommitted, newError(types.TXN_ALREADY_COMMITTED, "Transaction already committed")
	case TxnStateAborted:
		return AbortStatusAlreadyAborted, nil
	}
}

//--------------------------------------------------------
// Index functions (Supported by Aerospike 3+ servers only)
//--------------------------------------------------------

// CreateIndex creates a secondary index.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) CreateIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
) (*IndexTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	return clnt.createIndex(policy, namespace, setName, indexName, binName, indexType, ICT_DEFAULT, nil)
}

// CreateComplexIndex creates a secondary index, with the ability to put indexes
// on bin containing complex data types, e.g: Maps and Lists.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) CreateComplexIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
	indexCollectionType IndexCollectionType,
	ctx ...*CDTContext,
) (*IndexTask, Error) {
	return clnt.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType, nil, ctx...)
}

// CreateIndexWithExpression creates a secondary index with expressions.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) CreateIndexWithExpression(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	indexType IndexType,
	indexCollectionType IndexCollectionType,
	expression *Expression,
) (*IndexTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	return clnt.createIndex(policy, namespace, setName, indexName, "", indexType, indexCollectionType, expression)
}

// createIndex is a helper function to create a secondary index used by other CreateIndex external methods.
func (clnt *Client) createIndex(policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
	indexCollectionType IndexCollectionType,
	expression *Expression,
	ctx ...*CDTContext,
) (*IndexTask, Error) {
	policy = clnt.getUsableWritePolicy(policy)

	var strCmd bytes.Buffer
	strCmd.WriteString("sindex-create:ns=")
	strCmd.WriteString(namespace)

	if len(setName) > 0 {
		strCmd.WriteString(";set=")
		strCmd.WriteString(setName)
	}

	strCmd.WriteString(";indexname=")
	strCmd.WriteString(indexName)

	var bufEx *bufferEx
	if len(ctx) > 0 {
		sz, err := cdtContextList(ctx).packArray(nil)
		if err != nil {
			return nil, err
		}

		bufEx = newBuffer(sz)

		_, err = cdtContextList(ctx).packArray(bufEx)
		if err != nil {
			return nil, err
		}

		strCmd.WriteString(";context=")
		s := base64.StdEncoding.EncodeToString(bufEx.Bytes())
		strCmd.WriteString(s)
	}

	if expression != nil {
		if size, err := expression.size(); err == nil && size > 0 {
			b64, err := expression.Base64()
			if err != nil {
				return nil, err
			}
			strCmd.WriteString(";exp=")
			strCmd.WriteString(b64)
		}
	}

	if indexCollectionType != ICT_DEFAULT {
		strCmd.WriteString(";indextype=")
		strCmd.WriteString(ictToString(indexCollectionType))
	}

	if binName != "" {
		strCmd.WriteString(";indexdata=")
		strCmd.WriteString(binName)
		strCmd.WriteString(",")
	} else {
		strCmd.WriteString(";type=")
	}

	strCmd.WriteString(string(indexType))
	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy.TotalTimeout, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := responseMap[strCmd.String()]
	if strings.EqualFold(response, "OK") {
		// Return task that could optionally be polled for completion.
		return NewIndexTask(clnt.cluster, namespace, indexName), nil
	}

	return nil, parseInfoErrorCode(response)
}

// DropIndex deletes a secondary index. It will block until index is dropped on all nodes.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) DropIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
) Error {
	policy = clnt.getUsableWritePolicy(policy)
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
	responseMap, err := clnt.sendInfoCommand(policy.TotalTimeout, strCmd.String())
	if err != nil {
		return err
	}

	response := responseMap[strCmd.String()]

	if strings.EqualFold(response, "OK") {
		// Return task that could optionally be polled for completion.
		task := NewDropIndexTask(clnt.cluster, namespace, indexName)
		return <-task.OnComplete()
	}

	err = parseInfoErrorCode(response)
	if err.Matches(types.INDEX_NOTFOUND) {
		// Index did not previously exist. Return without error.
		return nil
	}

	return err
}

// Truncate removes records in specified namespace/set efficiently.  This method is many orders of magnitude
// faster than deleting records one at a time.  Works with Aerospike Server versions >= 3.12.
// This asynchronous server call may return before the truncation is complete.  The user can still
// write new records after the server call returns because new records will have last update times
// greater than the truncate cutoff (set at the time of truncate call).
// For more information, See https://www.aerospike.com/docs/reference/info#truncate
func (clnt *Client) Truncate(policy *InfoPolicy, namespace, set string, beforeLastUpdate *time.Time) Error {
	policy = clnt.getUsableInfoPolicy(policy)

	var strCmd bytes.Buffer
	if len(set) > 0 {
		strCmd.WriteString("truncate:namespace=")
		strCmd.WriteString(namespace)
		strCmd.WriteString(";set=")
		strCmd.WriteString(set)
	} else {
		strCmd.WriteString("truncate-namespace:namespace=")
		strCmd.WriteString(namespace)
	}
	if beforeLastUpdate != nil {
		strCmd.WriteString(";lut=")
		strCmd.WriteString(strconv.FormatInt(beforeLastUpdate.UnixNano(), 10))
	}

	responseMap, err := clnt.sendInfoCommand(policy.Timeout, strCmd.String())
	if err != nil {
		return err
	}

	response := responseMap[strCmd.String()]
	if strings.EqualFold(response, "OK") {
		return nil
	}

	return parseInfoErrorCode(response)
}

//-------------------------------------------------------
// User administration
//-------------------------------------------------------

// CreateUser creates a new user with password and roles. Clear-text password will be hashed using bcrypt
// before sending to server.
func (clnt *Client) CreateUser(policy *AdminPolicy, user string, password string, roles []string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.createUser(conn, policy, user, hash, roles)
	})

	return err
}

// CreatePKIUser creates a new user PKI user with roles. PKI users are authenticated via TLS and a certificate instead of a password.
// Supported by Aerospike Server v8.1+ Enterprise.
func (clnt *Client) CreatePKIUser(policy *AdminPolicy, user string, roles []string) Error {
	policy = clnt.getUsableAdminPolicy(policy)
	noPassword := "nopassword"
	serverMinVersion, _ := internal.Parse("8.1.0.0")

	hash, err := hashPassword(noPassword)
	if err != nil {
		return err
	}

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}
	// Check server version to ensure it supports PKI users.
	if node.version.IsSmaller(serverMinVersion) {
		return newCommonError(nil, fmt.Sprintf("Node version %s is less than required minimum version %s", node.version.String(), serverMinVersion))
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.createUser(conn, policy, user, hash, roles)
	})

	if err != nil {
		return newError(err.resultCode(), fmt.Sprintf("PKI user creation failed: %s", err.Error()))
	}

	return nil
}

// DropUser removes a user from the cluster.
func (clnt *Client) DropUser(policy *AdminPolicy, user string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.dropUser(conn, policy, user)
	})
	return err
}

// ChangePassword changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
func (clnt *Client) ChangePassword(policy *AdminPolicy, user string, password string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	if clnt.cluster.user == "" {
		return ErrInvalidUser.err()
	}

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)

		if user == clnt.cluster.user {
			// Change own password.
			err = command.changePassword(conn, policy, user, clnt.cluster.Password(), hash)
		} else {
			// Change other user's password by user admin.
			err = command.setPassword(conn, policy, user, hash)
		}
	})

	if err == nil {
		clnt.cluster.changePassword(user, password, hash)
	}

	return err
}

// GrantRoles adds roles to user's list of roles.
func (clnt *Client) GrantRoles(policy *AdminPolicy, user string, roles []string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.grantRoles(conn, policy, user, roles)
	})
	return err
}

// RevokeRoles removes roles from user's list of roles.
func (clnt *Client) RevokeRoles(policy *AdminPolicy, user string, roles []string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.revokeRoles(conn, policy, user, roles)
	})

	return err
}

// QueryUser retrieves roles for a given user.
func (clnt *Client) QueryUser(policy *AdminPolicy, user string) (res *UserRoles, err Error) {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		res, err = command.QueryUser(conn, policy, user)
	})
	return res, err
}

// QueryUsers retrieves all users and their roles.
func (clnt *Client) QueryUsers(policy *AdminPolicy) (res []*UserRoles, err Error) {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		res, err = command.QueryUsers(conn, policy)
	})
	return res, err
}

// QueryRole retrieves privileges for a given role.
func (clnt *Client) QueryRole(policy *AdminPolicy, role string) (res *Role, err Error) {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		res, err = command.QueryRole(conn, policy, role)
	})
	return res, err
}

// QueryRoles retrieves all roles and their privileges.
func (clnt *Client) QueryRoles(policy *AdminPolicy) (res []*Role, err Error) {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		res, err = command.QueryRoles(conn, policy)
	})
	return res, err
}

// CreateRole creates a user-defined role.
// Quotas require server security configuration "enable-quotas" to be set to true.
// Pass 0 for quota values for no limit.
func (clnt *Client) CreateRole(policy *AdminPolicy, roleName string, privileges []Privilege, whitelist []string, readQuota, writeQuota uint32) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.createRole(conn, policy, roleName, privileges, whitelist, readQuota, writeQuota)
	})
	return err
}

// DropRole removes a user-defined role.
func (clnt *Client) DropRole(policy *AdminPolicy, roleName string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.dropRole(conn, policy, roleName)
	})
	return err
}

// GrantPrivileges grant privileges to a user-defined role.
func (clnt *Client) GrantPrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.grantPrivileges(conn, policy, roleName, privileges)
	})
	return err
}

// RevokePrivileges revokes privileges from a user-defined role.
func (clnt *Client) RevokePrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.revokePrivileges(conn, policy, roleName, privileges)
	})
	return err
}

// SetWhitelist sets IP address whitelist for a role. If whitelist is nil or empty, it removes existing whitelist from role.
func (clnt *Client) SetWhitelist(policy *AdminPolicy, roleName string, whitelist []string) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.setWhitelist(conn, policy, roleName, whitelist)
	})
	return err
}

// SetQuotas sets maximum reads/writes per second limits for a role.  If a quota is zero, the limit is removed.
// Quotas require server security configuration "enable-quotas" to be set to true.
// Pass 0 for quota values for no limit.
func (clnt *Client) SetQuotas(policy *AdminPolicy, roleName string, readQuota, writeQuota uint32) Error {
	policy = clnt.getUsableAdminPolicy(policy)

	// prepare the node.tendConn
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return err
	}

	node.usingTendConn(policy.Timeout, func(conn *Connection) {
		command := NewAdminCommand(nil)
		err = command.setQuotas(conn, policy, roleName, readQuota, writeQuota)
	})
	return err
}

//-------------------------------------------------------
// Access Methods
//-------------------------------------------------------

// Cluster exposes the cluster object to the user
func (clnt *Client) Cluster() *Cluster {
	return clnt.cluster
}

// String implements the Stringer interface for client
func (clnt *Client) String() string {
	if clnt.cluster != nil {
		return clnt.cluster.String()
	}
	return ""
}

// MetricsEnabled returns true if metrics are enabled for the cluster.
func (clnt *Client) MetricsEnabled() bool {
	return clnt.cluster.MetricsEnabled()
}

// EnableMetrics enables the cluster command metrics gathering.
// If the parameters for the histogram in the policy are different from the one already
// on the cluster, the metrics will be reset.
func (clnt *Client) EnableMetrics(policy *MetricsPolicy) {
	if clnt.dynConfig == nil {
		clnt.cluster.EnableMetrics(policy)
		return
	}

	// Atomically load config to avoid race conditions
	currentConfig := clnt.dynConfig.config
	if currentConfig == nil ||
		currentConfig.Dynamic == nil ||
		currentConfig.Dynamic.Metrics == nil {

		clnt.cluster.EnableMetrics(policy)
	}
}

// DisableMetrics disables the cluster command metrics gathering.
func (clnt *Client) DisableMetrics() {
	if clnt.dynConfig != nil {
		logger.Logger.Warn("Dynamic configuration is enabled. Metrics cannot be disabled via the client API.")
	} else {
		clnt.cluster.DisableMetrics()
	}
}

// Stats returns internal statistics regarding the inner state of the client and the cluster.
func (clnt *Client) Stats() (map[string]any, Error) {
	resStats := clnt.cluster.statsCopy()

	mp := clnt.cluster.MetricsPolicy()
	clusterStats := *newNodeStats(mp)
	for _, stats := range resStats {
		clusterStats.aggregate(&stats)
	}

	clusterStats.StatLabels = clnt.cluster.getNodeLabels()
	resStats["cluster-aggregated-stats"] = clusterStats

	b, err := json.Marshal(resStats)
	if err != nil {
		return nil, newCommonError(err)
	}

	res := map[string]any{}
	err = json.Unmarshal(b, &res)
	if err != nil {
		return nil, newCommonError(err)
	}

	res["open-connections"] = clusterStats.ConnectionsOpen.Get()
	res["total-nodes"] = len(clnt.cluster.GetNodes())

	aggstats := res["cluster-aggregated-stats"].(map[string]any)
	aggstats["exceeded-max-retries"] = clnt.cluster.maxRetriesExceededCount.Get()
	aggstats["exceeded-total-timeout"] = clnt.cluster.totalTimeoutExceededCount.Get()

	return res, nil
}

// WarmUp fills the connection pool with connections for all nodes.
// This is necessary on startup for high traffic programs.
// If the count is <= 0, the connection queue will be filled.
// If the count is more than the size of the pool, the pool will be filled.
// Note: One connection per node is reserved for tend operations and is not used for transactions.
func (clnt *Client) WarmUp(count int) (int, Error) {
	return clnt.cluster.WarmUp(count)
}

//-------------------------------------------------------
// Internal Methods
//-------------------------------------------------------

func (clnt *Client) sendInfoCommand(timeout time.Duration, command string) (map[string]string, Error) {
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	policy := InfoPolicy{Timeout: timeout}
	return node.RequestInfo(&policy, command)
}

// -------------------------------------------------------
// Policy Methods
// -------------------------------------------------------
func (clnt *Client) getUsablePolicy(policy *BasePolicy) *BasePolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultPolicy != nil {
		return clnt.DefaultPolicy
	}
	return clnt.dynDefaultPolicy.Load()
}

func (clnt *Client) getUsableBatchPolicy(policy *BatchPolicy) *BatchPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultBatchPolicy != nil {
		return clnt.DefaultBatchPolicy
	}
	return clnt.dynDefaultBatchPolicy.Load()
}

func (clnt *Client) getUsableBatchReadPolicy(policy *BatchReadPolicy) *BatchReadPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultBatchReadPolicy != nil {
		return clnt.DefaultBatchReadPolicy
	}
	return clnt.dynDefaultBatchReadPolicy.Load()

}

func (clnt *Client) getUsableBatchWritePolicy(policy *BatchWritePolicy) *BatchWritePolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultBatchWritePolicy != nil {
		return clnt.DefaultBatchWritePolicy
	}
	return clnt.dynDefaultBatchWritePolicy.Load()
}

func (clnt *Client) getUsableBatchDeletePolicy(policy *BatchDeletePolicy) *BatchDeletePolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultBatchDeletePolicy != nil {
		return clnt.DefaultBatchDeletePolicy
	}
	return clnt.dynDefaultBatchDeletePolicy.Load()
}

func (clnt *Client) getUsableBatchUDFPolicy(policy *BatchUDFPolicy) *BatchUDFPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultBatchUDFPolicy != nil {
		return clnt.DefaultBatchUDFPolicy
	}
	return clnt.dynDefaultBatchUDFPolicy.Load()
}

func (clnt *Client) getUsableWritePolicy(policy *WritePolicy) *WritePolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultWritePolicy != nil {
		return clnt.DefaultWritePolicy
	}
	return clnt.dynDefaultWritePolicy.Load()
}

func (clnt *Client) getUsableScanPolicy(policy *ScanPolicy) *ScanPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultScanPolicy != nil {
		return clnt.DefaultScanPolicy
	}
	return clnt.dynDefaultScanPolicy.Load()
}

func (clnt *Client) getUsableQueryPolicy(policy *QueryPolicy) *QueryPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.pathDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultQueryPolicy != nil {
		return clnt.DefaultQueryPolicy
	}
	return clnt.dynDefaultQueryPolicy.Load()
}

func (clnt *Client) getUsableAdminPolicy(policy *AdminPolicy) *AdminPolicy {
	if policy == nil {
		if clnt.DefaultAdminPolicy != nil {
			return clnt.DefaultAdminPolicy
		}
		return NewAdminPolicy()
	}
	return policy
}

func (clnt *Client) getUsableInfoPolicy(policy *InfoPolicy) *InfoPolicy {
	if policy == nil {
		if clnt.DefaultInfoPolicy != nil {
			return clnt.DefaultInfoPolicy
		}
		return NewInfoPolicy()
	}
	return policy
}

func (clnt *Client) getUsableTxnRollPolicy(policy *TxnRollPolicy) *TxnRollPolicy {
	if policy != nil {
		// Merge policy with dynamic config
		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultTxnRollPolicy != nil {
		return clnt.DefaultTxnRollPolicy
	}
	return clnt.dynDefaultTxnRollPolicy.Load()
}

func (clnt *Client) getUsableTxnVerifyPolicy(policy *TxnVerifyPolicy) *TxnVerifyPolicy {
	if policy != nil {
		// Merge policy with dynamic config

		return policy.patchDynamic(clnt.dynConfig)
	}
	// Make sure to handle the case where the user is setting Default....Policy policy and
	// dynConfig is nil. Essentially, we do not want to treat cache as default
	// when dynConfig is nil. Separation of concerns.
	if clnt.dynConfig == nil && clnt.DefaultTxnVerifyPolicy != nil {
		return clnt.DefaultTxnVerifyPolicy
	}
	return clnt.dynDefaultTxnVerifyPolicy.Load()
}

func getUsableClientPolicy(policy *ClientPolicy, dynConfig *DynConfig) *ClientPolicy {
	if policy == nil {
		return NewClientPolicy().patchDynamic(dynConfig).ensureErrorRates()
	}

	return policy.patchDynamic(dynConfig).ensureErrorRates()
}

//-------------------------------------------------------
// Utility Functions
//-------------------------------------------------------

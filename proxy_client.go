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
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	iatomic "github.com/aerospike/aerospike-client-go/v6/internal/atomic"
	kvs "github.com/aerospike/aerospike-client-go/v6/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v6/types"
)

// ProxyClient encapsulates an Aerospike cluster.
// All database operations are available against this object.
type ProxyClient struct {
	// only for GRPC
	clientPolicy ClientPolicy
	grpcConnPool *sync.Pool
	grpcHost     *Host
	dialOptions  []grpc.DialOption

	authToken       atomic.Value
	authInterceptor *authInterceptor

	active iatomic.Bool

	// DefaultPolicy is used for all read commands without a specific policy.
	DefaultPolicy *BasePolicy
	// DefaultBatchPolicy is the default parent policy used in batch read commands. Base policy fields
	// include socketTimeout, totalTimeout, maxRetries, etc...
	DefaultBatchPolicy *BatchPolicy
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
}

func grpcClientFinalizer(f *ProxyClient) {
	f.Close()
}

//-------------------------------------------------------
// Constructors
//-------------------------------------------------------

// NewProxyClientWithPolicyAndHost generates a new ProxyClient with the specified ClientPolicy and
// sets up the cluster using the provided hosts.
// If the policy is nil, the default relevant policy will be used.
// Pass "dns:///<address>:<port>" (note the 3 slashes) for dns load balancing,
// automatically supported internally by grpc-go.
func NewProxyClientWithPolicyAndHost(policy *ClientPolicy, host *Host, dialOptions ...grpc.DialOption) (*ProxyClient, Error) {
	if policy == nil {
		policy = NewClientPolicy()
	}

	grpcClient := &ProxyClient{
		clientPolicy: *policy,
		grpcConnPool: new(sync.Pool),
		grpcHost:     host,
		dialOptions:  dialOptions,

		active: *iatomic.NewBool(true),

		DefaultPolicy:            NewPolicy(),
		DefaultBatchPolicy:       NewBatchPolicy(),
		DefaultBatchWritePolicy:  NewBatchWritePolicy(),
		DefaultBatchDeletePolicy: NewBatchDeletePolicy(),
		DefaultBatchUDFPolicy:    NewBatchUDFPolicy(),
		DefaultWritePolicy:       NewWritePolicy(0, 0),
		DefaultScanPolicy:        NewScanPolicy(),
		DefaultQueryPolicy:       NewQueryPolicy(),
		DefaultAdminPolicy:       NewAdminPolicy(),
	}

	if policy.RequiresAuthentication() {
		authInterceptor, err := newAuthInterceptor(grpcClient)
		if err != nil {
			return nil, err
		}

		grpcClient.authInterceptor = authInterceptor
	}

	// check the version to make sure we are connected to the server
	infoPolicy := NewInfoPolicy()
	infoPolicy.Timeout = policy.Timeout
	_, err := grpcClient.ServerVersion(infoPolicy)
	if err != nil {
		return nil, err
	}

	runtime.SetFinalizer(grpcClient, grpcClientFinalizer)
	return grpcClient, nil
}

//-------------------------------------------------------
// Policy methods
//-------------------------------------------------------

// DefaultPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultPolicy() *BasePolicy {
	return clnt.DefaultPolicy
}

// DefaultBatchPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultBatchPolicy() *BatchPolicy {
	return clnt.DefaultBatchPolicy
}

// DefaultBatchWritePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultBatchWritePolicy() *BatchWritePolicy {
	return clnt.DefaultBatchWritePolicy
}

// DefaultBatchDeletePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultBatchDeletePolicy() *BatchDeletePolicy {
	return clnt.DefaultBatchDeletePolicy
}

// DefaultBatchUDFPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultBatchUDFPolicy() *BatchUDFPolicy {
	return clnt.DefaultBatchUDFPolicy
}

// DefaultWritePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultWritePolicy() *WritePolicy {
	return clnt.DefaultWritePolicy
}

// DefaultScanPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultScanPolicy() *ScanPolicy {
	return clnt.DefaultScanPolicy
}

// DefaultQueryPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultQueryPolicy() *QueryPolicy {
	return clnt.DefaultQueryPolicy
}

// DefaultAdminPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultAdminPolicy() *AdminPolicy {
	return clnt.DefaultAdminPolicy
}

// DefaultInfoPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) GetDefaultInfoPolicy() *InfoPolicy {
	return clnt.DefaultInfoPolicy
}

// DefaultPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultPolicy(policy *BasePolicy) {
	clnt.DefaultPolicy = policy
}

// DefaultBatchPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultBatchPolicy(policy *BatchPolicy) {
	clnt.DefaultBatchPolicy = policy
}

// DefaultBatchWritePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultBatchWritePolicy(policy *BatchWritePolicy) {
	clnt.DefaultBatchWritePolicy = policy
}

// DefaultBatchDeletePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultBatchDeletePolicy(policy *BatchDeletePolicy) {
	clnt.DefaultBatchDeletePolicy = policy
}

// DefaultBatchUDFPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultBatchUDFPolicy(policy *BatchUDFPolicy) {
	clnt.DefaultBatchUDFPolicy = policy
}

// DefaultWritePolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultWritePolicy(policy *WritePolicy) {
	clnt.DefaultWritePolicy = policy
}

// DefaultScanPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultScanPolicy(policy *ScanPolicy) {
	clnt.DefaultScanPolicy = policy
}

// DefaultQueryPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultQueryPolicy(policy *QueryPolicy) {
	clnt.DefaultQueryPolicy = policy
}

// DefaultAdminPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultAdminPolicy(policy *AdminPolicy) {
	clnt.DefaultAdminPolicy = policy
}

// DefaultInfoPolicy returns corresponding default policy from the client
func (clnt *ProxyClient) SetDefaultInfoPolicy(policy *InfoPolicy) {
	clnt.DefaultInfoPolicy = policy
}

//-------------------------------------------------------
// Cluster Connection Management
//-------------------------------------------------------

func (clnt *ProxyClient) token() string {
	return clnt.authToken.Load().(string)
}

func (clnt *ProxyClient) setAuthToken(token string) {
	clnt.authToken.Store(token)
}

func (clnt *ProxyClient) grpcConn() (*grpc.ClientConn, Error) {
	pconn := clnt.grpcConnPool.Get()
	if pconn != nil {
		return pconn.(*grpc.ClientConn), nil
	}

	return clnt.createGrpcConn(!clnt.clientPolicy.RequiresAuthentication())
}

func (clnt *ProxyClient) returnGrpcConnToPool(conn *grpc.ClientConn) {
	if conn != nil {
		clnt.grpcConnPool.Put(conn)
	}
}

func (clnt *ProxyClient) createGrpcConn(noInterceptor bool) (*grpc.ClientConn, Error) {
	// make a new connection
	// Implement TLS and auth
	dialOptions := []grpc.DialOption{}
	if clnt.clientPolicy.TlsConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(clnt.clientPolicy.TlsConfig)))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	ctx, _ := context.WithTimeout(context.Background(), clnt.clientPolicy.Timeout)

	allOptions := append(dialOptions, clnt.dialOptions...)
	if !noInterceptor {
		allOptions = append(dialOptions,
			grpc.WithUnaryInterceptor(clnt.authInterceptor.Unary()),
			grpc.WithStreamInterceptor(clnt.authInterceptor.Stream()),
		)
	}

	conn, err := grpc.DialContext(ctx, clnt.grpcHost.String(), allOptions...)
	if err != nil {
		return nil, newError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, err.Error())
	}

	return conn, nil
}

// Close closes all Grpcclient connections to database server nodes.
func (clnt *ProxyClient) Close() {
	clnt.active.Set(false)
}

// IsConnected determines if the Grpcclient is ready to talk to the database server cluster.
func (clnt *ProxyClient) IsConnected() bool {
	return clnt.active.Get()
}

// GetNodes returns an array of active server nodes in the cluster.
func (clnt *ProxyClient) GetNodes() []*Node {
	panic("NOT_SUPPORTED")
}

// GetNodeNames returns a list of active server node names in the cluster.
func (clnt *ProxyClient) GetNodeNames() []string {
	panic("NOT_SUPPORTED")
}

// ServerVersion will return the version of the proxy server.
func (clnt *ProxyClient) ServerVersion(policy *InfoPolicy) (string, Error) {
	policy = clnt.getUsableInfoPolicy(policy)

	req := kvs.AboutRequest{}

	conn, err := clnt.grpcConn()
	if err != nil {
		return "", err
	}

	client := kvs.NewAboutClient(conn)

	ctx := policy.grpcDeadlineContext()

	res, gerr := client.Get(ctx, &req)
	if gerr != nil {
		return "", newGrpcError(gerr, gerr.Error())
	}

	clnt.returnGrpcConnToPool(conn)

	return res.Version, nil
}

//-------------------------------------------------------
// Write Record Operations
//-------------------------------------------------------

// Put writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Put(policy *WritePolicy, key *Key, binMap BinMap) Error {
	policy = clnt.getUsableWritePolicy(policy)
	command, err := newWriteCommand(nil, policy, key, nil, binMap, _WRITE)
	if err != nil {
		return err
	}

	return command.ExecuteGRPC(clnt)
}

// PutBins writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This method avoids using the BinMap allocation and iteration and is lighter on GC.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) PutBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	policy = clnt.getUsableWritePolicy(policy)
	command, err := newWriteCommand(nil, policy, key, bins, nil, _WRITE)
	if err != nil {
		return err
	}

	return command.ExecuteGRPC(clnt)
}

//-------------------------------------------------------
// Operations string
//-------------------------------------------------------

// Append appends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Append(policy *WritePolicy, key *Key, binMap BinMap) Error {
	ops := make([]*Operation, 0, len(binMap))
	for k, v := range binMap {
		ops = append(ops, AppendOp(NewBin(k, v)))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

// AppendBins works the same as Append, but avoids BinMap allocation and iteration.
func (clnt *ProxyClient) AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	ops := make([]*Operation, 0, len(bins))
	for _, bin := range bins {
		ops = append(ops, AppendOp(bin))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

// Prepend prepends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call works only for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Prepend(policy *WritePolicy, key *Key, binMap BinMap) Error {
	ops := make([]*Operation, 0, len(binMap))
	for k, v := range binMap {
		ops = append(ops, PrependOp(NewBin(k, v)))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

// PrependBins works the same as Prepend, but avoids BinMap allocation and iteration.
func (clnt *ProxyClient) PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	ops := make([]*Operation, 0, len(bins))
	for _, bin := range bins {
		ops = append(ops, PrependOp(bin))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

//-------------------------------------------------------
// Arithmetic Operations
//-------------------------------------------------------

// Add adds integer bin values to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for integer values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Add(policy *WritePolicy, key *Key, binMap BinMap) Error {
	ops := make([]*Operation, 0, len(binMap))
	for k, v := range binMap {
		ops = append(ops, AddOp(NewBin(k, v)))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

// AddBins works the same as Add, but avoids BinMap allocation and iteration.
func (clnt *ProxyClient) AddBins(policy *WritePolicy, key *Key, bins ...*Bin) Error {
	ops := make([]*Operation, 0, len(bins))
	for _, bin := range bins {
		ops = append(ops, AddOp(bin))
	}

	_, err := clnt.Operate(policy, key, ops...)
	return err
}

//-------------------------------------------------------
// Delete Operations
//-------------------------------------------------------

// Delete deletes a record for specified key.
// The policy specifies the transaction timeout.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Delete(policy *WritePolicy, key *Key) (bool, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	command, err := newDeleteCommand(nil, policy, key)
	if err != nil {
		return false, err
	}

	err = command.ExecuteGRPC(clnt)
	return command.Existed(), err
}

//-------------------------------------------------------
// Touch Operations
//-------------------------------------------------------

// Touch updates a record's metadata.
// If the record exists, the record's TTL will be reset to the
// policy's expiration.
// If the record doesn't exist, it will return an error.
func (clnt *ProxyClient) Touch(policy *WritePolicy, key *Key) Error {
	policy = clnt.getUsableWritePolicy(policy)
	command, err := newTouchCommand(nil, policy, key)
	if err != nil {
		return err
	}

	return command.ExecuteGRPC(clnt)
}

//-------------------------------------------------------
// Existence-Check Operations
//-------------------------------------------------------

// Exists determine if a record key exists.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Exists(policy *BasePolicy, key *Key) (bool, Error) {
	policy = clnt.getUsablePolicy(policy)
	command, err := newExistsCommand(nil, policy, key)
	if err != nil {
		return false, err
	}

	err = command.ExecuteGRPC(clnt)
	return command.Exists(), err
}

// BatchExists determines if multiple record keys exist in one batch request.
// The returned boolean array is in positional order with the original key array order.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) BatchExists(policy *BatchPolicy, keys []*Key) ([]bool, Error) {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	for _, key := range keys {
		batchRecordsIfc = append(batchRecordsIfc, NewBatchReadHeader(key))
	}

	err := clnt.BatchOperate(policy, batchRecordsIfc)
	records := make([]bool, 0, len(keys))
	for i := range batchRecordsIfc {
		records = append(records, batchRecordsIfc[i].BatchRec().Record != nil)
		// if nerr := batchRecordsIfc[i].BatchRec().Err; nerr != nil {
		// 	err = chainErrors(err, nerr)
		// }
	}

	return records, err
}

//-------------------------------------------------------
// Read Record Operations
//-------------------------------------------------------

// Get reads a record header and bins for specified key.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, Error) {
	policy = clnt.getUsablePolicy(policy)

	command, err := newReadCommand(nil, policy, key, binNames, nil)
	if err != nil {
		return nil, err
	}

	if err := command.ExecuteGRPC(clnt); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

// GetHeader reads a record generation and expiration only for specified key.
// Bins are not read.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) GetHeader(policy *BasePolicy, key *Key) (*Record, Error) {
	policy = clnt.getUsablePolicy(policy)

	command, err := newReadHeaderCommand(nil, policy, key)
	if err != nil {
		return nil, err
	}

	if err := command.ExecuteGRPC(clnt); err != nil {
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
func (clnt *ProxyClient) BatchGet(policy *BatchPolicy, keys []*Key, binNames ...string) ([]*Record, Error) {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	batchRecords := make([]*BatchRecord, 0, len(keys))
	for _, key := range keys {
		batchRead, batchRecord := newBatchRead(key, binNames)
		batchRecordsIfc = append(batchRecordsIfc, batchRead)
		batchRecords = append(batchRecords, batchRecord)
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	records := make([]*Record, 0, len(keys))
	for i := range batchRecords {
		records = append(records, batchRecords[i].Record)
	}

	return records, err
}

// BatchGetOperate reads multiple records for specified keys using read operations in one batch call.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
//
// If a batch request to a node fails, the entire batch is cancelled.
func (clnt *ProxyClient) BatchGetOperate(policy *BatchPolicy, keys []*Key, ops ...*Operation) ([]*Record, Error) {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	batchRecords := make([]*BatchRecord, 0, len(keys))
	for _, key := range keys {
		batchRead, batchRecord := newBatchReadOps(key, ops)
		batchRecordsIfc = append(batchRecordsIfc, batchRead)
		batchRecords = append(batchRecords, batchRecord)
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	records := make([]*Record, 0, len(keys))
	for i := range batchRecords {
		records = append(records, batchRecords[i].Record)
	}

	return records, err
}

// BatchGetComplex reads multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins to be requested for each key in the batch.
// The returned records are located in the same list.
// If the BatchRead key field is not found, the corresponding record field will be nil.
// The policy can be used to specify timeouts and maximum concurrent goroutines.
// This method requires Aerospike Server version >= 3.6.0.
func (clnt *ProxyClient) BatchGetComplex(policy *BatchPolicy, records []*BatchRead) Error {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(records))
	for _, record := range records {
		batchRecordsIfc = append(batchRecordsIfc, record)
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
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
func (clnt *ProxyClient) BatchGetHeader(policy *BatchPolicy, keys []*Key) ([]*Record, Error) {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	for _, key := range keys {
		batchRecordsIfc = append(batchRecordsIfc, NewBatchReadHeader(key))
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
	records := make([]*Record, 0, len(keys))
	for i := range batchRecordsIfc {
		records = append(records, batchRecordsIfc[i].BatchRec().Record)
		// if nerr := batchRecordsIfc[i].BatchRec().Err; nerr != nil {
		// 	err = chainErrors(err, nerr)
		// }
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
func (clnt *ProxyClient) BatchDelete(policy *BatchPolicy, deletePolicy *BatchDeletePolicy, keys []*Key) ([]*BatchRecord, Error) {
	policy = clnt.getUsableBatchPolicy(policy)
	deletePolicy = clnt.getUsableBatchDeletePolicy(deletePolicy)

	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	batchRecords := make([]*BatchRecord, 0, len(keys))
	for _, key := range keys {
		batchDelete, batchRecord := newBatchDelete(deletePolicy, key)
		batchRecordsIfc = append(batchRecordsIfc, batchDelete)
		batchRecords = append(batchRecords, batchRecord)
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}
	return batchRecords, err
}

func (clnt *ProxyClient) batchOperate(policy *BatchPolicy, records []BatchRecordIfc) (int, Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	batchNode, err := newGrpcBatchOperateListIfc(policy, records)
	if err != nil && policy.RespondAllKeys {
		return 0, err
	}

	cmd := newBatchCommandOperate(nil, batchNode, policy, records)
	return cmd.filteredOutCnt, cmd.ExecuteGRPC(clnt)
}

// BatchOperate will read/write multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins for each key in the batch.
// The returned records are located in the same list.
//
// BatchRecord can be *BatchRead, *BatchWrite, *BatchDelete or *BatchUDF.
//
// Requires server version 6.0+
func (clnt *ProxyClient) BatchOperate(policy *BatchPolicy, records []BatchRecordIfc) Error {
	_, err := clnt.batchOperate(policy, records)
	return err
}

// BatchExecute will read/write multiple records for specified batch keys in one batch call.
// This method allows different namespaces/bins for each key in the batch.
// The returned records are located in the same list.
//
// BatchRecord can be *BatchRead, *BatchWrite, *BatchDelete or *BatchUDF.
//
// Requires server version 6.0+
func (clnt *ProxyClient) BatchExecute(policy *BatchPolicy, udfPolicy *BatchUDFPolicy, keys []*Key, packageName string, functionName string, args ...Value) ([]*BatchRecord, Error) {
	batchRecordsIfc := make([]BatchRecordIfc, 0, len(keys))
	batchRecords := make([]*BatchRecord, 0, len(keys))
	for _, key := range keys {
		batchUDF, batchRecord := newBatchUDF(udfPolicy, key, packageName, functionName, args...)
		batchRecordsIfc = append(batchRecordsIfc, batchUDF)
		batchRecords = append(batchRecords, batchRecord)
	}

	filteredOut, err := clnt.batchOperate(policy, batchRecordsIfc)
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return batchRecords, err
}

//-------------------------------------------------------
// Generic Database Operations
//-------------------------------------------------------

// Operate performs multiple read/write operations on a single key in one batch request.
// An example would be to add an integer value to an existing record and then
// read the result, all in one database call.
//
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, Error) {
	policy = clnt.getUsableWritePolicy(policy)
	args, err := newOperateArgs(nil, policy, key, operations)
	if err != nil {
		return nil, err
	}

	command, err := newOperateCommand(nil, policy, key, args)
	if err != nil {
		return nil, err
	}

	if err := command.ExecuteGRPC(clnt); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
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
func (clnt *ProxyClient) ScanPartitions(apolicy *ScanPolicy, partitionFilter *PartitionFilter, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	// result recordset
	tracker := newPartitionTracker(&policy.MultiPolicy, partitionFilter, nil)
	res := newRecordset(policy.RecordQueueSize, 1)
	cmd := newGrpcScanPartitionCommand(&policy, tracker, partitionFilter, namespace, setName, binNames, res)
	go cmd.ExecuteGRPC(clnt)

	return res, nil
}

// ScanAll reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) ScanAll(apolicy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	return clnt.ScanPartitions(apolicy, NewPartitionFilterAll(), namespace, setName, binNames...)
}

// scanNodePartitions reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) scanNodePartitions(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	panic("NOT SUPPORTED")
}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) ScanNode(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error) {
	panic("NOT SUPPORTED")
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
func (clnt *ProxyClient) RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, Error) {
	panic("NOT SUPPORTED")
}

// RegisterUDF registers a package containing user defined functions with server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, Error) {
	panic("NOT SUPPORTED")
}

// RemoveUDF removes a package containing user defined functions in the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RemoveTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) RemoveUDF(policy *WritePolicy, udfName string) (*RemoveTask, Error) {
	panic("NOT SUPPORTED")
}

// ListUDF lists all packages containing user defined functions in the server.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) ListUDF(policy *BasePolicy) ([]*UDF, Error) {
	panic("NOT SUPPORTED")
}

// Execute executes a user defined function on server and return results.
// The function operates on a single record.
// The package name is used to locate the udf file location:
//
// udf file = <server udf dir>/<package name>.lua
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (interface{}, Error) {
	policy = clnt.getUsableWritePolicy(policy)

	command, err := newExecuteCommand(nil, policy, key, packageName, functionName, NewValueArray(args))
	if err != nil {
		return nil, err
	}

	if err := command.ExecuteGRPC(clnt); err != nil {
		return nil, err
	}

	if rec := command.GetRecord(); rec != nil && rec.Bins != nil {
		return rec.Bins["SUCCESS"], nil
	}
	return nil, nil
}

//----------------------------------------------------------
// Query/Execute (Supported by Aerospike 3+ servers only)
//----------------------------------------------------------

// QueryExecute applies operations on records that match the statement filter.
// Records are not returned to the Grpcclient.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) QueryExecute(policy *QueryPolicy,
	writePolicy *WritePolicy,
	statement *Statement,
	ops ...*Operation,
) (*ExecuteTask, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	writePolicy = clnt.getUsableWritePolicy(writePolicy)

	command := newServerCommand(nil, policy, writePolicy, statement, statement.TaskId, ops)

	if err := command.ExecuteGRPC(clnt); err != nil {
		return nil, err
	}

	return newGRPCExecuteTask(clnt, statement), nil
}

// ExecuteUDF applies user defined function on records that match the statement filter.
// Records are not returned to the Grpcclient.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) ExecuteUDF(policy *QueryPolicy,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	wpolicy := clnt.getUsableWritePolicy(nil)

	nstatement := *statement
	nstatement.SetAggregateFunction(packageName, functionName, functionArgs, false)
	command := newServerCommand(nil, policy, wpolicy, &nstatement, nstatement.TaskId, nil)

	if err := command.ExecuteGRPC(clnt); err != nil {
		return nil, err
	}

	return newGRPCExecuteTask(clnt, &nstatement), nil
}

// ExecuteUDFNode applies user defined function on records that match the statement filter on the specified node.
// Records are not returned to the Grpcclient.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) ExecuteUDFNode(policy *QueryPolicy,
	node *Node,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, Error) {
	panic("NOT SUPPORTED")
}

// SetXDRFilter sets XDR filter for given datacenter name and namespace. The expression filter indicates
// which records XDR should ship to the datacenter.
// Pass nil as filter to remove the currentl filter on the server.
func (clnt *ProxyClient) SetXDRFilter(policy *InfoPolicy, datacenter string, namespace string, filter *Expression) Error {
	panic("NOT SUPPORTED")
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
func (clnt *ProxyClient) QueryPartitions(policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter) (*Recordset, Error) {
	policy = clnt.getUsableQueryPolicy(policy)
	// result recordset
	tracker := newPartitionTracker(&policy.MultiPolicy, partitionFilter, nil)
	res := newRecordset(policy.RecordQueueSize, 1)
	cmd := newGrpcQueryPartitionCommand(policy, nil, statement, nil, tracker, partitionFilter, res)
	go cmd.ExecuteGRPC(clnt)

	return res, nil
}

// Query executes a query and returns a Recordset.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop records off the channel through the
// Recordset.Records channel.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) Query(policy *QueryPolicy, statement *Statement) (*Recordset, Error) {
	return clnt.QueryPartitions(policy, statement, NewPartitionFilterAll())
}

// QueryNode executes a query on a specific node and returns a recordset.
// The caller can concurrently pop records off the channel through the
// record channel.
//
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) QueryNode(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error) {
	panic("NOT SUPPORTED")
}

func (clnt *ProxyClient) queryNodePartitions(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error) {
	panic("NOT SUPPORTED")
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
func (clnt *ProxyClient) CreateIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
) (*IndexTask, Error) {
	panic("NOT SUPPORTED")
}

// CreateComplexIndex creates a secondary index, with the ability to put indexes
// on bin containing complex data types, e.g: Maps and Lists.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) CreateComplexIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
	indexCollectionType IndexCollectionType,
	ctx ...*CDTContext,
) (*IndexTask, Error) {
	panic("NOT SUPPORTED")
}

// DropIndex deletes a secondary index. It will block until index is dropped on all nodes.
// This method is only supported by Aerospike 3+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *ProxyClient) DropIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
) Error {
	panic("NOT SUPPORTED")
}

// Truncate removes records in specified namespace/set efficiently.  This method is many orders of magnitude
// faster than deleting records one at a time.  Works with Aerospike Server versions >= 3.12.
// This asynchronous server call may return before the truncation is complete.  The user can still
// write new records after the server call returns because new records will have last update times
// greater than the truncate cutoff (set at the time of truncate call).
// For more information, See https://www.aerospike.com/docs/reference/info#truncate
func (clnt *ProxyClient) Truncate(policy *WritePolicy, namespace, set string, beforeLastUpdate *time.Time) Error {
	panic("NOT SUPPORTED")
}

//-------------------------------------------------------
// User administration
//-------------------------------------------------------

// CreateUser creates a new user with password and roles. Clear-text password will be hashed using bcrypt
// before sending to server.
func (clnt *ProxyClient) CreateUser(policy *AdminPolicy, user string, password string, roles []string) Error {
	panic("NOT SUPPORTED")
}

// DropUser removes a user from the cluster.
func (clnt *ProxyClient) DropUser(policy *AdminPolicy, user string) Error {
	panic("NOT SUPPORTED")
}

// ChangePassword changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
func (clnt *ProxyClient) ChangePassword(policy *AdminPolicy, user string, password string) Error {
	panic("NOT SUPPORTED")
}

// GrantRoles adds roles to user's list of roles.
func (clnt *ProxyClient) GrantRoles(policy *AdminPolicy, user string, roles []string) Error {
	panic("NOT SUPPORTED")
}

// RevokeRoles removes roles from user's list of roles.
func (clnt *ProxyClient) RevokeRoles(policy *AdminPolicy, user string, roles []string) Error {
	panic("NOT SUPPORTED")
}

// QueryUser retrieves roles for a given user.
func (clnt *ProxyClient) QueryUser(policy *AdminPolicy, user string) (*UserRoles, Error) {
	panic("NOT SUPPORTED")
}

// QueryUsers retrieves all users and their roles.
func (clnt *ProxyClient) QueryUsers(policy *AdminPolicy) ([]*UserRoles, Error) {
	panic("NOT SUPPORTED")
}

// QueryRole retrieves privileges for a given role.
func (clnt *ProxyClient) QueryRole(policy *AdminPolicy, role string) (*Role, Error) {
	panic("NOT SUPPORTED")
}

// QueryRoles retrieves all roles and their privileges.
func (clnt *ProxyClient) QueryRoles(policy *AdminPolicy) ([]*Role, Error) {
	panic("NOT SUPPORTED")
}

// CreateRole creates a user-defined role.
// Quotas require server security configuration "enable-quotas" to be set to true.
// Pass 0 for quota values for no limit.
func (clnt *ProxyClient) CreateRole(policy *AdminPolicy, roleName string, privileges []Privilege, whitelist []string, readQuota, writeQuota uint32) Error {
	panic("NOT SUPPORTED")
}

// DropRole removes a user-defined role.
func (clnt *ProxyClient) DropRole(policy *AdminPolicy, roleName string) Error {
	panic("NOT SUPPORTED")
}

// GrantPrivileges grant privileges to a user-defined role.
func (clnt *ProxyClient) GrantPrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error {
	panic("NOT SUPPORTED")
}

// RevokePrivileges revokes privileges from a user-defined role.
func (clnt *ProxyClient) RevokePrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error {
	panic("NOT SUPPORTED")
}

// SetWhitelist sets IP address whitelist for a role. If whitelist is nil or empty, it removes existing whitelist from role.
func (clnt *ProxyClient) SetWhitelist(policy *AdminPolicy, roleName string, whitelist []string) Error {
	panic("NOT SUPPORTED")
}

// SetQuotas sets maximum reads/writes per second limits for a role.  If a quota is zero, the limit is removed.
// Quotas require server security configuration "enable-quotas" to be set to true.
// Pass 0 for quota values for no limit.
func (clnt *ProxyClient) SetQuotas(policy *AdminPolicy, roleName string, readQuota, writeQuota uint32) Error {
	panic("NOT SUPPORTED")
}

//-------------------------------------------------------
// Access Methods
//-------------------------------------------------------

// Cluster exposes the cluster object to the user
func (clnt *ProxyClient) Cluster() *Cluster {
	panic("NOT SUPPORTED")
}

// String implements the Stringer interface for Grpcclient
func (clnt *ProxyClient) String() string {
	return ""
}

// Stats returns internal statistics regarding the inner state of the Grpcclient and the cluster.
func (clnt *ProxyClient) Stats() (map[string]interface{}, Error) {
	panic("NOT SUPPORTED")
}

// WarmUp fills the connection pool with connections for all nodes.
// This is necessary on startup for high traffic programs.
// If the count is <= 0, the connection queue will be filled.
// If the count is more than the size of the pool, the pool will be filled.
// Note: One connection per node is reserved for tend operations and is not used for transactions.
func (clnt *ProxyClient) WarmUp(count int) (int, Error) {
	if count <= 0 || count > clnt.clientPolicy.ConnectionQueueSize {
		count = clnt.clientPolicy.ConnectionQueueSize
	}

	for i := 0; i < count; i++ {
		conn, err := clnt.createGrpcConn(!clnt.clientPolicy.RequiresAuthentication())
		if err != nil {
			return i, err
		}
		clnt.returnGrpcConnToPool(conn)
	}

	return count, nil
}

//-------------------------------------------------------
// Internal Methods
//-------------------------------------------------------

func (clnt *ProxyClient) grpcMode() bool {
	return clnt.grpcConnPool != nil
}

//-------------------------------------------------------
// Policy Methods
//-------------------------------------------------------

func (clnt *ProxyClient) getUsablePolicy(policy *BasePolicy) *BasePolicy {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			return clnt.DefaultPolicy
		}
		return NewPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableBatchPolicy(policy *BatchPolicy) *BatchPolicy {
	if policy == nil {
		if clnt.DefaultBatchPolicy != nil {
			return clnt.DefaultBatchPolicy
		}
		return NewBatchPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableBaseBatchWritePolicy(policy *BatchPolicy) *BatchPolicy {
	if policy == nil {
		if clnt.DefaultBatchPolicy != nil {
			return clnt.DefaultBatchPolicy
		}
		return NewBatchPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableBatchWritePolicy(policy *BatchWritePolicy) *BatchWritePolicy {
	if policy == nil {
		if clnt.DefaultBatchWritePolicy != nil {
			return clnt.DefaultBatchWritePolicy
		}
		return NewBatchWritePolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableBatchDeletePolicy(policy *BatchDeletePolicy) *BatchDeletePolicy {
	if policy == nil {
		if clnt.DefaultBatchDeletePolicy != nil {
			return clnt.DefaultBatchDeletePolicy
		}
		return NewBatchDeletePolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableBatchUDFPolicy(policy *BatchUDFPolicy) *BatchUDFPolicy {
	if policy == nil {
		if clnt.DefaultBatchUDFPolicy != nil {
			return clnt.DefaultBatchUDFPolicy
		}
		return NewBatchUDFPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableWritePolicy(policy *WritePolicy) *WritePolicy {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			return clnt.DefaultWritePolicy
		}
		return NewWritePolicy(0, 0)
	}
	return policy
}

func (clnt *ProxyClient) getUsableScanPolicy(policy *ScanPolicy) *ScanPolicy {
	if policy == nil {
		if clnt.DefaultScanPolicy != nil {
			return clnt.DefaultScanPolicy
		}
		return NewScanPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableQueryPolicy(policy *QueryPolicy) *QueryPolicy {
	if policy == nil {
		if clnt.DefaultQueryPolicy != nil {
			return clnt.DefaultQueryPolicy
		}
		return NewQueryPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableAdminPolicy(policy *AdminPolicy) *AdminPolicy {
	if policy == nil {
		if clnt.DefaultAdminPolicy != nil {
			return clnt.DefaultAdminPolicy
		}
		return NewAdminPolicy()
	}
	return policy
}

func (clnt *ProxyClient) getUsableInfoPolicy(policy *InfoPolicy) *InfoPolicy {
	if policy == nil {
		if clnt.DefaultInfoPolicy != nil {
			return clnt.DefaultInfoPolicy
		}
		return NewInfoPolicy()
	}
	return policy
}

//-------------------------------------------------------
// Utility Functions
//-------------------------------------------------------

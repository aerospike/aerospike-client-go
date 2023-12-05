//go:build !as_performance && !app_engine
// +build !as_performance,!app_engine

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
	"time"
)

// ClientIfc abstracts an Aerospike cluster.
type ClientIfc interface {
	Add(policy *WritePolicy, key *Key, binMap BinMap) Error
	AddBins(policy *WritePolicy, key *Key, bins ...*Bin) Error
	Append(policy *WritePolicy, key *Key, binMap BinMap) Error
	AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) Error
	BatchDelete(policy *BatchPolicy, deletePolicy *BatchDeletePolicy, keys []*Key) ([]*BatchRecord, Error)
	BatchExecute(policy *BatchPolicy, udfPolicy *BatchUDFPolicy, keys []*Key, packageName string, functionName string, args ...Value) ([]*BatchRecord, Error)
	BatchExists(policy *BatchPolicy, keys []*Key) ([]bool, Error)
	BatchGet(policy *BatchPolicy, keys []*Key, binNames ...string) ([]*Record, Error)
	BatchGetComplex(policy *BatchPolicy, records []*BatchRead) Error
	BatchGetHeader(policy *BatchPolicy, keys []*Key) ([]*Record, Error)
	BatchGetOperate(policy *BatchPolicy, keys []*Key, ops ...*Operation) ([]*Record, Error)
	BatchOperate(policy *BatchPolicy, records []BatchRecordIfc) Error
	ChangePassword(policy *AdminPolicy, user string, password string) Error
	Close()
	Cluster() *Cluster
	CreateComplexIndex(policy *WritePolicy, namespace string, setName string, indexName string, binName string, indexType IndexType, indexCollectionType IndexCollectionType, ctx ...*CDTContext) (*IndexTask, Error)
	CreateIndex(policy *WritePolicy, namespace string, setName string, indexName string, binName string, indexType IndexType) (*IndexTask, Error)
	CreateRole(policy *AdminPolicy, roleName string, privileges []Privilege, whitelist []string, readQuota, writeQuota uint32) Error
	CreateUser(policy *AdminPolicy, user string, password string, roles []string) Error
	Delete(policy *WritePolicy, key *Key) (bool, Error)
	DropIndex(policy *WritePolicy, namespace string, setName string, indexName string) Error
	DropRole(policy *AdminPolicy, roleName string) Error
	DropUser(policy *AdminPolicy, user string) Error
	Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (interface{}, Error)
	ExecuteUDF(policy *QueryPolicy, statement *Statement, packageName string, functionName string, functionArgs ...Value) (*ExecuteTask, Error)
	ExecuteUDFNode(policy *QueryPolicy, node *Node, statement *Statement, packageName string, functionName string, functionArgs ...Value) (*ExecuteTask, Error)
	Exists(policy *BasePolicy, key *Key) (bool, Error)
	Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, Error)
	GetHeader(policy *BasePolicy, key *Key) (*Record, Error)
	GetNodeNames() []string
	GetNodes() []*Node
	GrantPrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error
	GrantRoles(policy *AdminPolicy, user string, roles []string) Error
	IsConnected() bool
	ListUDF(policy *BasePolicy) ([]*UDF, Error)
	Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, Error)
	Prepend(policy *WritePolicy, key *Key, binMap BinMap) Error
	PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) Error
	Put(policy *WritePolicy, key *Key, binMap BinMap) Error
	PutBins(policy *WritePolicy, key *Key, bins ...*Bin) Error
	Query(policy *QueryPolicy, statement *Statement) (*Recordset, Error)
	QueryExecute(policy *QueryPolicy, writePolicy *WritePolicy, statement *Statement, ops ...*Operation) (*ExecuteTask, Error)
	QueryNode(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error)
	queryNodePartitions(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, Error)
	QueryPartitions(policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter) (*Recordset, Error)
	QueryRole(policy *AdminPolicy, role string) (*Role, Error)
	QueryRoles(policy *AdminPolicy) ([]*Role, Error)
	QueryUser(policy *AdminPolicy, user string) (*UserRoles, Error)
	QueryUsers(policy *AdminPolicy) ([]*UserRoles, Error)
	RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, Error)
	RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, Error)
	RemoveUDF(policy *WritePolicy, udfName string) (*RemoveTask, Error)
	RevokePrivileges(policy *AdminPolicy, roleName string, privileges []Privilege) Error
	RevokeRoles(policy *AdminPolicy, user string, roles []string) Error
	ScanAll(apolicy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, Error)
	ScanNode(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, Error)
	ScanPartitions(apolicy *ScanPolicy, partitionFilter *PartitionFilter, namespace string, setName string, binNames ...string) (*Recordset, Error)
	SetQuotas(policy *AdminPolicy, roleName string, readQuota, writeQuota uint32) Error
	SetWhitelist(policy *AdminPolicy, roleName string, whitelist []string) Error
	SetXDRFilter(policy *InfoPolicy, datacenter string, namespace string, filter *Expression) Error
	Stats() (map[string]interface{}, Error)
	String() string
	Touch(policy *WritePolicy, key *Key) Error
	Truncate(policy *InfoPolicy, namespace, set string, beforeLastUpdate *time.Time) Error
	WarmUp(count int) (int, Error)

	BatchGetObjects(policy *BatchPolicy, keys []*Key, objects []interface{}) (found []bool, err Error)
	GetObject(policy *BasePolicy, key *Key, obj interface{}) Error
	PutObject(policy *WritePolicy, key *Key, obj interface{}) (err Error)
	QueryAggregate(policy *QueryPolicy, statement *Statement, packageName, functionName string, functionArgs ...Value) (*Recordset, Error)
	QueryNodeObjects(policy *QueryPolicy, node *Node, statement *Statement, objChan interface{}) (*Recordset, Error)
	QueryObjects(policy *QueryPolicy, statement *Statement, objChan interface{}) (*Recordset, Error)
	QueryPartitionObjects(policy *QueryPolicy, statement *Statement, objChan interface{}, partitionFilter *PartitionFilter) (*Recordset, Error)
	ScanAllObjects(apolicy *ScanPolicy, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, Error)
	ScanNodeObjects(apolicy *ScanPolicy, node *Node, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, Error)
	ScanPartitionObjects(apolicy *ScanPolicy, objChan interface{}, partitionFilter *PartitionFilter, namespace string, setName string, binNames ...string) (*Recordset, Error)

	// TODO: Synchronization here for the sake of dynamic config in the future

	GetDefaultPolicy() *BasePolicy
	GetDefaultBatchPolicy() *BatchPolicy
	GetDefaultBatchWritePolicy() *BatchWritePolicy
	GetDefaultBatchDeletePolicy() *BatchDeletePolicy
	GetDefaultBatchUDFPolicy() *BatchUDFPolicy
	GetDefaultWritePolicy() *WritePolicy
	GetDefaultScanPolicy() *ScanPolicy
	GetDefaultQueryPolicy() *QueryPolicy
	GetDefaultAdminPolicy() *AdminPolicy
	GetDefaultInfoPolicy() *InfoPolicy

	SetDefaultPolicy(*BasePolicy)
	SetDefaultBatchPolicy(*BatchPolicy)
	SetDefaultBatchWritePolicy(*BatchWritePolicy)
	SetDefaultBatchDeletePolicy(*BatchDeletePolicy)
	SetDefaultBatchUDFPolicy(*BatchUDFPolicy)
	SetDefaultWritePolicy(*WritePolicy)
	SetDefaultScanPolicy(*ScanPolicy)
	SetDefaultQueryPolicy(*QueryPolicy)
	SetDefaultAdminPolicy(*AdminPolicy)
	SetDefaultInfoPolicy(*InfoPolicy)
}

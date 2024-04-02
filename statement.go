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
	"fmt"

	kvs "github.com/aerospike/aerospike-client-go/v6/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v6/types"
	xornd "github.com/aerospike/aerospike-client-go/v6/types/rand"
)

// Statement encapsulates query statement parameters.
type Statement struct {
	// Namespace determines query Namespace
	Namespace string

	// SetName determines query Set name (Optional)
	SetName string

	// IndexName determines query index name (Optional)
	// If not set, the server will determine the index from the filter's bin name.
	IndexName string

	// BinNames determines bin names (optional)
	BinNames []string

	// Filter determines query index filter (Optional).
	// This filter is applied to the secondary index on query.
	// Query index filters must reference a bin which has a secondary index defined.
	Filter *Filter

	packageName  string
	functionName string
	functionArgs []Value

	// TaskId determines query task id. (Optional)
	// This value is not used anymore and will be removed later.
	TaskId uint64

	// determines if the query should return data
	returnData bool
}

// NewStatement initializes a new Statement instance.
func NewStatement(ns string, set string, binNames ...string) *Statement {
	return &Statement{
		Namespace:  ns,
		SetName:    set,
		BinNames:   binNames,
		returnData: true,
		TaskId:     xornd.Uint64(),
	}
}

func (stmt *Statement) String() string {
	return fmt.Sprintf("Statement: {Namespace: %s, set: %s, IndexName: %s, BinNames: %v, Filter: %s, UDF: %s.%s(%v), TaskId: %d, return data: %v}",
		stmt.Namespace,
		stmt.SetName,
		stmt.IndexName,
		stmt.BinNames,
		stmt.Filter,
		stmt.packageName,
		stmt.functionName,
		stmt.functionArgs,
		stmt.TaskId, stmt.returnData,
	)
}

// SetFilter Sets a filter for the statement.
// Aerospike Server currently only supports using a single filter per statement/query.
func (stmt *Statement) SetFilter(filter *Filter) Error {
	stmt.Filter = filter

	return nil
}

// SetAggregateFunction sets aggregation function parameters.
// This function will be called on both the server
// and client for each selected item.
func (stmt *Statement) SetAggregateFunction(packageName string, functionName string, functionArgs []Value, returnData bool) {
	stmt.packageName = packageName
	stmt.functionName = functionName
	stmt.functionArgs = functionArgs
	stmt.returnData = returnData
}

// IsScan determines is the Statement is a full namespace/set scan or a selective Query.
func (stmt *Statement) IsScan() bool {
	return stmt.Filter == nil
}

func (stmt *Statement) terminationError() types.ResultCode {
	if stmt.IsScan() {
		return types.SCAN_TERMINATED
	}
	return types.QUERY_TERMINATED
}

// Always set the taskID client-side to a non-zero random value
func (stmt *Statement) prepare(returnData bool) {
	stmt.returnData = returnData
}

func (stmt *Statement) grpc(policy *QueryPolicy, ops []*Operation) *kvs.Statement {
	IndexName := stmt.IndexName
	// reset taskID every time
	TaskId := xornd.Int64()
	SetName := stmt.SetName

	MaxRecords := uint64(policy.MaxRecords)
	RecordsPerSecond := uint32(policy.RecordsPerSecond)

	funcArgs := make([][]byte, 0, len(stmt.functionArgs))
	for i := range stmt.functionArgs {
		funcArgs = append(funcArgs, grpcValuePacked(stmt.functionArgs[i]))
	}

	return &kvs.Statement{
		Namespace:        stmt.Namespace,
		SetName:          &SetName,
		IndexName:        &IndexName,
		BinNames:         stmt.BinNames,
		Filter:           stmt.Filter.grpc(),
		PackageName:      stmt.packageName,
		FunctionName:     stmt.functionName,
		FunctionArgs:     funcArgs,
		Operations:       grpcOperations(ops),
		TaskId:           &TaskId,
		MaxRecords:       &MaxRecords,
		RecordsPerSecond: &RecordsPerSecond,
	}
}

func grpcOperations(ops []*Operation) []*kvs.Operation {
	res := make([]*kvs.Operation, 0, len(ops))
	for i := range ops {
		res = append(res, ops[i].grpc())
	}
	return res
}

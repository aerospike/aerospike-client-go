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
	"math/rand"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/types"
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

	// BinNames determines for bin projections in queries (Optional).
	// Mutually exclusive with Operations.
	BinNames []string

	// Operations to be performed on query/execute.
	// For foreground queries (Query), only read operations are allowed.
	// For background queries (QueryExecute), only write operations are allowed.
	// Mutually exclusive with BinNames.
	Operations []*Operation

	// Filter determines query index filter (Optional).
	// This filter is applied to the secondary index on query.
	// Query index filters must reference a bin which has a secondary index defined.
	Filter *Filter

	packageName  string
	functionName string
	functionArgs []Value

	// TaskId determines query task id. (Optional)
	// This value is not used anymore and will be removed later.
	// Deprecated: Statement instances should not set TaskId directly since instances are meant to be immutable.
	TaskId uint64

	// determines if the query should return data
	ReturnData bool

	// orderBy specifies the ordering criteria for the Top-K request.
	orderBy *orderBySpec
	// topK specifies the maximum number of results to return.
	topK int
	// topKSet indicates whether the Top-K limit was explicitly set.
	topKSet bool
}

// OrderByType is the particle type a Top-K order-by bin is compared as.
// A record whose bin is missing or has another type ranks last.
type OrderByType byte

const (
	// OrderByInteger compares the bin as an integer.
	OrderByInteger OrderByType = 1
	// OrderByDouble compares the bin as a float. NaN ranks above every number.
	OrderByDouble OrderByType = 2
	// OrderByString compares the bin as a string, byte by byte.
	OrderByString OrderByType = 3
	// OrderByBytes compares the bin as a blob, byte by byte.
	OrderByBytes OrderByType = 4
)

// OrderDirection is the sort direction of a Top-K order-by bin.
type OrderDirection byte

const (
	// OrderAsc ranks the smallest value first.
	OrderAsc OrderDirection = 0
	// OrderDesc ranks the largest value first.
	OrderDesc OrderDirection = 1
)

// OrderByFlags modify how a Top-K order-by bin is compared.
type OrderByFlags byte

const (
	// OrderByFlagNone uses the default comparison.
	OrderByFlagNone OrderByFlags = 0
	// OrderByFlagCaseInsensitive ignores ASCII letter case. Valid for OrderByString only.
	OrderByFlagCaseInsensitive OrderByFlags = 1
)

type orderBySpec struct {
	binName   string
	orderType OrderByType
	direction OrderDirection
	flags     OrderByFlags
}

// NewStatement initializes a new Statement instance.
func NewStatement(ns string, set string, binNames ...string) *Statement {
	return &Statement{
		Namespace:  ns,
		SetName:    set,
		BinNames:   binNames,
		ReturnData: true,
		TaskId:     0,
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
		stmt.TaskId,
		stmt.ReturnData,
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
	stmt.ReturnData = returnData
}

// SetOrderBy sets the bin a Top-K query ranks records by. SetOrderBy alone
// does nothing; call SetTopK after it. Records tied on the bin are ranked by
// digest. It replaces an earlier order-by bin and clears an earlier SetTopK,
// so SetTopK must be called again.
//
// The bin must be returned by the query: listed in BinNames, written by one
// of the Operations (for example an ExpReadOp projecting a vector distance),
// or neither set. Requires a server with Top-K support.
func (stmt *Statement) SetOrderBy(binName string, orderType OrderByType, direction OrderDirection) Error {
	return stmt.SetOrderByWithFlags(binName, orderType, direction, OrderByFlagNone)
}

// SetOrderByWithFlags is SetOrderBy with comparison flags, such as
// OrderByFlagCaseInsensitive for an OrderByString bin.
func (stmt *Statement) SetOrderByWithFlags(binName string, orderType OrderByType, direction OrderDirection, flags OrderByFlags) Error {
	stmt.orderBy = &orderBySpec{binName, orderType, direction, flags}
	stmt.topKSet = false
	return nil
}

// SetTopK makes the query return only the k best records, ranked by the
// order-by bin, best first. k must be in [1, 1000]. SetOrderBy must be
// called first.
//
// Top-K is supported by Query, QueryPartitions and QueryNode only, with
// IncludeBinData set and MaxRecords 0. Each node ranks its records and sends
// its k best when its scan ends, so no records arrive until then; the client
// merges the nodes' results. If any node lacks Top-K support, the client ranks
// all matching records itself.
//
// Top-K is single-shot and always runs as a long query; a short-query hint is
// ignored. Pass a fresh PartitionFilter: a filter reused from an earlier query
// ranks only the records after its resume point.
func (stmt *Statement) SetTopK(k int) Error {
	if stmt.orderBy == nil {
		return newError(types.PARAMETER_ERROR, "SetTopK requires SetOrderBy to be called first")
	}
	stmt.topK = k
	stmt.topKSet = true
	return nil
}

// hasTopK reports whether the statement is a Top-K query.
func (stmt *Statement) hasTopK() bool {
	return stmt.topKSet
}

// rejectTopK refuses a Top-K statement on a command that does not support it.
func (stmt *Statement) rejectTopK() Error {
	if stmt.hasTopK() {
		return newError(types.PARAMETER_ERROR, "Top-K is only supported by Query, QueryPartitions and QueryNode")
	}
	return nil
}

// validateTopK checks a Top-K statement against the query policy before any
// node is contacted.
func (stmt *Statement) validateTopK(policy *QueryPolicy) Error {
	if !stmt.hasTopK() {
		return nil
	}

	spec := stmt.orderBy
	if len(spec.binName) == 0 || len(spec.binName) > 15 || strings.IndexByte(spec.binName, 0) >= 0 {
		return newError(types.PARAMETER_ERROR, "Top-K order-by bin name must be 1-15 bytes without a null byte")
	}
	if spec.orderType < OrderByInteger || spec.orderType > OrderByBytes {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid OrderByType: %d", spec.orderType))
	}
	if spec.direction != OrderAsc && spec.direction != OrderDesc {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid OrderDirection: %d", spec.direction))
	}
	if spec.flags&^OrderByFlagCaseInsensitive != 0 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid OrderByFlags: %d", spec.flags))
	}
	if spec.flags != OrderByFlagNone && spec.orderType != OrderByString {
		return newError(types.PARAMETER_ERROR, "Top-K order-by flags are only valid for OrderByString")
	}
	if !stmt.hasProjectedBin(spec.binName) {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Top-K order-by bin %s must be returned by the query's BinNames or Operations", spec.binName))
	}

	if stmt.topK < 1 || stmt.topK > 1000 {
		return newError(types.PARAMETER_ERROR, "Top-K limit must be in [1, 1000]")
	}
	if stmt.functionName != "" {
		return newError(types.PARAMETER_ERROR, "Top-K is incompatible with an aggregate function")
	}
	if policy.MaxRecords != 0 {
		return newError(types.PARAMETER_ERROR, "Top-K is incompatible with MaxRecords")
	}
	if !policy.IncludeBinData {
		return newError(types.PARAMETER_ERROR, "Top-K is incompatible with IncludeBinData=false")
	}
	return nil
}

// hasProjectedBin reports whether the query returns binName. With no BinNames
// or Operations the whole record is returned, so every bin counts as projected.
// When Operations are set they decide the returned bins and BinNames is ignored.
func (stmt *Statement) hasProjectedBin(binName string) bool {
	if len(stmt.Operations) > 0 {
		for _, op := range stmt.Operations {
			if op.binName == binName {
				return true
			}
		}
		return false
	}
	if len(stmt.BinNames) > 0 {
		for _, name := range stmt.BinNames {
			if name == binName {
				return true
			}
		}
		return false
	}
	return true
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
	stmt.ReturnData = returnData
}

func (stmt *Statement) prepareTaskId() uint64 {
	// If TaskId is 0, set it to a new random value and return the same statement.
	// This also means that that the taskId was never set by the user.
	// If TaskId is non-zero, it means that the user set it manually.
	// This is important because the user might want to reuse the same statement for multiple queries.
	// However the control of the taskId is now with the client library and not the user.
	// Important to remember is that the server will reject queries that have already been executed and the result is not
	// available yet.
	//
	// The statement.TaskId is deprecated and will be removed/revamped in future major release.
	if stmt.TaskId == 0 {
		taskId := rand.Uint64()

		return taskId
	}

	return stmt.TaskId
}

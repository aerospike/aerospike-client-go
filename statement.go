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

	// orderBy is the Top-K order-by bin set by SetOrderBy.
	orderBy    orderBySpec
	orderBySet bool
	// thenOrderBy is the optional second order-by bin set by ThenOrderBy,
	// used only for records that tie on orderBy.
	thenOrderBy    orderBySpec
	hasThenOrderBy bool
	// topK specifies the maximum number of results to return.
	topK int
	// topKSet indicates whether the Top-K limit was explicitly set.
	topKSet bool
}

// BinDataType is the particle type a Top-K order-by bin is compared as.
// A record whose bin is missing or has another type ranks last.
type BinDataType byte

const (
	// BinDataTypeInteger compares the bin as an integer.
	BinDataTypeInteger BinDataType = 1
	// BinDataTypeDouble compares the bin as a float. NaN ranks above every
	// number, and -0.0 equals 0.0.
	BinDataTypeDouble BinDataType = 2
	// BinDataTypeString compares the bin as a string, byte by byte. On a
	// common prefix, the shorter string comes first.
	BinDataTypeString BinDataType = 3
	// BinDataTypeBytes compares the bin as a blob, byte by byte. On a common
	// prefix, the shorter blob comes first.
	BinDataTypeBytes BinDataType = 4
)

// OrderDirection is the sort direction of a Top-K order-by bin.
type OrderDirection byte

const (
	// OrderAsc ranks the smallest value first.
	OrderAsc OrderDirection = 0
	// OrderDesc ranks the largest value first.
	OrderDesc OrderDirection = 1
)

// OrderByFlags is a bitmask that controls how Top-K order-by values are compared.
type OrderByFlags byte

const (
	// OrderByFlagNone compares values as they are.
	OrderByFlagNone OrderByFlags = 0
	// OrderByFlagCaseInsensitive ignores the case of the ASCII letters A–Z;
	// other characters compare as they are. Valid only with BinDataTypeString.
	OrderByFlagCaseInsensitive OrderByFlags = 1 << 0
)

type orderBySpec struct {
	binName   string
	orderType BinDataType
	direction OrderDirection
	flags     OrderByFlags
}

// wireSize returns the spec's size in bytes in the ORDER_BY field.
func (spec *orderBySpec) wireSize() int {
	return 4 + len(spec.binName)
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

// SetOrderBy sets the bin a Top-K query ranks records by. SetTopK sets how
// many records to return. Records that tie are ordered by the ThenOrderBy
// bin, if set, then by digest.
//
// Calling SetOrderBy again replaces both order-by bins and keeps k. Without
// SetTopK, the server rejects the query.
//
// The bin must be returned by the query: listed in BinNames, written by
// exactly one of the Operations (for example an ExpReadOp projecting a vector
// distance), or neither set. If an ExpReadOp can't compute the bin for a
// record, the record is left out; with ExpReadFlagEvalNoFail it ranks last.
//
// Every node in the cluster must support Top-K; otherwise the query returns a
// PARAMETER_ERROR before anything is sent.
func (stmt *Statement) SetOrderBy(binName string, orderType BinDataType, direction OrderDirection) Error {
	return stmt.SetOrderByWithFlags(binName, orderType, direction, OrderByFlagNone)
}

// SetOrderByWithFlags is SetOrderBy with comparison flags for this bin, such
// as OrderByFlagCaseInsensitive for a BinDataTypeString bin.
func (stmt *Statement) SetOrderByWithFlags(binName string, orderType BinDataType, direction OrderDirection, flags OrderByFlags) Error {
	stmt.orderBy = orderBySpec{binName, orderType, direction, flags}
	stmt.orderBySet = true
	stmt.thenOrderBy = orderBySpec{}
	stmt.hasThenOrderBy = false
	return nil
}

// ThenOrderBy sets a second Top-K order-by bin, used only to order records
// that tie on the SetOrderBy bin.
//
// Call it once, after SetOrderBy; otherwise it returns a PARAMETER_ERROR.
// Calling SetOrderBy again removes it. The bin must be returned by the query,
// as for SetOrderBy.
func (stmt *Statement) ThenOrderBy(binName string, orderType BinDataType, direction OrderDirection) Error {
	return stmt.ThenOrderByWithFlags(binName, orderType, direction, OrderByFlagNone)
}

// ThenOrderByWithFlags is ThenOrderBy with comparison flags for this bin,
// such as OrderByFlagCaseInsensitive for a BinDataTypeString bin.
func (stmt *Statement) ThenOrderByWithFlags(binName string, orderType BinDataType, direction OrderDirection, flags OrderByFlags) Error {
	if !stmt.orderBySet {
		return newError(types.PARAMETER_ERROR, "ThenOrderBy requires SetOrderBy to be called first")
	}
	if stmt.hasThenOrderBy {
		return newError(types.PARAMETER_ERROR, "order-by names more than 2 bins")
	}
	stmt.thenOrderBy = orderBySpec{binName, orderType, direction, flags}
	stmt.hasThenOrderBy = true
	return nil
}

// SetTopK limits the query to the k best records by the order-by bins,
// returned best first. Call it after SetOrderBy. k must be in [1, 1000].
// Calling SetTopK again replaces k.
//
// Supported by Query, QueryPartitions and QueryNode only. Requires every node
// in the cluster to support Top-K, IncludeBinData=true and MaxRecords=0.
// QueryNode ranks only its own records.
//
// Results are returned after nodes finish scanning. For large scans, set
// SocketTimeout high enough to cover the scan duration, or to 0.
//
// Top-K is single-shot and always runs as a long query. QueryPartitions
// returns a PARAMETER_ERROR for a PartitionFilter that carries resume points
// from an earlier query; use a fresh one.
func (stmt *Statement) SetTopK(k int) Error {
	if !stmt.hasOrderBy() {
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

// hasOrderBy reports whether SetOrderBy was called.
func (stmt *Statement) hasOrderBy() bool {
	return stmt.orderBySet
}

// rejectTopK refuses a statement with Top-K settings on a command that does
// not support them.
func (stmt *Statement) rejectTopK() Error {
	if stmt.hasTopK() || stmt.hasOrderBy() {
		return newError(types.PARAMETER_ERROR, "Top-K is only supported by Query, QueryPartitions and QueryNode")
	}
	return nil
}

// validateTopK checks a Top-K statement against the query policy before any
// node is contacted. An order-by without SetTopK is left to the server.
func (stmt *Statement) validateTopK(policy *QueryPolicy) Error {
	if !stmt.hasTopK() {
		return nil
	}

	if err := stmt.validateOrderBy(stmt.orderBy, "Top-K order-by"); err != nil {
		return err
	}
	if stmt.hasThenOrderBy {
		if err := stmt.validateOrderBy(stmt.thenOrderBy, "Top-K then-order-by"); err != nil {
			return err
		}
		if stmt.thenOrderBy.binName == stmt.orderBy.binName {
			return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by names bin '%s' more than once", stmt.orderBy.binName))
		}
	}

	if stmt.topK < 1 || stmt.topK > 1000 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("top-k limit %d out of range - must be 1 to 1000", stmt.topK))
	}
	if stmt.functionName != "" {
		return newError(types.PARAMETER_ERROR, "top-k/order-by only valid on foreground (basic) queries")
	}
	if policy.MaxRecords != 0 {
		return newError(types.PARAMETER_ERROR, "top-k incompatible with max-records/sample-max")
	}
	if !policy.IncludeBinData {
		return newError(types.PARAMETER_ERROR, "top-k incompatible with no-bin-data (metadata-only) queries")
	}
	return nil
}

// validateTopKCluster refuses a statement with Top-K settings before anything
// is sent when any node in the cluster lacks Top-K support.
func (stmt *Statement) validateTopKCluster(cluster *Cluster) Error {
	if stmt.hasOrderBy() && !cluster.supportsQueryOrderBy.Get() {
		return newError(types.PARAMETER_ERROR, "Top-K is not supported by all nodes in the cluster")
	}
	return nil
}

// validateOrderBy checks one order-by bin of a Top-K statement. label names
// the bin in an error that only the client reports.
func (stmt *Statement) validateOrderBy(spec orderBySpec, label string) Error {
	if len(spec.binName) == 0 || len(spec.binName) > 15 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by bin name length %d invalid - must be 1 to 15", len(spec.binName)))
	}
	if strings.IndexByte(spec.binName, 0) >= 0 {
		return newError(types.PARAMETER_ERROR, "order-by bin name is not a valid bin name")
	}
	if spec.orderType < BinDataTypeInteger || spec.orderType > BinDataTypeBytes {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by type %d invalid - must be 1 (integer), 2 (double), 3 (string) or 4 (bytes)", spec.orderType))
	}
	if spec.direction != OrderAsc && spec.direction != OrderDesc {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by direction %d invalid - must be 0 (ascending) or 1 (descending)", spec.direction))
	}
	if spec.flags&^OrderByFlagCaseInsensitive != 0 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by flags 0x%x invalid - only 0x1 (case-insensitive) is defined", spec.flags))
	}
	if spec.flags != OrderByFlagNone && spec.orderType != BinDataTypeString {
		return newError(types.PARAMETER_ERROR, "order-by case-insensitive flag applies only to string ordering")
	}
	if !stmt.hasProjectedBin(spec.binName) {
		if len(stmt.Operations) > 0 {
			return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by bin '%s' is not produced by any of the query's read ops", spec.binName))
		}
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("order-by bin '%s' is not in the query's bin list - add it to the bins selected", spec.binName))
	}
	if stmt.operationsReturning(spec.binName) > 1 {
		return newError(types.PARAMETER_ERROR, label+" bin must be produced by exactly one projection operation")
	}
	return nil
}

// operationsReturning counts the operations that return binName.
func (stmt *Statement) operationsReturning(binName string) int {
	count := 0
	for _, op := range stmt.Operations {
		if op.binName == binName {
			count++
		}
	}
	return count
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

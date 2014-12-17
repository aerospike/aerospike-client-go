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

// Statement encapsulates query statement parameters.
type Statement struct {
	// Namespace determines query Namespace
	Namespace string

	// SetName determines query Set name (Optional)
	SetName string

	// IndexName determines query index name (Optional)
	// If not set, the server will determine the index from the filter's bin name.
	IndexName string

	// BinNames detemines bin names (optional)
	BinNames []string

	// Filters determine query filters (Optional)
	// Currently, only one filter is allowed by the server on a secondary index lookup.
	// If multiple filters are necessary, see QueryFilter example for a workaround.
	// QueryFilter demonstrates how to add additional filters in an user-defined
	// aggregation function.
	Filters []*Filter

	packageName  string
	functionName string
	functionArgs []Value

	// TaskId determines query task id. (Optional)
	TaskId int64

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
	}
}

// Addfilter adds a filter to the statement.
func (stmt *Statement) Addfilter(filter *Filter) error {
	stmt.Filters = append(stmt.Filters, filter)

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
	return stmt.Filters == nil
}

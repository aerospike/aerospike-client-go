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

/**
 * Query statement parameters.
 */
type Statement struct {
	namespace    string
	setName      string
	indexName    string
	binNames     []string
	filters      []*Filter
	packageName  string
	functionName string
	functionArgs []Value
	taskId       int
	returnData   bool
}

func NewStatement(ns string, set string, binNames ...string) *Statement {
	return &Statement{
		namespace:  ns,
		setName:    set,
		binNames:   binNames,
		returnData: true,
	}
}

/**
 * Set query namespace.
 */
func (this *Statement) SetNamespace(namespace string) {
	this.namespace = namespace
}

/**
 * Set optional query setname.
 */
func (this *Statement) SetSetName(setName string) {
	this.setName = setName
}

/**
 * Set optional query index name.  If not set, the server
 * will determine the index from the filter's bin name.
 */
func (this *Statement) SetIndexName(indexName string) {
	this.indexName = indexName
}

/**
 * Set query bin names.
 */
func (this *Statement) SetBinNames(binNames ...string) {
	this.binNames = binNames
}

/**
 * Set optional query filters.
 * Currently, only one filter is allowed by the server on a secondary index lookup.
 * If multiple filters are necessary, see QueryFilter example for a workaround.
 * QueryFilter demonstrates how to add additional filters in an user-defined
 * aggregation function.
 */
func (this *Statement) SetFilters(filters ...*Filter) {
	this.filters = filters
}

/**
 * Set optional query task id.
 */
func (this *Statement) SetTaskId(taskId int) {
	this.taskId = taskId
}

/**
 * Set Lua aggregation function parameters.  This function will be called on both the server
 * and client for each selected item.
 *
 * @param packageName     server package where user defined function resides
 * @param functionName      aggregation function name
 * @param functionArgs      arguments to pass to function name, if any
 */
func (this *Statement) SetAggregateFunction(packageName string, functionName string, functionArgs []Value, returnData bool) {
	this.packageName = packageName
	this.functionName = functionName
	this.functionArgs = functionArgs
	this.returnData = returnData
}

/**
 * Return if full namespace/set scan is specified.
 */
func (this *Statement) IsScan() bool {
	return this.filters == nil
}

/**
 * Return query filters.
 */
func (this *Statement) GetFilters() []*Filter {
	return this.filters
}

/**
 * Return aggregation file name.
 */
func (this *Statement) GetPackageName() string {
	return this.packageName
}

/**
 * Return aggregation function name.
 */
func (this *Statement) GetFunctionName() string {
	return this.functionName
}

/**
 * Return aggregation function arguments.
 */
func (this *Statement) GetFunctionArgs() []Value {
	return this.functionArgs
}

/**
 * Return task ID.
 */
func (this *Statement) GetTaskId() int {
	return this.taskId
}

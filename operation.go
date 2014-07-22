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

// Aerospike operation type
type OperationType int

var (
	READ        OperationType = 1
	READ_HEADER OperationType = 1
	WRITE       OperationType = 2
	ADD         OperationType = 5
	APPEND      OperationType = 9
	PREPEND     OperationType = 10
	TOUCH       OperationType = 11
)

// Database operation definition.  The class is used in client's operate() method.
type Operation struct {

	// Type of operation.
	OpType OperationType

	// Optional bin name used in operation.
	BinName *string

	// Optional bin value used in operation.
	BinValue Value
}

// Create read bin database operation.
func NewGetOpByBinName(binName string) *Operation {
	return &Operation{OpType: READ, BinName: &binName}
}

// Create read all record bins database operation.
func NewGetOp() *Operation {
	return &Operation{OpType: READ}
}

// Create read record header database operation.
func NewGetHeaderOp() *Operation {
	return &Operation{OpType: READ_HEADER}
}

// Create set database operation.
func NewPutOp(bin Bin) *Operation {
	return &Operation{OpType: WRITE, BinValue: bin.Value}
}

// Create string append database operation.
func NewAppendOp(bin Bin) *Operation {
	return &Operation{OpType: APPEND, BinValue: bin.Value}
}

// Create string prepend database operation.
func NewPrependOp(bin Bin) *Operation {
	return &Operation{OpType: PREPEND, BinValue: bin.Value}
}

// Create integer add database operation.
func NewAddOp(bin Bin) *Operation {
	return &Operation{OpType: ADD, BinValue: bin.Value}
}

// Create touch database operation.
func NewTouchOp() *Operation {
	return &Operation{OpType: TOUCH}
}

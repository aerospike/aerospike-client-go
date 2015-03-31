// Copyright 2013-2015 Aerospike, Inc.
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

// OperationType determines operation type
type OperationType byte

const (
	READ OperationType = 1
	// READ_HEADER OperationType = 1
	WRITE   OperationType = 2
	ADD     OperationType = 5
	APPEND  OperationType = 9
	PREPEND OperationType = 10
	TOUCH   OperationType = 11
)

// Operation contasins operation definition.
// This struct is used in client's operate() method.
type Operation struct {

	// OpType determines type of operation.
	OpType OperationType

	// BinName (Optional) determines the name of bin used in operation.
	BinName string

	// BinValue (Optional) determines bin value used in operation.
	BinValue Value

	// will be true ONLY for GetHeader() operation
	headerOnly bool
}

// GetOpForBin creates read bin database operation.
func GetOpForBin(binName string) *Operation {
	return &Operation{OpType: READ, BinName: binName, BinValue: NewNullValue()}
}

// GetOp creates read all record bins database operation.
func GetOp() *Operation {
	return &Operation{OpType: READ, BinValue: NewNullValue()}
}

// GetHeaderOp creates read record header database operation.
func GetHeaderOp() *Operation {
	return &Operation{OpType: READ, headerOnly: true, BinValue: NewNullValue()}
}

// PutOp creates set database operation.
func PutOp(bin *Bin) *Operation {
	return &Operation{OpType: WRITE, BinName: bin.Name, BinValue: bin.Value}
}

// AppendOp creates string append database operation.
func AppendOp(bin *Bin) *Operation {
	return &Operation{OpType: APPEND, BinName: bin.Name, BinValue: bin.Value}
}

// PrependOp creates string prepend database operation.
func PrependOp(bin *Bin) *Operation {
	return &Operation{OpType: PREPEND, BinName: bin.Name, BinValue: bin.Value}
}

// AddOp creates integer add database operation.
func AddOp(bin *Bin) *Operation {
	return &Operation{OpType: ADD, BinName: bin.Name, BinValue: bin.Value}
}

// TouchOp creates touch database operation.
func TouchOp() *Operation {
	return &Operation{OpType: TOUCH, BinValue: NewNullValue()}
}

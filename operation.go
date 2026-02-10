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

// OperationType determines operation type
type OperationType struct {
	op       byte
	isWrite  bool
	enumDist byte // make the values like enum to distinguish them from each other
}

type operationSubType *int

// Valid OperationType values that can be used to create custom Operations.
// The names are self-explanatory.
var (
	_READ        = OperationType{1, false, 0}
	_READ_HEADER = OperationType{1, false, 1}
	_WRITE       = OperationType{2, true, 2}
	_CDT_READ    = OperationType{3, false, 3}
	_CDT_MODIFY  = OperationType{4, true, 4}
	_MAP_READ    = OperationType{3, false, 5}
	_MAP_MODIFY  = OperationType{4, true, 6}
	_ADD         = OperationType{5, true, 7}
	_EXP_READ    = OperationType{7, false, 8}
	_EXP_MODIFY  = OperationType{8, true, 9}
	_APPEND      = OperationType{9, true, 10}
	_PREPEND     = OperationType{10, true, 11}
	_TOUCH       = OperationType{11, true, 12}
	_BIT_READ    = OperationType{12, false, 13}
	_BIT_MODIFY  = OperationType{13, true, 14}
	_DELETE      = OperationType{14, true, 15}
	_HLL_READ    = OperationType{15, false, 16}
	_HLL_MODIFY  = OperationType{16, true, 17}
)

// Operation contains operation definition.
// This struct is used in client's operate() method.
type Operation struct {

	// OpType determines type of operation.
	opType OperationType
	// used in CDT commands
	opSubType operationSubType
	// CDT context for nested types
	ctx []*CDTContext

	encoder func(*Operation, BufferEx) (int, Error)

	// binName (Optional) determines the name of bin used in operation.
	binName string

	// binValue (Optional) determines bin value used in operation.
	binValue Value
}

// size returns the size of the operation on the wire protocol.
func (op *Operation) size() (int, Error) {
	size := len(op.binName)

	// Simple case
	if op.encoder == nil {
		valueLength, err := op.binValue.EstimateSize()
		if err != nil {
			return -1, err
		}

		size += valueLength + 8
		return size, nil
	}

	// Complex case, for CDTs
	valueLength, err := op.encoder(op, nil)
	if err != nil {
		return -1, err
	}

	size += valueLength + 8
	return size, nil
}

// GetBinOp creates read bin database operation.
func GetBinOp(binName string) *Operation {
	return &Operation{opType: _READ, binName: binName, binValue: NewNullValue()}
}

// GetOp creates read all record bins database operation.
func GetOp() *Operation {
	return &Operation{opType: _READ, binValue: NewNullValue()}
}

// GetHeaderOp creates read record header database operation.
func GetHeaderOp() *Operation {
	return &Operation{opType: _READ_HEADER, binValue: NewNullValue()}
}

// PutOp creates set database operation.
func PutOp(bin *Bin) *Operation {
	return &Operation{opType: _WRITE, binName: bin.Name, binValue: bin.Value}
}

// AppendOp creates string append database operation.
func AppendOp(bin *Bin) *Operation {
	return &Operation{opType: _APPEND, binName: bin.Name, binValue: bin.Value}
}

// PrependOp creates string prepend database operation.
func PrependOp(bin *Bin) *Operation {
	return &Operation{opType: _PREPEND, binName: bin.Name, binValue: bin.Value}
}

// AddOp creates integer add database operation.
func AddOp(bin *Bin) *Operation {
	return &Operation{opType: _ADD, binName: bin.Name, binValue: bin.Value}
}

// TouchOp creates touch record database operation.
func TouchOp() *Operation {
	return &Operation{opType: _TOUCH, binValue: NewNullValue()}
}

// DeleteOp creates delete record database operation.
func DeleteOp() *Operation {
	return &Operation{opType: _DELETE, binValue: NewNullValue()}
}

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

var _ BatchRecordIfc = &BatchUDF{}

// BatchUDF encapsulates a batch user defined function operation.
type BatchUDF struct {
	BatchRecord

	// Policy is the optional UDF Policy.
	Policy *BatchUDFPolicy

	// PackageName specify the lua module name.
	PackageName string

	// FunctionName specify Lua function name.
	FunctionName string

	// FunctionArgs specify optional arguments to lua function.
	FunctionArgs []Value

	// Wire protocol bytes for function args. For internal use only.
	argBytes []byte
}

// NewBatchUDF creates a batch UDF operation.
func NewBatchUDF(policy *BatchUDFPolicy, key *Key, packageName, functionName string, functionArgs ...Value) *BatchUDF {
	return &BatchUDF{
		BatchRecord:  *newSimpleBatchRecord(key, true),
		Policy:       policy,
		PackageName:  packageName,
		FunctionName: functionName,
		FunctionArgs: functionArgs,
	}
}

// newBatchUDF creates a batch UDF operation.
func newBatchUDF(policy *BatchUDFPolicy, key *Key, packageName, functionName string, functionArgs ...Value) (*BatchUDF, *BatchRecord) {
	res := &BatchUDF{
		BatchRecord:  *newSimpleBatchRecord(key, true),
		Policy:       policy,
		PackageName:  packageName,
		FunctionName: functionName,
		FunctionArgs: functionArgs,
	}
	return res, &res.BatchRecord
}

func (bu *BatchUDF) isWrite() bool {
	return bu.hasWrite
}

func (bu *BatchUDF) key() *Key {
	return bu.Key
}

// Return batch command type.
func (bu *BatchUDF) getType() batchRecordType {
	return _BRT_BATCH_UDF
}

// Optimized reference equality check to determine batch wire protocol repeat flag.
// For internal use only.
func (bu *BatchUDF) equals(obj BatchRecordIfc) bool {
	if other, ok := obj.(*BatchUDF); !ok {
		return false
	} else {
		return bu.FunctionName == other.FunctionName && &bu.FunctionArgs == &other.FunctionArgs &&
			bu.PackageName == other.PackageName && bu.Policy == other.Policy
	}
}

// Return wire protocol size. For internal use only.
func (bu *BatchUDF) size(parentPolicy *BasePolicy) (int, Error) {
	size := 2 // gen(2) = 2

	if bu.Policy != nil {
		if bu.Policy.FilterExpression != nil {
			sz, err := bu.Policy.FilterExpression.size()
			if err != nil {
				return -1, err
			}
			size += sz + int(_FIELD_HEADER_SIZE)
		}

		if (bu.Policy.SendKey || parentPolicy.SendKey) && bu.Key.hasValueToSend() {
			if sz, err := bu.Key.userKey.EstimateSize(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE) + 1
			}
		}
	} else if parentPolicy.SendKey && bu.Key.hasValueToSend() {
		sz, err := bu.Key.userKey.EstimateSize()
		if err != nil {
			return -1, err
		}
		size += sz + int(_FIELD_HEADER_SIZE) + 1
	}

	size += len(bu.PackageName) + int(_FIELD_HEADER_SIZE)
	size += len(bu.FunctionName) + int(_FIELD_HEADER_SIZE)

	packer := newPacker()
	sz, err := packValueArray(packer, bu.FunctionArgs)
	if err != nil {
		return -1, err
	}

	bu.argBytes = packer.Bytes()

	size += sz + int(_FIELD_HEADER_SIZE)
	return size, nil
}

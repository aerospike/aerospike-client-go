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

	// Optional UDF policy.
	policy *BatchUDFPolicy

	// Package or lua module name.
	packageName string

	// Lua function name.
	functionName string

	// Optional arguments to lua function.
	functionArgs []Value

	// Wire protocol bytes for function args. For internal use only.
	argBytes []byte
}

// NewBatchUDF creates a batch UDF operation.
func NewBatchUDF(policy *BatchUDFPolicy, key *Key, packageName, functionName string, functionArgs ...Value) *BatchUDF {
	return &BatchUDF{
		BatchRecord:  *newSimpleBatchRecord(key, true),
		policy:       policy,
		packageName:  packageName,
		functionName: functionName,
		functionArgs: functionArgs,
	}
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
		return bu.functionName == other.functionName && &bu.functionArgs == &other.functionArgs &&
			bu.packageName == other.packageName && bu.policy == other.policy
	}
}

// Return wire protocol size. For internal use only.
func (bu *BatchUDF) size() (int, Error) {
	size := 6 // gen(2) + exp(4) = 6

	if bu.policy != nil {
		if bu.policy.FilterExpression != nil {
			sz, err := bu.policy.FilterExpression.pack(nil)
			if err != nil {
				return -1, err
			}
			size += sz
		}

		if bu.policy.SendKey {
			if sz, err := bu.Key.userKey.EstimateSize(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE) + 1
			}
		}
	}
	size += len(bu.packageName) + int(_FIELD_HEADER_SIZE)
	size += len(bu.functionName) + int(_FIELD_HEADER_SIZE)

	packer := newPacker()
	sz, err := packValueArray(packer, bu.functionArgs)
	if err != nil {
		return -1, err
	}

	bu.argBytes = packer.Bytes()

	size += sz + int(_FIELD_HEADER_SIZE)
	return size, nil
}

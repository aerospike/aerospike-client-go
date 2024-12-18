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

import "github.com/aerospike/aerospike-client-go/v7/types"

var _ BatchRecordIfc = &BatchWrite{}

// BatchWrite encapsulates a batch key and read/write operations with write policy.
type BatchWrite struct {
	BatchRecord

	// Policy is an optional write Policy.
	Policy *BatchWritePolicy

	// Ops specify required operations for this key.
	Ops []*Operation
}

// NewBatchWrite initializesa policy, batch key and read/write operations.
// ANy GetOp() is not allowed because it returns a variable number of bins and
// makes it difficult (sometimes impossible) to lineup operations with results. Instead,
// use GetBinOp(string) for each bin name.
func NewBatchWrite(policy *BatchWritePolicy, key *Key, ops ...*Operation) *BatchWrite {
	return &BatchWrite{
		BatchRecord: *newSimpleBatchRecord(key, true),
		Ops:         ops,
		Policy:      policy,
	}
}

func (bw *BatchWrite) isWrite() bool {
	return bw.hasWrite
}

func (bw *BatchWrite) key() *Key {
	return bw.Key
}

// Return batch command type.
func (bw *BatchWrite) getType() batchRecordType {
	return _BRT_BATCH_WRITE
}

// Optimized reference equality check to determine batch wire protocol repeat flag.
// For internal use only.
func (bw *BatchWrite) equals(obj BatchRecordIfc) bool {
	other, ok := obj.(*BatchWrite)
	if !ok {
		return false
	}

	return &bw.Ops == &other.Ops && bw.Policy == other.Policy && (bw.Policy == nil || !bw.Policy.SendKey)
}

// Return wire protocol size. For internal use only.
func (bw *BatchWrite) size(parentPolicy *BasePolicy) (int, Error) {
	size := 2 // gen(2) = 2

	if bw.Policy != nil {
		if bw.Policy.FilterExpression != nil {
			if sz, err := bw.Policy.FilterExpression.size(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE)
			}
		}

		if (bw.Policy.SendKey || parentPolicy.SendKey) && bw.Key.hasValueToSend() {
			if sz, err := bw.Key.userKey.EstimateSize(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE) + 1
			}
		}
	} else if parentPolicy.SendKey && bw.Key.hasValueToSend() {
		sz, err := bw.Key.userKey.EstimateSize()
		if err != nil {
			return -1, err
		}
		size += sz + int(_FIELD_HEADER_SIZE) + 1
	}

	hasWrite := false

	for _, op := range bw.Ops {
		if op.opType.isWrite {
			hasWrite = true
		}
		sz, err := op.size()
		if err != nil {
			return -1, err
		}
		size += sz
	}

	if !hasWrite {
		return -1, newError(types.PARAMETER_ERROR, "Batch write operations do not contain a write")
	}
	return size, nil
}

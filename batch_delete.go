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

var _ BatchRecordIfc = &BatchDelete{}

// BatchDelete encapsulates a batch delete operation.
type BatchDelete struct {
	BatchRecord

	// policy os the optional write policy.
	policy *BatchDeletePolicy
}

func (bd *BatchDelete) hasWrite() bool {
	return bd.BatchRecord.hasWrite
}

func (bd *BatchDelete) key() *Key {
	return bd.Key
}

// NewBatchDelete creates a batch delete operation.
func NewBatchDelete(policy *BatchDeletePolicy, key *Key) *BatchDelete {
	return &BatchDelete{
		BatchRecord: *newSimpleBatchRecord(key, true),
		policy:      policy,
	}
}

// Return batch command type.
func (bd *BatchDelete) getType() batchRecordType {
	return _BRT_BATCH_DELETE
}

// Optimized reference equality check to determine batch wire protocol repeat flag.
// For internal use only.
func (bd *BatchDelete) equals(obj BatchRecordIfc) bool {
	other, ok := obj.(*BatchDelete)
	if !ok {
		return false
	}

	return bd.policy == other.policy
}

// Return wire protocol size. For internal use only.
func (bd *BatchDelete) size(parentPolicy *BasePolicy) (int, Error) {
	size := 6 // gen(2) + exp(4) = 6

	if bd.policy != nil {
		if bd.policy.FilterExpression != nil {
			if sz, err := bd.policy.FilterExpression.size(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE)
			}
		}

		if bd.policy.SendKey || parentPolicy.SendKey {
			if sz, err := bd.Key.userKey.EstimateSize(); err != nil {
				return -1, err
			} else {
				size += sz + int(_FIELD_HEADER_SIZE) + 1
			}
		}
	} else if parentPolicy.SendKey {
		sz, err := bd.Key.userKey.EstimateSize()
		if err != nil {
			return -1, err
		}
		size += sz + int(_FIELD_HEADER_SIZE) + 1
	}

	return size, nil
}

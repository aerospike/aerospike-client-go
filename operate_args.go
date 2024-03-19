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

type operateArgs struct {
	writePolicy *WritePolicy
	operations  []*Operation
	partition   *Partition
	readAttr    int
	writeAttr   int
	hasWrite    bool
}

func newOperateArgs(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	operations []*Operation,
) (res operateArgs, err Error) {
	res = operateArgs{
		operations:  operations,
		writePolicy: policy,
	}

	rattr := 0
	wattr := 0
	write := false
	readBin := false
	readHeader := false
	respondAllOps := false

	for _, operation := range operations {
		switch operation.opType {
		case _BIT_READ, _EXP_READ, _HLL_READ, _MAP_READ:
			// Map operations require respondAllOps to be true.
			respondAllOps = true
			// Fall through to read.
			fallthrough
		case _CDT_READ, _READ:
			rattr |= _INFO1_READ

			// Read all bins if no bin is specified.
			if len(operation.binName) == 0 {
				rattr |= _INFO1_GET_ALL
			}
			readBin = true
		case _READ_HEADER:
			rattr |= _INFO1_READ
			readHeader = true
		case _BIT_MODIFY, _EXP_MODIFY, _HLL_MODIFY, _MAP_MODIFY:
			// Map operations require respondAllOps to be true.
			respondAllOps = true
			// Fall through to write.
			fallthrough
		default:
			wattr = _INFO2_WRITE
			write = true
		}

	}
	res.hasWrite = write

	if readHeader && !readBin {
		rattr |= _INFO1_NOBINDATA
	}
	res.readAttr = rattr

	// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
	if (respondAllOps || res.writePolicy.RespondPerEachOp) && (rattr&_INFO1_GET_ALL) == 0 {
		wattr |= _INFO2_RESPOND_ALL_OPS
	}
	res.writeAttr = wattr

	if cluster != nil {
		if write {
			res.partition, err = PartitionForWrite(cluster, &res.writePolicy.BasePolicy, key)
			if err != nil {
				return operateArgs{}, err
			}
		} else {
			res.partition, err = PartitionForRead(cluster, &res.writePolicy.BasePolicy, key)
			if err != nil {
				return operateArgs{}, err
			}
		}
	}
	return res, nil
}

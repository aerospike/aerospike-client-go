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

type batchAttr struct {
	filterExp  *Expression
	readAttr   int
	writeAttr  int
	infoAttr   int
	txnAttr    int
	expiration uint32
	generation uint32
	hasWrite   bool
	sendKey    bool
}

func newBatchAttr(policy *BatchPolicy, rattr int) *batchAttr {
	res := &batchAttr{}
	res.setRead(policy)
	res.readAttr |= rattr

	return res
}

func newBatchAttrOpsAttr(policy *BatchPolicy, rattr int, ops []*Operation) *batchAttr {
	res := &batchAttr{}
	res.setRead(policy)
	res.readAttr = rattr
	if len(ops) > 0 {
		res.adjustRead(ops)
	}
	return res
}

func (ba *batchAttr) setRead(rp *BatchPolicy) {
	ba.filterExp = nil
	ba.readAttr = _INFO1_READ

	if rp.ReadModeAP == ReadModeAPAll {
		ba.readAttr |= _INFO1_READ_MODE_AP_ALL
	}

	ba.writeAttr = 0

	switch rp.ReadModeSC {
	default:
	case ReadModeSCSession:
		ba.infoAttr = 0
	case ReadModeSCLinearize:
		ba.infoAttr = _INFO3_SC_READ_TYPE
	case ReadModeSCAllowReplica:
		ba.infoAttr = _INFO3_SC_READ_RELAX
	case ReadModeSCAllowUnavailable:
		ba.infoAttr = _INFO3_SC_READ_TYPE | _INFO3_SC_READ_RELAX
	}
	ba.txnAttr = 0
	ba.expiration = uint32(rp.ReadTouchTTLPercent)
	ba.generation = 0
	ba.hasWrite = false
	ba.sendKey = false
}

func (ba *batchAttr) setBatchRead(rp *BatchReadPolicy) {
	ba.filterExp = rp.FilterExpression
	ba.readAttr = _INFO1_READ

	if rp.ReadModeAP == ReadModeAPAll {
		ba.readAttr |= _INFO1_READ_MODE_AP_ALL
	}

	ba.writeAttr = 0

	switch rp.ReadModeSC {
	default:
	case ReadModeSCSession:
		ba.infoAttr = 0
	case ReadModeSCLinearize:
		ba.infoAttr = _INFO3_SC_READ_TYPE
	case ReadModeSCAllowReplica:
		ba.infoAttr = _INFO3_SC_READ_RELAX
	case ReadModeSCAllowUnavailable:
		ba.infoAttr = _INFO3_SC_READ_TYPE | _INFO3_SC_READ_RELAX
	}
	ba.txnAttr = 0
	ba.expiration = uint32(rp.ReadTouchTTLPercent)
	ba.generation = 0
	ba.hasWrite = false
	ba.sendKey = false
}

func (ba *batchAttr) adjustRead(ops []*Operation) {
	for _, op := range ops {
		switch op.opType {
		case _READ:
			if len(op.binName) == 0 {
				ba.readAttr |= _INFO1_GET_ALL
			}
		case _READ_HEADER:
			ba.readAttr |= _INFO1_NOBINDATA
		}
	}
}

func (ba *batchAttr) adjustReadForAllBins(readAllBins bool) {
	if readAllBins {
		ba.readAttr |= _INFO1_GET_ALL
	} else {
		ba.readAttr |= _INFO1_NOBINDATA
	}
}

func (ba *batchAttr) setBatchWrite(wp *BatchWritePolicy) {
	ba.filterExp = wp.FilterExpression
	ba.readAttr = 0
	ba.writeAttr = _INFO2_WRITE | _INFO2_RESPOND_ALL_OPS
	ba.infoAttr = 0
	ba.txnAttr = 0
	ba.expiration = wp.Expiration
	ba.hasWrite = true
	ba.sendKey = wp.SendKey

	switch wp.GenerationPolicy {
	default:
		fallthrough
	case NONE:
		ba.generation = 0

	case EXPECT_GEN_EQUAL:
		ba.generation = wp.Generation
		ba.writeAttr |= _INFO2_GENERATION

	case EXPECT_GEN_GT:
		ba.generation = wp.Generation
		ba.writeAttr |= _INFO2_GENERATION_GT
	}

	switch wp.RecordExistsAction {
	case UPDATE:
	case UPDATE_ONLY:
		ba.infoAttr |= _INFO3_UPDATE_ONLY
	case REPLACE:
		ba.infoAttr |= _INFO3_CREATE_OR_REPLACE
	case REPLACE_ONLY:
		ba.infoAttr |= _INFO3_REPLACE_ONLY
	case CREATE_ONLY:
		ba.writeAttr |= _INFO2_CREATE_ONLY
	}

	if wp.DurableDelete {
		ba.writeAttr |= _INFO2_DURABLE_DELETE
	}

	if wp.OnLockingOnly {
		ba.txnAttr |= _INFO4_MRT_ON_LOCKING_ONLY
	}

	if wp.CommitLevel == COMMIT_MASTER {
		ba.infoAttr |= _INFO3_COMMIT_MASTER
	}
}

func (ba *batchAttr) adjustWrite(ops []*Operation) {
	readAllBins := false
	readHeader := false
	hasRead := false

	for _, op := range ops {
		switch op.opType {
		case _READ_HEADER:
			readHeader = true
			hasRead = true
		case _BIT_READ, _EXP_READ, _HLL_READ, _MAP_READ, _CDT_READ, _READ:
			// _Read all bins if no bin is specified.
			if op.binName == "" {
				readAllBins = true
			}
			hasRead = true

		default:
		}
	}

	if hasRead {
		ba.readAttr |= _INFO1_READ

		if readAllBins {
			ba.readAttr |= _INFO1_GET_ALL
		} else if readHeader {
			ba.readAttr |= _INFO1_NOBINDATA
		}
	}
}

func (ba *batchAttr) setBatchUDF(up *BatchUDFPolicy) {
	ba.filterExp = up.FilterExpression
	ba.readAttr = 0
	ba.writeAttr = _INFO2_WRITE
	ba.infoAttr = 0
	ba.txnAttr = 0
	ba.expiration = up.Expiration
	ba.generation = 0
	ba.hasWrite = true
	ba.sendKey = up.SendKey

	if up.DurableDelete {
		ba.writeAttr |= _INFO2_DURABLE_DELETE
	}

	if up.OnLockingOnly {
		ba.txnAttr |= _INFO4_MRT_ON_LOCKING_ONLY
	}

	if up.CommitLevel == COMMIT_MASTER {
		ba.infoAttr |= _INFO3_COMMIT_MASTER
	}
}

func (ba *batchAttr) setBatchDelete(dp *BatchDeletePolicy) {
	ba.filterExp = dp.FilterExpression
	ba.readAttr = 0
	ba.writeAttr = _INFO2_WRITE | _INFO2_RESPOND_ALL_OPS | _INFO2_DELETE
	ba.infoAttr = 0
	ba.txnAttr = 0
	ba.expiration = 0
	ba.hasWrite = true
	ba.sendKey = dp.SendKey

	switch dp.GenerationPolicy {
	default:
	case NONE:
		ba.generation = 0
	case EXPECT_GEN_EQUAL:
		ba.generation = dp.Generation
		ba.writeAttr |= _INFO2_GENERATION
	case EXPECT_GEN_GT:
		ba.generation = dp.Generation
		ba.writeAttr |= _INFO2_GENERATION_GT
	}

	if dp.DurableDelete {
		ba.writeAttr |= _INFO2_DURABLE_DELETE
	}

	if dp.CommitLevel == COMMIT_MASTER {
		ba.infoAttr |= _INFO3_COMMIT_MASTER
	}
}

func (ba *batchAttr) setTxn(attr int) {
	ba.filterExp = nil
	ba.readAttr = 0
	ba.writeAttr = _INFO2_WRITE | _INFO2_RESPOND_ALL_OPS | _INFO2_DURABLE_DELETE
	ba.infoAttr = 0
	ba.txnAttr = attr
	ba.expiration = 0
	ba.generation = 0
	ba.hasWrite = true
	ba.sendKey = false
}

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

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v6/types"
)

type batchRecordType byte

// Batch command type.
const (
	_BRT_INVALID batchRecordType = iota
	_BRT_BATCH_READ
	_BRT_BATCH_WRITE
	_BRT_BATCH_DELETE
	_BRT_BATCH_UDF
)

// BatchRecordIfc is the interface type to encapsulate BatchRead, BatchWrite and BatchUDF commands.
type BatchRecordIfc interface {
	// Returns the BatchRecord
	BatchRec() *BatchRecord

	key() *Key
	resultCode() types.ResultCode
	isWrite() bool

	prepare()
	setRecord(record *Record)
	setError(resultCode types.ResultCode, inDoubt bool)
	chainError(err Error)
	String() string
	getType() batchRecordType
	size() (int, Error)
	equals(BatchRecordIfc) bool
}

// BatchRecord encasulates the Batch key and record result.
type BatchRecord struct {
	// Key.
	Key *Key

	// Record result after batch command has completed.  Will be nil if record was not found
	// or an error occurred. See ResultCode.
	Record *Record

	// ResultCode for this returned record. See types.ResultCode.
	// If not OK, the record will be nil.
	ResultCode types.ResultCode

	// Err encapsulates the possible error chain for this keys
	Err Error

	// InDoubt signifies the possiblity that the write transaction may have completed even though an error
	// occurred for this record. This may be the case when a client error occurs (like timeout)
	// after the command was sent to the server.
	InDoubt bool

	// Does this command contain a write operation. For internal use only.
	hasWrite bool
}

func newSimpleBatchRecord(key *Key, hasWrite bool) *BatchRecord {
	return &BatchRecord{
		Key:        key,
		ResultCode: types.NO_RESPONSE,
		hasWrite:   hasWrite,
	}
}

func newBatchRecord(key *Key, record *Record, resultCode types.ResultCode, inDoubt, hasWrite bool) *BatchRecord {
	return &BatchRecord{
		Key:        key,
		Record:     record,
		ResultCode: resultCode,
		InDoubt:    inDoubt,
		hasWrite:   hasWrite,
	}
}

// BatchRec returns the embedded batch record in the interface.
func (br *BatchRecord) BatchRec() *BatchRecord {
	return br
}

func (br *BatchRecord) chainError(e Error) {
	br.Err = chainErrors(e, br.Err)
}

func (br *BatchRecord) isWrite() bool {
	return br.hasWrite
}

func (br *BatchRecord) key() *Key {
	return br.Key
}

func (br *BatchRecord) resultCode() types.ResultCode {
	return br.ResultCode
}

// Prepare for upcoming batch call. Reset result fields because this instance might be
// reused. For internal use only.
func (br *BatchRecord) prepare() {
	br.Record = nil
	br.ResultCode = types.NO_RESPONSE
	br.InDoubt = false
}

// Set record result. For internal use only.
func (br *BatchRecord) setRecord(record *Record) {
	br.Record = record
	br.ResultCode = types.OK
}

// Set error result. For internal use only.
func (br *BatchRecord) setError(resultCode types.ResultCode, inDoubt bool) {
	br.ResultCode = resultCode
	br.InDoubt = inDoubt
}

// String implements the Stringer interface.
func (br *BatchRecord) String() string {
	return fmt.Sprintf("Key: %s, Record: %s, ResultCode: %s, InDoubt: %t, Err: %v", br.Key, br.Record, br.ResultCode.String(), br.InDoubt, br.Err)
}

func (br *BatchRecord) equals(other BatchRecordIfc) bool {
	return false
}

// Return batch command type. For internal use only.
func (br *BatchRecord) getType() batchRecordType {
	return _BRT_INVALID
}

// Return wire protocol size. For internal use only.
func (br *BatchRecord) size() (int, Error) {
	return 0, nil
}

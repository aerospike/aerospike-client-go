//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// RecordResult is one row of a [RecordStream].
//
// Per-record failures arrive as data on the row rather than as an error from
// iteration: check [RecordResult.IsOK], or call [RecordResult.OrRaise] to
// convert the row into an error.
type RecordResult struct {
	// Key is the record's key.
	Key *as.Key
	// Record is the record payload, nil on error, not-found, and UDF rows.
	Record *as.Record
	// ResultCode is the server result code for this row.
	ResultCode types.ResultCode
	// InDoubt marks a write that may have completed despite the error.
	InDoubt bool
	// Index is the row's position in the originating batch: 0 for a
	// single-key operation, -1 for a query.
	Index int64
	// Err carries the embedded error when the client placed one in-stream.
	Err *Error
	// UDFResult is the Lua return value for a user-defined function.
	UDFResult as.Value
}

// newRecordResult builds a row with the query defaults.
func newRecordResult(key *as.Key, rec *as.Record, rc types.ResultCode) *RecordResult {
	return &RecordResult{Key: key, Record: rec, ResultCode: rc, Index: -1}
}

// IsOK reports whether the row succeeded.
func (r *RecordResult) IsOK() bool { return r.ResultCode == types.OK }

// OrRaise returns the row when it succeeded, else an error. The embedded error
// takes precedence over one built from the result code.
func (r *RecordResult) OrRaise() (*RecordResult, error) {
	if r.IsOK() {
		return r, nil
	}
	if r.Err != nil {
		return nil, r.Err
	}
	return nil, ErrorFromResultCode(r.ResultCode, "", r.InDoubt)
}

// RecordOrRaise returns the row's record, or an error when the row failed or
// carries no payload.
func (r *RecordResult) RecordOrRaise() (*as.Record, error) {
	if _, err := r.OrRaise(); err != nil {
		return nil, err
	}
	if r.Record == nil {
		return nil, NewError(KindAerospike, "row succeeded but carries no record")
	}
	return r.Record, nil
}

// AsBool interprets the row as an existence check: true when it succeeded,
// false when the record was not found, an error otherwise.
func (r *RecordResult) AsBool() (bool, error) {
	switch {
	case r.IsOK():
		return true, nil
	case r.ResultCode == types.KEY_NOT_FOUND_ERROR:
		return false, nil
	default:
		_, err := r.OrRaise()
		return false, err
	}
}

// OperationResult reports the i-th of the operate's returned results.
//
// When several operations write to the same bin, the bin map cannot hold all of
// their results; this positional view can. Only operations that produce a value
// are represented, so the positions follow the returned results rather than the
// request: a chain of put, add and get yields one result, from the get.
func (r *RecordResult) OperationResult(i int) (any, bool) {
	if r.Record == nil {
		return nil, false
	}
	return r.Record.OperationResult(i)
}

// OperationResults reports every returned operation result, in order.
func (r *RecordResult) OperationResults() []any {
	if r.Record == nil {
		return nil
	}
	return r.Record.OpResults
}

// OperationResultAt wraps the i-th operation result in an [OperationResult],
// for typed access.
func (r *RecordResult) OperationResultAt(i int) (*OperationResult, bool) {
	v, ok := r.OperationResult(i)
	if !ok {
		return nil, false
	}
	return NewOperationResult(v), true
}

// GetHLLConfig interprets a bin written by an HLL describe operation.
// It reports a nil configuration when the bin or record is absent.
func (r *RecordResult) GetHLLConfig(bin string) (*HLLConfig, error) {
	if r.Record == nil {
		return nil, nil
	}
	raw, ok := r.Record.Bins[bin]
	if !ok || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok || len(list) != 2 {
		return nil, NewError(KindInvalidArgument,
			"bin %q does not hold an HLL description", bin)
	}
	index, ok1 := toInt64(list[0])
	minHash, ok2 := toInt64(list[1])
	if !ok1 || !ok2 {
		return nil, NewError(KindInvalidArgument,
			"bin %q holds a malformed HLL description", bin)
	}
	return &HLLConfig{IndexBitCount: index, MinHashBitCount: minHash}, nil
}

// toInt64 narrows the integral types the client may hand back.
func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case int32:
		return int64(t), true
	}
	return 0, false
}

// batchRecordsToResults converts core batch records into rows, stamping the
// input index onto each.
func batchRecordsToResults(records []as.BatchRecordIfc) []*RecordResult {
	out := make([]*RecordResult, 0, len(records))
	for i, br := range records {
		rec := br.BatchRec()
		row := &RecordResult{
			Key:        rec.Key,
			Record:     rec.Record,
			ResultCode: rec.ResultCode,
			InDoubt:    rec.InDoubt,
			Index:      int64(i),
		}
		if rec.Err != nil {
			row.Err = WrapError(rec.Err)
		}
		out = append(out, row)
	}
	return out
}

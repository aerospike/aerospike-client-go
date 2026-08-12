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
	"bytes"
	"iter"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// RecordStream is a stream of [RecordResult] rows.
//
// Failures arrive on two channels, deliberately: cluster-level failures are
// returned as an error from [RecordStream.Next], while per-record failures
// arrive as data on the row. Which channel a per-record failure takes is
// decided at execute time by the [OnError] disposition.
//
// Always close a stream when you stop reading it early; Close is idempotent
// and safe to defer.
type RecordStream struct {
	mu     sync.Mutex
	closed bool

	// Exactly one source is active.
	rows    []*RecordResult // list and single sources
	rowIdx  int
	chained []*RecordStream // chain source

	recordset *as.Recordset // query and chunked sources
	records   iter.Seq2[*as.Record, as.Error]
	recNext   func() (*as.Record, as.Error, bool)
	recStop   func()

	// Channel source, used by the lazy batch path.
	rowCh <-chan *RecordResult
	errCh <-chan error

	// Chunked-cursor state.
	chunked    bool
	chunkFirst bool
	chunkLimit int64
	chunkCount int64
	reexecute  func() (*as.Recordset, error)
}

// NewRecordStreamFromList wraps already-materialized rows.
func NewRecordStreamFromList(rows []*RecordResult) *RecordStream {
	return &RecordStream{rows: rows, chunkFirst: true}
}

// EmptyRecordStream returns an exhausted stream.
func EmptyRecordStream() *RecordStream { return &RecordStream{chunkFirst: true} }

// ChainRecordStreams drains the given streams in order.
func ChainRecordStreams(streams ...*RecordStream) *RecordStream {
	return &RecordStream{chained: streams, chunkFirst: true}
}

// NewRecordStreamFromSingle wraps a single-key outcome. A nil record becomes a
// not-found row.
func NewRecordStreamFromSingle(key *as.Key, rec *as.Record) *RecordStream {
	rc := types.OK
	if rec == nil {
		rc = types.KEY_NOT_FOUND_ERROR
	}
	row := &RecordResult{Key: key, Record: rec, ResultCode: rc, Index: 0}
	return NewRecordStreamFromList([]*RecordResult{row})
}

// NewRecordStreamFromError wraps a single failed row.
func NewRecordStreamFromError(key *as.Key, rc types.ResultCode, inDoubt bool, err *Error) *RecordStream {
	row := &RecordResult{Key: key, ResultCode: rc, InDoubt: inDoubt, Err: err, Index: 0}
	return NewRecordStreamFromList([]*RecordResult{row})
}

// newRecordStreamFromRecordset wraps a query recordset.
func newRecordStreamFromRecordset(rs *as.Recordset) *RecordStream {
	s := &RecordStream{recordset: rs, chunkFirst: true}
	s.bindRecordset(rs)
	return s
}

// newChunkedRecordStream wraps a chunked query cursor.
func newChunkedRecordStream(rs *as.Recordset, limit int64, reexec func() (*as.Recordset, error)) *RecordStream {
	s := &RecordStream{
		recordset:  rs,
		chunked:    true,
		chunkFirst: true,
		chunkLimit: limit,
		reexecute:  reexec,
	}
	s.bindRecordset(rs)
	return s
}

// bindRecordset attaches a pull iterator to a recordset.
func (s *RecordStream) bindRecordset(rs *as.Recordset) {
	s.records = rs.Records()
	s.recNext, s.recStop = iter.Pull2(s.records)
}

// Next advances the stream.
//
// It reports (nil, nil) when the stream is exhausted or closed. A returned
// error is a cluster-level failure; per-record failures ride on the row.
func (s *RecordStream) Next() (*RecordResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextLocked()
}

func (s *RecordStream) nextLocked() (*RecordResult, error) {
	if s.closed {
		return nil, nil
	}

	// Materialized rows.
	if s.rowIdx < len(s.rows) {
		row := s.rows[s.rowIdx]
		s.rowIdx++
		return row, nil
	}

	// Chained streams.
	for len(s.chained) > 0 {
		row, err := s.chained[0].Next()
		if err != nil {
			return nil, err
		}
		if row != nil {
			return row, nil
		}
		s.chained[0].Close()
		s.chained = s.chained[1:]
	}

	// Channel-backed source: the lazy batch path.
	if s.rowCh != nil {
		select {
		case err := <-s.errCh:
			if err != nil {
				return nil, err
			}
		default:
		}
		row, ok := <-s.rowCh
		if !ok {
			// Drain a failure the producer reported as it finished.
			select {
			case err := <-s.errCh:
				if err != nil {
					return nil, err
				}
			default:
			}
			return nil, nil
		}
		return row, nil
	}

	// Recordset-backed sources.
	if s.recNext != nil {
		rec, aerr, ok := s.recNext()
		if !ok {
			return nil, nil
		}
		if aerr != nil {
			return nil, WrapError(aerr)
		}
		s.chunkCount++
		return newRecordResult(rec.Key, rec, types.OK), nil
	}

	return nil, nil
}

// Iter returns a range-over-function iterator.
//
// Iteration stops at the first cluster-level error; call [RecordStream.Err]
// after the loop to retrieve it. Per-record failures are delivered as rows.
func (s *RecordStream) Iter() iter.Seq[*RecordResult] {
	return func(yield func(*RecordResult) bool) {
		for {
			row, err := s.Next()
			if err != nil {
				s.setErr(err)
				return
			}
			if row == nil {
				return
			}
			if !yield(row) {
				return
			}
		}
	}
}

// iterErr holds the error observed during Iter.
var iterErrs sync.Map // *RecordStream -> error

func (s *RecordStream) setErr(err error) { iterErrs.Store(s, err) }

// Err reports the cluster-level error that stopped [RecordStream.Iter], if any.
func (s *RecordStream) Err() error {
	if v, ok := iterErrs.Load(s); ok {
		return v.(error)
	}
	return nil
}

// Pop returns one row, leaving the stream open.
func (s *RecordStream) Pop() (*RecordResult, error) { return s.Next() }

// PopOrRaise returns one row, converting a failed row into an error, and
// leaves the stream open.
func (s *RecordStream) PopOrRaise() (*RecordResult, error) {
	row, err := s.Next()
	if err != nil || row == nil {
		return nil, err
	}
	return row.OrRaise()
}

// First returns one row and closes the stream.
func (s *RecordStream) First() (*RecordResult, error) {
	row, err := s.Next()
	s.Close()
	return row, err
}

// FirstOrRaise returns one row, converting a failed row or an empty stream
// into an error, and closes the stream.
func (s *RecordStream) FirstOrRaise() (*RecordResult, error) {
	row, err := s.First()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, NewError(KindAerospike, "stream produced no rows")
	}
	return row.OrRaise()
}

// FirstUDFResult scans forward for the first row carrying a user-defined
// function result.
func (s *RecordStream) FirstUDFResult() (as.Value, error) {
	for {
		row, err := s.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if row.UDFResult != nil {
			return row.UDFResult, nil
		}
	}
}

// FindRecord scans forward for the row whose key digest matches and returns
// its record. Rows before the match are consumed.
func (s *RecordStream) FindRecord(key *as.Key) (*as.Record, error) {
	if key == nil {
		return nil, NewError(KindInvalidArgument, "key must not be nil")
	}
	want := key.Digest()
	for {
		row, err := s.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if row.Key != nil && bytes.Equal(row.Key.Digest(), want) {
			return row.Record, nil
		}
	}
}

// Collect drains the stream, preserving order.
func (s *RecordStream) Collect() ([]*RecordResult, error) {
	defer s.Close()
	var out []*RecordResult
	for {
		row, err := s.Next()
		if err != nil {
			return out, err
		}
		if row == nil {
			return out, nil
		}
		out = append(out, row)
	}
}

// Failures drains the stream, keeping only the rows that failed.
func (s *RecordStream) Failures() ([]*RecordResult, error) {
	rows, err := s.Collect()
	var out []*RecordResult
	for _, r := range rows {
		if !r.IsOK() {
			out = append(out, r)
		}
	}
	return out, err
}

// HasMoreChunks advances a chunked query to its next chunk.
//
// It reports true on its first call for every stream shape, so one loop shape
// serves chunked and non-chunked queries alike:
//
//	for stream.HasMoreChunks() {
//	    for row, err := stream.Next(); row != nil; row, err = stream.Next() { ... }
//	}
func (s *RecordStream) HasMoreChunks() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.chunkFirst {
		s.chunkFirst = false
		return true, nil
	}
	if !s.chunked || s.reexecute == nil {
		return false, nil
	}
	if s.chunkLimit > 0 && s.chunkCount >= s.chunkLimit {
		return false, nil
	}

	rs, err := s.reexecute()
	if err != nil {
		return false, err
	}
	if rs == nil {
		return false, nil
	}

	if s.recStop != nil {
		s.recStop()
	}
	if s.recordset != nil {
		_ = s.recordset.Close()
	}
	s.recordset = rs
	s.closed = false
	s.bindRecordset(rs)
	return true, nil
}

// Close stops iteration and releases the producer. It is idempotent.
func (s *RecordStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.recStop != nil {
		s.recStop()
		s.recStop = nil
	}
	if s.recordset != nil {
		_ = s.recordset.Close()
		s.recordset = nil
	}
	for _, c := range s.chained {
		c.Close()
	}
	s.chained = nil
	s.rows = nil
	// The producer goroutine finishes on its own and closes the row channel;
	// dropping the reference lets it be collected once it does.
	s.rowCh = nil
	s.errCh = nil
	iterErrs.Delete(s)
}

// newChannelRecordStream wraps a producer goroutine's output channel.
//
// This is the lazy batch source: rows arrive in completion order as nodes
// respond, and a cluster-level failure arrives on the error channel.
func newChannelRecordStream(rows <-chan *RecordResult, errs <-chan error) *RecordStream {
	return &RecordStream{rowCh: rows, errCh: errs, chunkFirst: true}
}

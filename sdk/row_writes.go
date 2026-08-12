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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// RowWriteBuilder writes many records of the same shape without repeating the
// bin names: declare them once, then add one row per record.
//
// Each row becomes its own write segment, so guards stay per-record and results
// come back one row per record in insertion order. The chain is infallible:
// argument errors surface from Execute.
type RowWriteBuilder struct {
	session *Session
	ds      *DataSet
	verb    opType

	binNames []string
	rows     []rowEntry

	defaultTTL *int64
	txn        *as.Txn
	txnSet     bool

	pendingErr error
}

// rowEntry is one record: its identifier, its values, and its guards.
type rowEntry struct {
	id         any
	values     []any
	generation *uint32
	ttlSeconds *int64
}

// deferErr records the first error.
func (b *RowWriteBuilder) deferErr(err error) *RowWriteBuilder {
	if err != nil && b.pendingErr == nil {
		b.pendingErr = err
	}
	return b
}

// Bins declares the shared bin names. Call it once, before the first row.
func (b *RowWriteBuilder) Bins(names ...string) *RowWriteBuilder {
	if len(names) == 0 {
		return b.deferErr(NewError(KindInvalidArgument, "Bins needs at least one bin name"))
	}
	if len(b.rows) > 0 {
		return b.deferErr(NewError(KindInvalidArgument, "Bins must be called before the first Row"))
	}
	b.binNames = names
	return b
}

// Row adds one record: its dataset identifier plus one value per declared bin.
func (b *RowWriteBuilder) Row(id any, values ...any) *RowWriteBuilder {
	if len(b.binNames) == 0 {
		return b.deferErr(NewError(KindInvalidArgument, "Bins must be called before Row"))
	}
	if len(values) != len(b.binNames) {
		return b.deferErr(NewError(KindInvalidArgument,
			"row %d has %d values but %d bins were declared", len(b.rows), len(values), len(b.binNames)))
	}
	b.rows = append(b.rows, rowEntry{id: id, values: values})
	return b
}

// last reports the most recently added row.
func (b *RowWriteBuilder) last(what string) *rowEntry {
	if len(b.rows) == 0 {
		b.deferErr(NewError(KindInvalidArgument, "%s must be called after Row", what))
		return nil
	}
	return &b.rows[len(b.rows)-1]
}

// EnsureGenerationIs guards the most recently added row.
func (b *RowWriteBuilder) EnsureGenerationIs(generation uint32) *RowWriteBuilder {
	if generation == 0 {
		return b.deferErr(NewError(KindInvalidArgument, "generation guard must not be zero"))
	}
	if e := b.last("EnsureGenerationIs"); e != nil {
		e.generation = &generation
	}
	return b
}

// ExpireRecordAfterSeconds sets the expiration of the most recent row.
func (b *RowWriteBuilder) ExpireRecordAfterSeconds(seconds int64) *RowWriteBuilder {
	if e := b.last("ExpireRecordAfterSeconds"); e != nil {
		e.ttlSeconds = &seconds
	}
	return b
}

// ExpireRecordAfter sets the expiration of the most recent row.
func (b *RowWriteBuilder) ExpireRecordAfter(d time.Duration) *RowWriteBuilder {
	return b.ExpireRecordAfterSeconds(int64(d / time.Second))
}

// ExpireRecordAt sets an absolute expiry on the most recent row.
//
// Unlike the per-segment setter, this returns the builder rather than an error:
// a non-future instant is reported from Execute.
func (b *RowWriteBuilder) ExpireRecordAt(t time.Time) *RowWriteBuilder {
	secs, err := secondsUntil(t)
	if err != nil {
		return b.deferErr(err)
	}
	return b.ExpireRecordAfterSeconds(secs)
}

// NeverExpire keeps the most recent row forever.
func (b *RowWriteBuilder) NeverExpire() *RowWriteBuilder {
	return b.ExpireRecordAfterSeconds(ttlNeverExpire)
}

// WithNoChangeInExpiration leaves the most recent row's expiration untouched.
func (b *RowWriteBuilder) WithNoChangeInExpiration() *RowWriteBuilder {
	return b.ExpireRecordAfterSeconds(ttlDontUpdate)
}

// DefaultExpireRecordAfterSeconds sets the expiration for rows without one.
func (b *RowWriteBuilder) DefaultExpireRecordAfterSeconds(seconds int64) *RowWriteBuilder {
	b.defaultTTL = &seconds
	return b
}

// DefaultExpireRecordAfter sets the builder-wide expiration default.
func (b *RowWriteBuilder) DefaultExpireRecordAfter(d time.Duration) *RowWriteBuilder {
	return b.DefaultExpireRecordAfterSeconds(int64(d / time.Second))
}

// WithTxn joins a transaction for every row.
func (b *RowWriteBuilder) WithTxn(txn *as.Txn) *RowWriteBuilder {
	b.txn, b.txnSet = txn, true
	return b
}

// Execute writes every row.
func (b *RowWriteBuilder) Execute() (*RecordStream, error) { return b.execute(nil) }

// ExecuteOnError writes every row with an explicit error disposition.
func (b *RowWriteBuilder) ExecuteOnError(onErr *OnError) (*RecordStream, error) {
	return b.execute(onErr)
}

// execute materializes the rows into a chain and runs it.
func (b *RowWriteBuilder) execute(onErr *OnError) (*RecordStream, error) {
	if b.pendingErr != nil {
		return nil, b.pendingErr
	}
	if len(b.binNames) == 0 {
		return nil, NewError(KindInvalidArgument, "Bins was never called")
	}
	if len(b.rows) == 0 {
		return nil, NewError(KindInvalidArgument, "no rows to write")
	}

	c := newChain(b.session)
	if b.txnSet {
		c.txn = b.txn
		c.txnSet = true
		c.txnOptOut = b.txn == nil
	}

	for i, row := range b.rows {
		key, err := b.ds.ID(row.id)
		if err != nil {
			return nil, NewError(KindInvalidArgument, "row %d: %s", i, err)
		}
		c.startSegment(b.verb, []*as.Key{key}, false)
		for j, name := range b.binNames {
			c.pushOp(as.PutOp(as.NewBin(name, row.values[j])))
		}
		c.current.generation = row.generation
		if row.ttlSeconds != nil {
			c.current.ttlSeconds = row.ttlSeconds
		} else if b.defaultTTL != nil {
			c.current.ttlSeconds = b.defaultTTL
		}
	}
	return c.execute(onErr)
}

// rowVerb opens a row-write builder.
func rowVerb(s *Session, ds *DataSet, verb opType) *RowWriteBuilder {
	b := &RowWriteBuilder{session: s, ds: ds, verb: verb}
	if ds == nil {
		b.pendingErr = NewError(KindInvalidArgument, "dataset must not be nil")
	}
	return b
}

// UpsertRows writes many records of the same shape, creating or updating.
func (s *Session) UpsertRows(ds *DataSet) *RowWriteBuilder { return rowVerb(s, ds, opUpsert) }

// InsertRows writes many records, failing on any that already exist.
func (s *Session) InsertRows(ds *DataSet) *RowWriteBuilder { return rowVerb(s, ds, opInsert) }

// UpdateRows updates many records, failing on any that are absent.
func (s *Session) UpdateRows(ds *DataSet) *RowWriteBuilder { return rowVerb(s, ds, opUpdate) }

// ReplaceRows replaces many records, removing bins that are not written.
func (s *Session) ReplaceRows(ds *DataSet) *RowWriteBuilder { return rowVerb(s, ds, opReplace) }

// ReplaceIfExistsRows replaces many existing records only.
func (s *Session) ReplaceIfExistsRows(ds *DataSet) *RowWriteBuilder {
	return rowVerb(s, ds, opReplaceIfExists)
}

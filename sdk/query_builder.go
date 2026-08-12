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

// QueryBuilder builds the read side of a chain: point reads, batch reads and
// set-wide dataset queries.
type QueryBuilder struct {
	c *chain
}

// pushOp satisfies opSink.
func (q *QueryBuilder) pushOp(op *as.Operation) { q.c.pushOp(op) }

// chainRef satisfies opSink.
func (q *QueryBuilder) chainRef() *chain { return q.c }

// --- Projection ---

// Bins restricts the read to the named bins.
func (q *QueryBuilder) Bins(names ...string) *QueryBuilder {
	if len(names) == 0 {
		q.c.deferErr(NewError(KindInvalidArgument, "Bins needs at least one bin name"))
		return q
	}
	if q.c.requireCurrent("Bins") {
		q.c.current.binNames = names
	}
	return q
}

// WithNoBins reads only record headers.
func (q *QueryBuilder) WithNoBins() *QueryBuilder {
	if q.c.requireCurrent("WithNoBins") {
		q.c.current.noBins = true
	}
	return q
}

// AddOperation appends a raw core operation to the read.
func (q *QueryBuilder) AddOperation(op *as.Operation) *QueryBuilder {
	q.c.pushOp(op)
	return q
}

// WithOpProjection reads through the given operations instead of whole bins.
func (q *QueryBuilder) WithOpProjection(ops ...*as.Operation) *QueryBuilder {
	for _, op := range ops {
		q.c.pushOp(op)
	}
	return q
}

// WithWriteOperations sets the operations a background task applies.
func (q *QueryBuilder) WithWriteOperations(ops ...*as.Operation) *QueryBuilder {
	q.c.writeOperations = ops
	return q
}

// Bin descends into the read-only per-bin builder.
func (q *QueryBuilder) Bin(name string) *QueryBinBuilder {
	return &QueryBinBuilder{parent: q, bin: name}
}

// --- Filtering ---

// Where sets a server-side filter. It accepts a typed expression or Aerospike
// Expression Language source text, which the server compiles (8.1.3+).
func (q *QueryBuilder) Where[P Predicate](pred P) *QueryBuilder {
	p, err := resolvePredicate(pred)
	if err != nil {
		q.c.deferErr(err)
		return q
	}
	if q.c.requireCurrent("Where") {
		q.c.current.filter = p
	}
	return q
}

// DefaultWhere sets the filter for every segment that does not set its own.
func (q *QueryBuilder) DefaultWhere[P Predicate](pred P) *QueryBuilder {
	p, err := resolvePredicate(pred)
	if err != nil {
		q.c.deferErr(err)
		return q
	}
	q.c.defaultFilter = p
	return q
}

// FilterExpression is an alias for [QueryBuilder.Where] taking a typed
// expression.
func (q *QueryBuilder) FilterExpression(exp *as.Expression) *QueryBuilder {
	return q.Where(exp)
}

// Filter adds a secondary-index filter. It requires a matching index.
func (q *QueryBuilder) Filter(f *as.Filter) *QueryBuilder {
	if f == nil {
		q.c.deferErr(NewError(KindInvalidArgument, "filter must not be nil"))
		return q
	}
	q.c.filters = append(q.c.filters, f)
	return q
}

// --- Partitioning and throughput ---

// Partition scopes the query to a partition filter.
func (q *QueryBuilder) Partition(pf *as.PartitionFilter) *QueryBuilder {
	q.c.partitionFilter = pf
	return q
}

// OnPartition scopes the query to one partition.
func (q *QueryBuilder) OnPartition(id int) *QueryBuilder {
	q.c.partitionFilter = as.NewPartitionFilterById(id)
	return q
}

// OnPartitionRange scopes the query to a half-open partition range.
func (q *QueryBuilder) OnPartitionRange(begin, end int) *QueryBuilder {
	if end <= begin {
		q.c.deferErr(NewError(KindInvalidArgument,
			"partition range end (%d) must be greater than begin (%d)", end, begin))
		return q
	}
	q.c.partitionFilter = as.NewPartitionFilterByRange(begin, end-begin)
	return q
}

// MaxRecords caps the number of records returned.
func (q *QueryBuilder) MaxRecords(n int64) *QueryBuilder {
	if n <= 0 {
		q.c.deferErr(NewError(KindInvalidArgument, "MaxRecords must be positive"))
		return q
	}
	q.c.maxRecords = n
	return q
}

// Limit is an alias for [QueryBuilder.MaxRecords].
func (q *QueryBuilder) Limit(n int64) *QueryBuilder { return q.MaxRecords(n) }

// RecordsPerSecond throttles a dataset query server-side.
func (q *QueryBuilder) RecordsPerSecond(n int) *QueryBuilder {
	q.c.recordsPerSecond = n
	return q
}

// ChunkSize turns a dataset query into a paged cursor. Advance it with
// [RecordStream.HasMoreChunks].
func (q *QueryBuilder) ChunkSize(n int64) *QueryBuilder {
	if n <= 0 {
		q.c.deferErr(NewError(KindInvalidArgument, "ChunkSize must be positive"))
		return q
	}
	q.c.chunkSize = n
	return q
}

// ReadTouchTTLPercent overrides the Behavior setting for this call: the server
// resets a read record's expiration to that percentage of the expiration its
// last write set. Use -1 to never reset and 0 for the server default.
func (q *QueryBuilder) ReadTouchTTLPercent(pct int32) *QueryBuilder {
	q.c.readTouchTTLPct = &pct
	return q
}

// --- Row semantics ---

// IncludeMissingKeys emits rows for keys that were not found.
func (q *QueryBuilder) IncludeMissingKeys() *QueryBuilder {
	q.c.respondAllKeys = true
	return q
}

// RespondAllKeys is an alias for [QueryBuilder.IncludeMissingKeys].
func (q *QueryBuilder) RespondAllKeys() *QueryBuilder { return q.IncludeMissingKeys() }

// FailOnFilteredOut surfaces filtered records instead of dropping them.
func (q *QueryBuilder) FailOnFilteredOut() *QueryBuilder {
	q.c.failOnFilteredOut = true
	return q
}

// WithTxn joins a transaction, or leaves the ambient one when txn is nil.
func (q *QueryBuilder) WithTxn(txn *as.Txn) *QueryBuilder {
	q.c.txn = txn
	q.c.txnSet = true
	q.c.txnOptOut = txn == nil
	return q
}

// --- Chain-wide expiration defaults ---

// DefaultExpireRecordAfterSeconds sets the expiration for segments that do not
// set their own.
func (q *QueryBuilder) DefaultExpireRecordAfterSeconds(seconds int64) *QueryBuilder {
	q.c.defaultTTLSeconds = &seconds
	return q
}

// DefaultExpireRecordAfter sets the default expiration from a duration.
func (q *QueryBuilder) DefaultExpireRecordAfter(d time.Duration) *QueryBuilder {
	return q.DefaultExpireRecordAfterSeconds(int64(d / time.Second))
}

// DefaultNeverExpire makes the default expiration "never".
func (q *QueryBuilder) DefaultNeverExpire() *QueryBuilder {
	v := ttlNeverExpire
	q.c.defaultTTLSeconds = &v
	return q
}

// DefaultWithNoChangeInExpiration leaves expirations untouched by default.
func (q *QueryBuilder) DefaultWithNoChangeInExpiration() *QueryBuilder {
	v := ttlDontUpdate
	q.c.defaultTTLSeconds = &v
	return q
}

// DefaultExpiryFromServerDefault applies the namespace default by default.
func (q *QueryBuilder) DefaultExpiryFromServerDefault() *QueryBuilder {
	v := ttlServerDefault
	q.c.defaultTTLSeconds = &v
	return q
}

// ExpireRecordAfterSeconds sets the expiration a background touch applies.
func (q *QueryBuilder) ExpireRecordAfterSeconds(seconds int64) *QueryBuilder {
	q.c.setTTLSeconds(seconds)
	return q
}

// --- Segment transitions ---

// Query stacks another read segment.
func (q *QueryBuilder) Query[T QueryTarget](target T) *QueryBuilder {
	return chainQuery(q.c, target)
}

// Upsert finalizes this segment and starts an upsert segment.
func (q *QueryBuilder) Upsert[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opUpsert, target)
}

// Insert finalizes this segment and starts an insert segment.
func (q *QueryBuilder) Insert[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opInsert, target)
}

// Update finalizes this segment and starts an update segment.
func (q *QueryBuilder) Update[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opUpdate, target)
}

// Replace finalizes this segment and starts a replace segment.
func (q *QueryBuilder) Replace[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opReplace, target)
}

// ReplaceIfExists finalizes this segment and starts a replace-only segment.
func (q *QueryBuilder) ReplaceIfExists[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opReplaceIfExists, target)
}

// Delete finalizes this segment and starts a delete segment.
func (q *QueryBuilder) Delete[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opDelete, target)
}

// Touch finalizes this segment and starts a touch segment.
func (q *QueryBuilder) Touch[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opTouch, target)
}

// Exists finalizes this segment and starts an existence-check segment.
func (q *QueryBuilder) Exists[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(q.c, opExists, target)
}

// ExecuteUDF finalizes this segment and starts a user-defined function segment.
func (q *QueryBuilder) ExecuteUDF[T WriteTarget](target T) *UDFFunctionBuilder {
	return chainUDF(q.c, target)
}

// --- Terminals ---

// Execute runs the chain, buffering the results.
func (q *QueryBuilder) Execute() (*RecordStream, error) { return q.c.execute(nil) }

// ExecuteOnError runs the chain with an explicit error disposition.
func (q *QueryBuilder) ExecuteOnError(onErr *OnError) (*RecordStream, error) {
	return q.c.execute(onErr)
}

// Stream runs the chain lazily. Rows arrive as nodes respond, in completion
// order rather than input order; use [RecordResult.Index] to recover the input
// position. There is no writes-complete-on-return guarantee.
//
// A dataset query is already lazy server-side, and a chain that requires
// sequencing cannot stream, so both fall back to [QueryBuilder.Execute].
func (q *QueryBuilder) Stream() (*RecordStream, error) { return q.c.stream(nil) }

// StreamOnError runs the chain lazily with an explicit error disposition.
func (q *QueryBuilder) StreamOnError(onErr *OnError) (*RecordStream, error) {
	return q.c.stream(onErr)
}

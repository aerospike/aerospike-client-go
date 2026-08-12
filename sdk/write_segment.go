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

// WriteSegmentBuilder builds one write segment of a chain.
//
// Every method returns the builder, so a chain reads top to bottom; the first
// argument error is held and surfaced from the terminal.
type WriteSegmentBuilder struct {
	c *chain
}

// pushOp satisfies opSink.
func (b *WriteSegmentBuilder) pushOp(op *as.Operation) { b.c.pushOp(op) }

// chainRef satisfies opSink.
func (b *WriteSegmentBuilder) chainRef() *chain { return b.c }

// --- Segment modifiers ---

// Where sets a server-side filter for this segment. It accepts a typed
// expression or Aerospike Expression Language source text.
func (b *WriteSegmentBuilder) Where[P Predicate](pred P) *WriteSegmentBuilder {
	p, err := resolvePredicate(pred)
	if err != nil {
		b.c.deferErr(err)
		return b
	}
	if b.c.requireCurrent("Where") {
		b.c.current.filter = p
	}
	return b
}

// ExpireRecordAfterSeconds sets an explicit expiration.
func (b *WriteSegmentBuilder) ExpireRecordAfterSeconds(seconds int64) *WriteSegmentBuilder {
	b.c.setTTLSeconds(seconds)
	return b
}

// ExpireRecordAfter sets an explicit expiration from a duration.
func (b *WriteSegmentBuilder) ExpireRecordAfter(d time.Duration) *WriteSegmentBuilder {
	return b.ExpireRecordAfterSeconds(int64(d / time.Second))
}

// ExpireRecordAt sets an absolute expiry, converted to a relative expiration
// immediately. A non-future instant is an error surfaced from the terminal.
func (b *WriteSegmentBuilder) ExpireRecordAt(t time.Time) *WriteSegmentBuilder {
	b.c.setExpireAt(t)
	return b
}

// NeverExpire keeps the record forever.
func (b *WriteSegmentBuilder) NeverExpire() *WriteSegmentBuilder {
	b.c.setTTLSeconds(ttlNeverExpire)
	return b
}

// WithNoChangeInExpiration leaves the current expiration untouched.
func (b *WriteSegmentBuilder) WithNoChangeInExpiration() *WriteSegmentBuilder {
	b.c.setTTLSeconds(ttlDontUpdate)
	return b
}

// ExpiryFromServerDefault applies the namespace default expiration.
func (b *WriteSegmentBuilder) ExpiryFromServerDefault() *WriteSegmentBuilder {
	b.c.setTTLSeconds(ttlServerDefault)
	return b
}

// EnsureGenerationIs guards the write with an optimistic lock.
func (b *WriteSegmentBuilder) EnsureGenerationIs(generation uint32) *WriteSegmentBuilder {
	if generation == 0 {
		b.c.deferErr(NewError(KindInvalidArgument, "generation guard must not be zero"))
		return b
	}
	if b.c.requireCurrent("EnsureGenerationIs") {
		b.c.current.generation = &generation
	}
	return b
}

// WithDurableDelete forces the durable-delete flag on.
func (b *WriteSegmentBuilder) WithDurableDelete() *WriteSegmentBuilder {
	if b.c.requireCurrent("WithDurableDelete") {
		b.c.current.durableDelete = BoolPtr(true)
	}
	return b
}

// WithoutDurableDelete forces the durable-delete flag off.
func (b *WriteSegmentBuilder) WithoutDurableDelete() *WriteSegmentBuilder {
	if b.c.requireCurrent("WithoutDurableDelete") {
		b.c.current.durableDelete = BoolPtr(false)
	}
	return b
}

// DefaultWithDurableDelete prefers durable delete while still honoring the
// Behavior's resolution.
func (b *WriteSegmentBuilder) DefaultWithDurableDelete() *WriteSegmentBuilder {
	if b.c.requireCurrent("DefaultWithDurableDelete") {
		b.c.current.durableDeleteDefault = BoolPtr(true)
	}
	return b
}

// DefaultWithoutDurableDelete prefers no durable delete while still honoring
// the Behavior's resolution.
func (b *WriteSegmentBuilder) DefaultWithoutDurableDelete() *WriteSegmentBuilder {
	if b.c.requireCurrent("DefaultWithoutDurableDelete") {
		b.c.current.durableDeleteDefault = BoolPtr(false)
	}
	return b
}

// IncludeMissingKeys emits rows for keys that were not found.
func (b *WriteSegmentBuilder) IncludeMissingKeys() *WriteSegmentBuilder {
	b.c.respondAllKeys = true
	return b
}

// RespondAllKeys is an alias for [WriteSegmentBuilder.IncludeMissingKeys].
func (b *WriteSegmentBuilder) RespondAllKeys() *WriteSegmentBuilder { return b.IncludeMissingKeys() }

// FailOnFilteredOut surfaces filtered records instead of dropping them.
func (b *WriteSegmentBuilder) FailOnFilteredOut() *WriteSegmentBuilder {
	b.c.failOnFilteredOut = true
	return b
}

// ReplaceOnly switches the current segment to replace-if-exists semantics.
func (b *WriteSegmentBuilder) ReplaceOnly() *WriteSegmentBuilder {
	if b.c.requireCurrent("ReplaceOnly") {
		b.c.current.verb = opReplaceIfExists
	}
	return b
}

// WithTxn joins a transaction, or leaves the ambient one when txn is nil.
func (b *WriteSegmentBuilder) WithTxn(txn *as.Txn) *WriteSegmentBuilder {
	b.c.txn = txn
	b.c.txnSet = true
	b.c.txnOptOut = txn == nil
	return b
}

// --- Operations ---

// Put writes each bin.
func (b *WriteSegmentBuilder) Put(bins ...*as.Bin) *WriteSegmentBuilder {
	for _, bin := range bins {
		b.c.pushOp(as.PutOp(bin))
	}
	return b
}

// SetBins is an alias for [WriteSegmentBuilder.Put].
func (b *WriteSegmentBuilder) SetBins(bins ...*as.Bin) *WriteSegmentBuilder { return b.Put(bins...) }

// SetTo sets a bin to a value.
func (b *WriteSegmentBuilder) SetTo(bin string, value any) *WriteSegmentBuilder {
	b.c.pushOp(as.PutOp(as.NewBin(bin, value)))
	return b
}

// SetBinsTo sets several bins from parallel name and value slices.
func (b *WriteSegmentBuilder) SetBinsTo(names []string, values []any) *WriteSegmentBuilder {
	if len(names) != len(values) {
		b.c.deferErr(NewError(KindInvalidArgument,
			"SetBinsTo needs one value per name: %d names, %d values", len(names), len(values)))
		return b
	}
	for i, n := range names {
		b.c.pushOp(as.PutOp(as.NewBin(n, values[i])))
	}
	return b
}

// Add performs a numeric add.
func (b *WriteSegmentBuilder) Add(bin string, value any) *WriteSegmentBuilder {
	b.c.pushOp(as.AddOp(as.NewBin(bin, value)))
	return b
}

// IncrementBy is an alias for [WriteSegmentBuilder.Add].
func (b *WriteSegmentBuilder) IncrementBy(bin string, value any) *WriteSegmentBuilder {
	return b.Add(bin, value)
}

// Append appends to a string bin.
func (b *WriteSegmentBuilder) Append(bin, value string) *WriteSegmentBuilder {
	b.c.pushOp(as.AppendOp(as.NewBin(bin, value)))
	return b
}

// Prepend prepends to a string bin.
func (b *WriteSegmentBuilder) Prepend(bin, value string) *WriteSegmentBuilder {
	b.c.pushOp(as.PrependOp(as.NewBin(bin, value)))
	return b
}

// Get reads a bin back within the same operation.
func (b *WriteSegmentBuilder) Get(bin string) *WriteSegmentBuilder {
	b.c.pushOp(as.GetBinOp(bin))
	return b
}

// RemoveBin deletes a bin by writing nil to it.
func (b *WriteSegmentBuilder) RemoveBin(bin string) *WriteSegmentBuilder {
	b.c.pushOp(as.PutOp(as.NewBin(bin, nil)))
	return b
}

// DeleteRecord deletes the record atomically with the other operations.
func (b *WriteSegmentBuilder) DeleteRecord() *WriteSegmentBuilder {
	b.c.pushOp(as.DeleteOp())
	if b.c.hasCurrent {
		b.c.current.containsRecordDelete = true
	}
	return b
}

// TouchRecord resets the expiration as part of the operation.
func (b *WriteSegmentBuilder) TouchRecord() *WriteSegmentBuilder {
	b.c.pushOp(as.TouchOp())
	return b
}

// AddOperation appends a raw core operation.
func (b *WriteSegmentBuilder) AddOperation(op *as.Operation) *WriteSegmentBuilder {
	b.c.pushOp(op)
	return b
}

// Bin descends into the per-bin builder.
func (b *WriteSegmentBuilder) Bin(name string) *WriteBinBuilder {
	return &WriteBinBuilder{parent: b, bin: name}
}

// --- Segment transitions ---

// Upsert finalizes this segment and starts an upsert segment.
func (b *WriteSegmentBuilder) Upsert[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opUpsert, target)
}

// Insert finalizes this segment and starts an insert segment.
func (b *WriteSegmentBuilder) Insert[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opInsert, target)
}

// Update finalizes this segment and starts an update segment.
func (b *WriteSegmentBuilder) Update[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opUpdate, target)
}

// Replace finalizes this segment and starts a replace segment.
func (b *WriteSegmentBuilder) Replace[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opReplace, target)
}

// ReplaceIfExists finalizes this segment and starts a replace-only segment.
func (b *WriteSegmentBuilder) ReplaceIfExists[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opReplaceIfExists, target)
}

// Delete finalizes this segment and starts a delete segment.
func (b *WriteSegmentBuilder) Delete[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opDelete, target)
}

// Touch finalizes this segment and starts a touch segment.
func (b *WriteSegmentBuilder) Touch[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opTouch, target)
}

// Exists finalizes this segment and starts an existence-check segment.
func (b *WriteSegmentBuilder) Exists[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opExists, target)
}

// Query finalizes this segment and starts a read segment.
func (b *WriteSegmentBuilder) Query[T QueryTarget](target T) *QueryBuilder {
	return chainQuery(b.c, target)
}

// ExecuteUDF finalizes this segment and starts a user-defined function segment.
func (b *WriteSegmentBuilder) ExecuteUDF[T WriteTarget](target T) *UDFFunctionBuilder {
	return chainUDF(b.c, target)
}

// chainWrite finalizes the current segment and opens a write segment.
func chainWrite[T WriteTarget](c *chain, verb opType, target T) *WriteSegmentBuilder {
	keys, single, err := resolveWriteTarget(target)
	if err != nil {
		c.deferErr(err)
		c.startSegment(verb, nil, false)
		return &WriteSegmentBuilder{c: c}
	}
	c.startSegment(verb, keys, single)
	return &WriteSegmentBuilder{c: c}
}

// chainQuery finalizes the current segment and opens a read segment.
func chainQuery[T QueryTarget](c *chain, target T) *QueryBuilder {
	resolved, err := resolveQueryTarget(target)
	if err != nil {
		c.deferErr(err)
		c.startSegment(opRead, nil, false)
		return &QueryBuilder{c: c}
	}
	if resolved.isDataset() {
		c.deferErr(NewError(KindInvalidArgument,
			"a dataset query cannot be chained after another segment"))
		c.startSegment(opRead, nil, false)
		return &QueryBuilder{c: c}
	}
	c.startSegment(opRead, resolved.keys, resolved.single)
	return &QueryBuilder{c: c}
}

// IntoQueryBuilder exposes the full query-builder surface on this chain.
func (b *WriteSegmentBuilder) IntoQueryBuilder() *QueryBuilder { return &QueryBuilder{c: b.c} }

// --- Terminals ---

// Execute runs the chain, buffering the results. Writes are complete when it
// returns.
func (b *WriteSegmentBuilder) Execute() (*RecordStream, error) {
	return b.c.execute(nil)
}

// ExecuteOnError runs the chain with an explicit error disposition.
func (b *WriteSegmentBuilder) ExecuteOnError(onErr *OnError) (*RecordStream, error) {
	return b.c.execute(onErr)
}

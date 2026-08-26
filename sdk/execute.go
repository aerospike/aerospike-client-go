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

// Routing rule: a segment addressing exactly one key, expressed as one key, is
// always issued as a point operation through the core client's dedicated
// single-key call -- Get, GetHeader, Delete, Touch, Exists, Execute for a UDF,
// or Operate when the segment carries operations. It never becomes a one-row
// batch. executeSingle is the only place that decides which of those it is.
//
// A target expressed as a *slice* of keys stays on the batch path even when the
// slice holds one element, and that is deliberate rather than an oversight. The
// `single` flag also selects the error disposition (see resolveDisposition): a
// point operation raises a missing record as an error, while a batch reports it
// as a row. Routing a one-element slice to the point call would therefore make
// behavior depend on length, so a loop over a variable-length key slice would
// raise at length one and not at length two. The caller's expression of the
// target fixes the contract; the length does not.

// execute runs the chain and buffers the results.
func (c *chain) execute(onErr *OnError) (*RecordStream, error) {
	if err := c.prepare(); err != nil {
		return nil, err
	}
	specs := c.allSpecs()

	// A dataset query is its own path.
	if c.dataset != nil {
		return c.executeDatasetQuery(specs)
	}

	// A one-key segment always takes the point-operation path, whatever its
	// verb: requiresSequential only exists to keep a UDF out of a *shared*
	// batch, and a lone segment has nothing to share with.
	if len(specs) == 1 && specs[0].single {
		return c.executeSingle(specs[0], onErr)
	}
	// A UDF segment forces sequential execution; otherwise a multi-segment
	// chain collapses into one batch.
	if c.requiresSequential(specs) {
		return c.executeSequential(specs, onErr)
	}

	// A multi-key write batch on a strong-consistency namespace is wrapped in
	// an implicit transaction, so its writes commit atomically.
	if c.implicitTxnApplies(specs) {
		return c.runInImplicitTxn(func(txn *as.Txn) (*RecordStream, error) {
			sub := *c
			sub.txn = txn
			sub.txnSet = true
			return sub.executeBatch(specs, onErr)
		})
	}
	return c.executeBatch(specs, onErr)
}

// stream runs the chain lazily.
//
// A dataset query is already lazy server-side, and a chain that needs
// sequencing or an implicit transaction cannot stream, so both fall back to the
// buffered path. Everything else produces rows as they arrive, in completion
// order rather than input order; RecordResult.Index recovers the input
// position.
func (c *chain) stream(onErr *OnError) (*RecordStream, error) {
	if err := c.prepare(); err != nil {
		return nil, err
	}
	specs := c.allSpecs()

	if c.dataset != nil {
		return c.executeDatasetQuery(specs)
	}
	// Checked before the sequential and implicit-transaction fallbacks: a
	// one-key segment is a point operation, and neither fallback has anything to
	// do for a single record.
	if len(specs) == 1 && specs[0].single {
		return c.executeSingle(specs[0], onErr)
	}
	if c.requiresSequential(specs) || c.implicitTxnApplies(specs) {
		return c.executeBatch(specs, onErr)
	}
	return c.streamBatch(specs, onErr)
}

// streamBatch runs the batch on a goroutine and hands rows over as they are
// produced, so a caller can start work before the whole batch lands.
func (c *chain) streamBatch(specs []operationSpec, onErr *OnError) (*RecordStream, error) {
	// The batch is validated on the calling goroutine, so an argument error is
	// still returned from the terminal rather than surfacing mid-iteration.
	core, err := c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	records := make([]as.BatchRecordIfc, 0, 16)
	owners := make([]operationSpec, 0, 16)
	for _, spec := range specs {
		recs, err := c.batchRecordsFor(spec)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			records = append(records, r)
			owners = append(owners, spec)
		}
	}
	if len(records) == 0 {
		return EmptyRecordStream(), nil
	}
	policy, err := c.batchPolicy()
	if err != nil {
		return nil, err
	}

	single := len(records) == 1 && specs[0].single
	disp, handler := resolveDisposition(onErr, single)

	rows := make(chan *RecordResult, len(records))
	errs := make(chan error, 1)

	go func() {
		defer close(rows)
		aerr := core.BatchOperate(policy, records)
		if aerr != nil && !hasPerRecordOutcomes(records) {
			if disp == dispRaise {
				errs <- WrapError(aerr)
				return
			}
		}
		for i, r := range records {
			row := batchRowAt(r, i)
			verb := owners[i].verb
			liftUDFResult(row, verb)

			if !row.IsOK() && c.isActionable(row.ResultCode, verb) {
				switch disp {
				case dispRaise:
					errs <- rowError(row)
					return
				case dispHandler:
					handler(row.Key, row.Index, rowError(row))
					continue
				}
			}
			if !c.shouldInclude(row.ResultCode, verb) {
				continue
			}
			rows <- row
		}
	}()

	return newChannelRecordStream(rows, errs), nil
}

// batchRowAt converts one core batch record into a row.
func batchRowAt(r as.BatchRecordIfc, index int) *RecordResult {
	br := r.BatchRec()
	row := &RecordResult{
		Key:        br.Key,
		Record:     br.Record,
		ResultCode: br.ResultCode,
		InDoubt:    br.InDoubt,
		Index:      int64(index),
	}
	if br.Err != nil {
		row.Err = WrapError(br.Err)
	}
	return row
}

// rowError builds the error for a failed row.
func rowError(row *RecordResult) *Error {
	if row.Err != nil {
		return row.Err
	}
	return ErrorFromResultCode(row.ResultCode, "", row.InDoubt)
}

// prepare validates the chain before any I/O.
func (c *chain) prepare() error {
	if c.pendingErr != nil {
		return c.pendingErr
	}
	if c.finalized {
		return NewError(KindInvalidArgument, "this chain has already been executed")
	}
	c.finalized = true

	// AEL text is refused before sending when any node is too old.
	if c.usesAEL() && !c.session.client.SupportsServerCompiledAEL() {
		return NewError(KindInvalidArgument,
			"Aerospike Expression Language filter text requires server 8.1.3 or newer on every node")
	}
	return nil
}

// usesAEL reports whether any segment carries AEL source text.
func (c *chain) usesAEL() bool {
	if c.defaultFilter.isAEL() {
		return true
	}
	if c.hasCurrent && c.current.filter.isAEL() {
		return true
	}
	for _, s := range c.specs {
		if s.filter.isAEL() {
			return true
		}
	}
	return false
}

// requiresSequential reports whether the chain cannot collapse into one batch.
func (c *chain) requiresSequential(specs []operationSpec) bool {
	for _, s := range specs {
		if s.verb == opUDF {
			return true
		}
	}
	return false
}

// filterExpression resolves a segment's filter into a core expression.
func (c *chain) filterExpression(spec operationSpec) (*as.Expression, error) {
	f := spec.filter
	if f.empty() {
		return nil, nil
	}
	if f.expression != nil {
		return f.expression, nil
	}
	// AEL source reaches the server as a two-element filter expression; the
	// core client exposes this through ExpAELVal.
	return aelExpression(f.ael)
}

// executeSingle runs a one-key segment through the point-operation path.
func (c *chain) executeSingle(spec operationSpec, onErr *OnError) (*RecordStream, error) {
	core, err := c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	key := spec.keys[0]
	disp, handler := resolveDisposition(onErr, true)

	// A one-key UDF is a point call, not a one-row batch. The core returns the
	// Lua value already extracted, so there is no SUCCESS bin to lift here --
	// see liftUDFResult, which does that job on the batch path.
	if spec.verb == opUDF {
		policy, err := c.writePolicy(spec)
		if err != nil {
			return nil, err
		}
		res, aerr := core.Execute(policy, key, spec.udfPackage, spec.udfFunction, spec.udfArgs...)
		if aerr != nil {
			return c.singleOutcome(key, nil, aerr, spec, disp, handler)
		}
		row := &RecordResult{Key: key, ResultCode: types.OK, Index: 0}
		if res != nil {
			row.UDFResult = as.NewValue(res)
		}
		return NewRecordStreamFromList([]*RecordResult{row}), nil
	}

	if spec.verb == opRead && len(spec.operations) == 0 {
		policy, err := c.readPolicy(spec)
		if err != nil {
			return nil, err
		}
		var rec *as.Record
		var aerr as.Error
		switch {
		case spec.noBins:
			rec, aerr = core.GetHeader(policy, key)
		case len(spec.binNames) > 0:
			rec, aerr = core.Get(policy, key, spec.binNames...)
		default:
			rec, aerr = core.Get(policy, key)
		}
		return c.singleOutcome(key, rec, aerr, spec, disp, handler)
	}

	policy, err := c.writePolicy(spec)
	if err != nil {
		return nil, err
	}
	ops := c.operationsFor(spec)
	if len(ops) == 0 {
		// Verbs that carry no explicit operations still need one.
		switch spec.verb {
		case opDelete:
			existed, aerr := core.Delete(policy, key)
			if aerr != nil {
				return c.singleOutcome(key, nil, aerr, spec, disp, handler)
			}
			rc := types.OK
			if !existed {
				rc = types.KEY_NOT_FOUND_ERROR
			}
			return NewRecordStreamFromList([]*RecordResult{
				{Key: key, ResultCode: rc, Index: 0},
			}), nil
		case opTouch:
			aerr := core.Touch(policy, key)
			return c.singleOutcome(key, nil, aerr, spec, disp, handler)
		case opExists:
			exists, aerr := core.Exists(&policy.BasePolicy, key)
			if aerr != nil {
				return c.singleOutcome(key, nil, aerr, spec, disp, handler)
			}
			rc := types.OK
			if !exists {
				rc = types.KEY_NOT_FOUND_ERROR
			}
			return NewRecordStreamFromList([]*RecordResult{
				{Key: key, ResultCode: rc, Index: 0},
			}), nil
		default:
			return nil, NewError(KindInvalidArgument, "write segment carries no operations")
		}
	}

	rec, aerr := core.Operate(policy, key, ops...)
	return c.singleOutcome(key, rec, aerr, spec, disp, handler)
}

// singleOutcome routes a single-key outcome through the disposition.
func (c *chain) singleOutcome(
	key *as.Key, rec *as.Record, aerr as.Error,
	spec operationSpec, disp disposition, handler ErrorHandler,
) (*RecordStream, error) {
	if aerr == nil {
		return NewRecordStreamFromList([]*RecordResult{
			{Key: key, Record: rec, ResultCode: types.OK, Index: 0},
		}), nil
	}

	e := WrapError(aerr)
	rc, _ := e.ResultCode()
	if !c.isActionable(rc, spec.verb) {
		if !c.shouldInclude(rc, spec.verb) {
			return EmptyRecordStream(), nil
		}
		return NewRecordStreamFromError(key, rc, e.InDoubt(), e), nil
	}

	switch disp {
	case dispRaise:
		return nil, e
	case dispHandler:
		handler(key, 0, e)
		return EmptyRecordStream(), nil
	default:
		return NewRecordStreamFromError(key, rc, e.InDoubt(), e), nil
	}
}

// executeBatch runs every segment as one batch.
func (c *chain) executeBatch(specs []operationSpec, onErr *OnError) (*RecordStream, error) {
	core, err := c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}

	records := make([]as.BatchRecordIfc, 0, 16)
	owners := make([]operationSpec, 0, 16)
	for _, spec := range specs {
		recs, err := c.batchRecordsFor(spec)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			records = append(records, r)
			owners = append(owners, spec)
		}
	}
	if len(records) == 0 {
		return EmptyRecordStream(), nil
	}

	policy, err := c.batchPolicy()
	if err != nil {
		return nil, err
	}

	single := len(records) == 1 && specs[0].single
	disp, handler := resolveDisposition(onErr, single)

	aerr := core.BatchOperate(policy, records)
	if aerr != nil {
		// A batch that fails as a call still carries per-row outcomes.
		if e := WrapError(aerr); disp == dispRaise && !hasPerRecordOutcomes(records) {
			return nil, e
		}
	}

	rows := make([]*RecordResult, 0, len(records))
	for i, r := range records {
		row := batchRowAt(r, i)
		verb := owners[i].verb
		liftUDFResult(row, verb)

		if !row.IsOK() && c.isActionable(row.ResultCode, verb) {
			switch disp {
			case dispRaise:
				if row.Err != nil {
					return nil, row.Err
				}
				return nil, ErrorFromResultCode(row.ResultCode, "", row.InDoubt)
			case dispHandler:
				e := row.Err
				if e == nil {
					e = ErrorFromResultCode(row.ResultCode, "", row.InDoubt)
				}
				handler(row.Key, row.Index, e)
				continue
			}
		}
		if !c.shouldInclude(row.ResultCode, verb) {
			continue
		}
		rows = append(rows, row)
	}
	return NewRecordStreamFromList(rows), nil
}

// hasPerRecordOutcomes reports whether any row carries its own result code.
func hasPerRecordOutcomes(records []as.BatchRecordIfc) bool {
	for _, r := range records {
		if r.BatchRec().ResultCode != types.OK {
			return true
		}
	}
	return false
}

// liftUDFResult moves a Lua return value out of the server's SUCCESS bin so a
// batch UDF row has the same shape as a single-key one.
func liftUDFResult(row *RecordResult, verb opType) {
	if verb != opUDF || row.Record == nil {
		return
	}
	if v, ok := row.Record.Bins["SUCCESS"]; ok {
		row.UDFResult = as.NewValue(v)
		row.Record = nil
		return
	}
	if len(row.Record.Bins) == 1 {
		for _, v := range row.Record.Bins {
			row.UDFResult = as.NewValue(v)
		}
		row.Record = nil
	}
}

// executeSequential runs segments one at a time and chains their streams.
func (c *chain) executeSequential(specs []operationSpec, onErr *OnError) (*RecordStream, error) {
	streams := make([]*RecordStream, 0, len(specs))
	for _, spec := range specs {
		sub := *c
		sub.specs = []operationSpec{spec}
		sub.hasCurrent = false
		sub.finalized = false
		var (
			st  *RecordStream
			err error
		)
		if spec.single {
			st, err = sub.executeSingle(spec, onErr)
		} else {
			st, err = sub.executeBatch([]operationSpec{spec}, onErr)
		}
		if err != nil {
			for _, s := range streams {
				s.Close()
			}
			return nil, err
		}
		streams = append(streams, st)
	}
	return ChainRecordStreams(streams...), nil
}

// batchRecordsFor builds the core batch records for one segment.
func (c *chain) batchRecordsFor(spec operationSpec) ([]as.BatchRecordIfc, error) {
	filter, err := c.filterExpression(spec)
	if err != nil {
		return nil, err
	}
	out := make([]as.BatchRecordIfc, 0, len(spec.keys))

	switch spec.verb {
	case opRead:
		policy, err := ToBatchReadPolicy(c.settings(OpRead, ShapeBatch))
		if err != nil {
			return nil, err
		}
		policy.FilterExpression = filter
		if c.readTouchTTLPct != nil {
			policy.ReadTouchTTLPercent = *c.readTouchTTLPct
		}
		for _, k := range spec.keys {
			switch {
			case len(spec.operations) > 0:
				out = append(out, as.NewBatchReadOps(policy, k, spec.operations...))
			case spec.noBins:
				out = append(out, as.NewBatchReadHeader(policy, k))
			case len(spec.binNames) > 0:
				out = append(out, as.NewBatchRead(policy, k, spec.binNames))
			default:
				out = append(out, as.NewBatchRead(policy, k, nil))
			}
		}

	case opDelete:
		policy := ToBatchDeletePolicy(c.settings(OpWriteNonRetryable, ShapeBatch))
		policy.FilterExpression = filter
		if spec.generation != nil {
			policy.Generation = *spec.generation
			policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		}
		policy.DurableDelete = ResolveDurableDelete(
			c.settings(OpWriteNonRetryable, ShapeBatch).DurableDelete,
			spec.durableDeleteDefault, spec.durableDelete)
		for _, k := range spec.keys {
			out = append(out, as.NewBatchDelete(policy, k))
		}

	case opExists:
		// An existence check is a header read, not a write. Building it as a
		// write makes the server answer NO_RESPONSE for every row in the batch,
		// not just this segment's.
		policy, err := ToBatchReadPolicy(c.settings(OpRead, ShapeBatch))
		if err != nil {
			return nil, err
		}
		policy.FilterExpression = filter
		for _, k := range spec.keys {
			out = append(out, as.NewBatchReadHeader(policy, k))
		}

	case opUDF:
		policy := ToBatchUDFPolicy(c.settings(OpWriteNonRetryable, ShapeBatch))
		policy.FilterExpression = filter
		policy.Expiration = expirationFor(spec.ttlSeconds)
		for _, k := range spec.keys {
			out = append(out, as.NewBatchUDF(policy, k, spec.udfPackage, spec.udfFunction, spec.udfArgs...))
		}

	default:
		policy := ToBatchWritePolicy(c.settings(OpWriteNonRetryable, ShapeBatch))
		policy.FilterExpression = filter
		policy.RecordExistsAction = spec.verb.recordExistsAction()
		policy.Expiration = expirationFor(spec.ttlSeconds)
		if spec.generation != nil {
			policy.Generation = *spec.generation
			policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		}
		policy.DurableDelete = ResolveDurableDelete(
			c.settings(OpWriteNonRetryable, ShapeBatch).DurableDelete,
			spec.durableDeleteDefault, spec.durableDelete)

		ops := c.operationsFor(spec)
		if len(ops) == 0 {
			return nil, NewError(KindInvalidArgument,
				"write segment for %d key(s) carries no operations", len(spec.keys))
		}
		for _, k := range spec.keys {
			out = append(out, as.NewBatchWrite(policy, k, ops...))
		}
	}
	return out, nil
}

// operationsFor returns a segment's operations, injecting the implicit one for
// verbs that carry none.
func (c *chain) operationsFor(spec operationSpec) []*as.Operation {
	ops := spec.operations
	if len(ops) > 0 {
		return ops
	}
	switch spec.verb {
	case opTouch:
		return []*as.Operation{as.TouchOp()}
	case opDelete:
		return []*as.Operation{as.DeleteOp()}
	case opExists:
		return []*as.Operation{as.GetHeaderOp()}
	}
	return ops
}

// readPolicy resolves the point read policy for a segment.
func (c *chain) readPolicy(spec operationSpec) (*as.BasePolicy, error) {
	p, err := ToReadPolicy(c.settings(OpRead, ShapePoint))
	if err != nil {
		return nil, err
	}
	if f, err := c.filterExpression(spec); err != nil {
		return nil, err
	} else if f != nil {
		p.FilterExpression = f
	}
	if c.readTouchTTLPct != nil {
		p.ReadTouchTTLPercent = *c.readTouchTTLPct
	}
	if c.txn != nil && !c.txnOptOut {
		p.Txn = c.txn
	}
	return p, nil
}

// writePolicy resolves the point write policy for a segment.
func (c *chain) writePolicy(spec operationSpec) (*as.WritePolicy, error) {
	settings := c.settings(OpWriteNonRetryable, ShapePoint)
	p, err := ToWritePolicy(settings)
	if err != nil {
		return nil, err
	}
	p.RecordExistsAction = spec.verb.recordExistsAction()
	p.Expiration = expirationFor(spec.ttlSeconds)
	if spec.generation != nil {
		p.Generation = *spec.generation
		p.GenerationPolicy = as.EXPECT_GEN_EQUAL
	}
	p.DurableDelete = ResolveDurableDelete(settings.DurableDelete, spec.durableDeleteDefault, spec.durableDelete)
	if f, err := c.filterExpression(spec); err != nil {
		return nil, err
	} else if f != nil {
		p.FilterExpression = f
	}
	if c.txn != nil && !c.txnOptOut {
		p.Txn = c.txn
	}
	return p, nil
}

// batchPolicy resolves the batch policy for the chain.
func (c *chain) batchPolicy() (*as.BatchPolicy, error) {
	p, err := ToBatchPolicy(c.settings(OpRead, ShapeBatch))
	if err != nil {
		return nil, err
	}
	p.RespondAllKeys = c.respondAllKeys
	if c.txn != nil && !c.txnOptOut {
		p.Txn = c.txn
	}
	if c.ctx != nil {
		base, err := applyCtxDeadline(&p.BasePolicy, c.ctx)
		if err != nil {
			return nil, err
		}
		p.BasePolicy = *base
	}
	return p, nil
}

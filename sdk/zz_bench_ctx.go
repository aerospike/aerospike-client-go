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

// Throwaway prototype for benchmarking the marginal cost of accepting
// context.Context on the existing fast paths (Get, InsertRows), against the
// existing no-context calls. Not part of the permanent API.

package sdk

import (
	"context"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// applyCtxDeadline returns policy unchanged if ctx carries no usable
// deadline (the cheap path -- true for context.Background(), context.TODO(),
// and any ctx built only with WithValue on top of those: ctx.Deadline()
// returns ok=false immediately, no allocation). If ctx does carry a deadline
// tighter than the policy's own TotalTimeout, it returns a stamped copy with
// TotalTimeout lowered to match -- context can only tighten the operational
// ceiling, never loosen it.
func applyCtxDeadline(policy *as.BasePolicy, ctx context.Context) (*as.BasePolicy, error) {
	if ctx == nil {
		return policy, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, NewError(KindTimeout, "context: %v", err)
	}
	d, ok := ctx.Deadline()
	if !ok {
		return policy, nil
	}
	remaining := time.Until(d)
	if policy.TotalTimeout != 0 && remaining >= policy.TotalTimeout {
		return policy, nil
	}
	stamped := *policy
	stamped.TotalTimeout = remaining
	return &stamped, nil
}

// GetCtx is the context-aware counterpart to the existing package-level Get.
func GetCtx[B BinsArg](s *Session, ctx context.Context, key *as.Key, bins B) (*as.Record, error) {
	if key == nil {
		return nil, NewError(KindInvalidArgument, "key must not be nil")
	}
	names, headerOnly, err := resolveBins(bins)
	if err != nil {
		return nil, err
	}
	policy, err := s.readPolicyFor(key.Namespace())
	if err != nil {
		return nil, err
	}
	policy, err = applyCtxDeadline(policy, ctx)
	if err != nil {
		return nil, err
	}
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}

	var rec *as.Record
	var aerr as.Error
	switch {
	case headerOnly:
		rec, aerr = core.GetHeader(policy, key)
	case len(names) > 0:
		rec, aerr = core.Get(policy, key, names...)
	default:
		rec, aerr = core.Get(policy, key)
	}
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	if rec == nil {
		return nil, ErrorFromResultCode(keyNotFoundCode, "record not found", false)
	}
	return rec, nil
}

// ExecuteCtx is the context-aware counterpart to RowWriteBuilder.Execute --
// same InsertRows/UpsertRows/etc. builder, just with a deadline threaded
// into the batch policy the same way GetCtx does for reads.
func (b *RowWriteBuilder) ExecuteCtx(ctx context.Context) (*RecordStream, error) {
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
	c.ctx = ctx
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
	return c.execute(nil)
}
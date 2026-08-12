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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// TTL sentinels understood by the expiration setters.
const (
	// ttlNeverExpire keeps the record forever.
	ttlNeverExpire int64 = -1
	// ttlDontUpdate leaves the current expiration untouched.
	ttlDontUpdate int64 = -2
	// ttlServerDefault applies the namespace default.
	ttlServerDefault int64 = 0
)

// opType is the verb a chain segment carries. The zero value is a read.
type opType int

const (
	opRead opType = iota
	opUpsert
	opInsert
	opUpdate
	opReplace
	opReplaceIfExists
	opDelete
	opTouch
	opExists
	opUDF
)

// recordExistsAction maps a verb onto the server's record-exists semantics.
func (t opType) recordExistsAction() as.RecordExistsAction {
	switch t {
	case opInsert:
		return as.CREATE_ONLY
	case opUpdate:
		return as.UPDATE_ONLY
	case opReplace:
		return as.REPLACE
	case opReplaceIfExists:
		return as.REPLACE_ONLY
	default:
		return as.UPDATE
	}
}

// requiresExistingKey reports whether a missing record is a real failure for
// this verb. It decides whether KEY_NOT_FOUND is actionable.
func (t opType) requiresExistingKey() bool {
	return t == opUpdate || t == opReplaceIfExists
}

// isWrite reports whether the verb writes.
func (t opType) isWrite() bool {
	switch t {
	case opRead, opExists:
		return false
	default:
		return true
	}
}

// operationSpec is one segment of a chain: a verb applied to a set of keys
// with its own operations, guards and modifiers.
type operationSpec struct {
	keys       []*as.Key
	single     bool
	operations []*as.Operation
	binNames   []string
	noBins     bool

	verb opType

	filter     resolvedPredicate
	generation *uint32
	ttlSeconds *int64

	durableDelete        *bool
	durableDeleteDefault *bool

	containsRecordDelete bool

	udfPackage  string
	udfFunction string
	udfArgs     []as.Value
}

// chain accumulates the segments of one fluent chain. Every public builder is
// a view over this type.
type chain struct {
	session *Session

	// specs holds the finalized segments; current is the segment being built.
	specs      []operationSpec
	current    operationSpec
	hasCurrent bool

	// Chain-wide defaults, applied to segments that do not override them.
	defaultFilter     resolvedPredicate
	defaultTTLSeconds *int64

	// Dataset query state.
	dataset          *DataSet
	filters          []*as.Filter
	partitionFilter  *as.PartitionFilter
	maxRecords       int64
	recordsPerSecond int
	chunkSize        int64

	failOnFilteredOut bool
	respondAllKeys    bool
	readTouchTTLPct   *int32

	writeOperations []*as.Operation

	// txnSet distinguishes "no transaction chosen" from "explicitly none",
	// which also disables the implicit batch-write transaction.
	txn       *as.Txn
	txnSet    bool
	txnOptOut bool

	// pendingErr holds the first argument error; terminals surface it.
	pendingErr error
	// finalized guards against reusing a chain after a terminal ran.
	finalized bool
}

// newChain starts a chain on a session.
func newChain(s *Session) *chain {
	c := &chain{session: s}
	if s.txn != nil {
		c.txn = s.txn
		c.txnSet = true
	}
	return c
}

// deferErr records the first error. It returns the chain for convenience.
func (c *chain) deferErr(err error) *chain {
	if err != nil && c.pendingErr == nil {
		c.pendingErr = err
	}
	return c
}

// namespace reports the namespace the chain addresses, for policy resolution.
func (c *chain) namespace() string {
	if c.dataset != nil {
		return c.dataset.namespace
	}
	if c.hasCurrent && len(c.current.keys) > 0 {
		return c.current.keys[0].Namespace()
	}
	for _, s := range c.specs {
		if len(s.keys) > 0 {
			return s.keys[0].Namespace()
		}
	}
	return ""
}

// setName reports the set the chain addresses.
func (c *chain) setName() string {
	if c.dataset != nil {
		return c.dataset.setName
	}
	if c.hasCurrent && len(c.current.keys) > 0 {
		return c.current.keys[0].SetName()
	}
	return ""
}

// mode resolves the namespace consistency mode.
func (c *chain) mode() Mode { return c.session.mode(c.namespace()) }

// settings resolves the behavior settings for a point of the operation space.
func (c *chain) settings(kind OpKind, shape OpShape) Settings {
	return c.session.behavior.Settings(kind, shape, c.mode())
}

// pushOp appends an operation to the current segment. It satisfies opSink.
func (c *chain) pushOp(op *as.Operation) {
	if !c.hasCurrent {
		c.deferErr(NewError(KindInvalidArgument, "no active segment to add an operation to"))
		return
	}
	c.current.operations = append(c.current.operations, op)
}

// startSegment finalizes any current segment and opens a new one.
func (c *chain) startSegment(verb opType, keys []*as.Key, single bool) {
	c.finalizeCurrent()
	c.current = operationSpec{verb: verb, keys: keys, single: single}
	c.hasCurrent = true
}

// finalizeCurrent moves the current segment into the finalized list.
func (c *chain) finalizeCurrent() {
	if !c.hasCurrent {
		return
	}
	spec := c.current
	if spec.filter.empty() {
		spec.filter = c.defaultFilter
	}
	if spec.ttlSeconds == nil {
		spec.ttlSeconds = c.defaultTTLSeconds
	}
	c.specs = append(c.specs, spec)
	c.current = operationSpec{}
	c.hasCurrent = false
}

// allSpecs finalizes and returns every segment.
func (c *chain) allSpecs() []operationSpec {
	c.finalizeCurrent()
	return c.specs
}

// requireCurrent reports whether a segment is open, recording an error when
// not.
func (c *chain) requireCurrent(what string) bool {
	if !c.hasCurrent {
		c.deferErr(NewError(KindInvalidArgument, "%s must be called on an open segment", what))
		return false
	}
	return true
}

// setTTLSeconds records an explicit expiration on the current segment.
func (c *chain) setTTLSeconds(v int64) {
	if !c.requireCurrent("an expiration setter") {
		return
	}
	c.current.ttlSeconds = &v
}

// setExpireAt converts an absolute instant into a relative expiration.
func (c *chain) setExpireAt(t time.Time) {
	secs, err := secondsUntil(t)
	if err != nil {
		c.deferErr(err)
		return
	}
	c.setTTLSeconds(secs)
}

// secondsUntil converts a future instant into whole seconds, rounding up.
func secondsUntil(t time.Time) (int64, error) {
	d := time.Until(t)
	if d <= 0 {
		return 0, NewError(KindInvalidArgument, "expiration time must be in the future")
	}
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return secs, nil
}

// expirationFor converts a segment's TTL into the wire representation.
func expirationFor(ttl *int64) uint32 {
	if ttl == nil {
		return uint32(as.TTLServerDefault)
	}
	switch *ttl {
	case ttlNeverExpire:
		return uint32(as.TTLDontExpire)
	case ttlDontUpdate:
		return uint32(as.TTLDontUpdate)
	case ttlServerDefault:
		return uint32(as.TTLServerDefault)
	default:
		return uint32(*ttl)
	}
}

// isActionable reports whether a result code is a failure worth routing for
// this verb.
//
// KEY_NOT_FOUND matters only for verbs that require an existing record;
// FILTERED_OUT matters only when the caller asked to see filtered records.
func (c *chain) isActionable(rc types.ResultCode, verb opType) bool {
	switch rc {
	case types.OK:
		return false
	case types.KEY_NOT_FOUND_ERROR:
		return verb.requiresExistingKey()
	case types.FILTERED_OUT:
		return c.failOnFilteredOut
	default:
		return true
	}
}

// shouldInclude reports whether a row belongs in the stream.
//
// A delete always publishes its not-found rows, because deleting an absent key
// is a benign per-row outcome rather than an omitted row. Reads need an
// explicit request.
func (c *chain) shouldInclude(rc types.ResultCode, verb opType) bool {
	switch rc {
	case types.OK:
		return true
	case types.KEY_NOT_FOUND_ERROR:
		return c.respondAllKeys || verb == opDelete
	case types.FILTERED_OUT:
		return c.failOnFilteredOut || c.respondAllKeys
	default:
		return true
	}
}

// opSink receives operations from a bin or CDT builder.
type opSink interface {
	pushOp(op *as.Operation)
	chainRef() *chain
}

// chainRef satisfies opSink.
func (c *chain) chainRef() *chain { return c }

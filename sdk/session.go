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
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// keyNotFoundCode is the result code for a missing record.
const keyNotFoundCode = types.KEY_NOT_FOUND_ERROR

// cachedPolicies holds the point-operation policies a session resolves once
// and reuses. The whole struct is swapped atomically, so a reader either sees
// the old set or the new one, never a mix.
type cachedPolicies struct {
	readAP  *as.BasePolicy
	readSC  *as.BasePolicy
	writeAP *as.WritePolicy
	writeSC *as.WritePolicy

	// generation records the behavior cache generation these policies were
	// resolved from.
	generation uint64
}

// Session binds a [Behavior] and is the entry point for data operations.
//
// A Session is cheap to copy by pointer and safe for concurrent use. Create
// many per cluster.
type Session struct {
	client   *Client
	behavior *Behavior

	cached  atomic.Pointer[cachedPolicies]
	rebuild sync.Mutex

	// txn is the ambient transaction, set on sessions minted by
	// [Session.Transaction].
	txn *as.Txn
}

// newSession resolves the base policies and returns a session.
func newSession(c *Client, b *Behavior, txn *as.Txn) (*Session, error) {
	s := &Session{client: c, behavior: b, txn: txn}
	if _, err := s.policies(); err != nil {
		return nil, err
	}
	return s, nil
}

// policies returns the cached policy set, rebuilding it when the behavior's
// cache generation has moved (a configuration reload).
func (s *Session) policies() (*cachedPolicies, error) {
	gen := s.behavior.Generation()
	if c := s.cached.Load(); c != nil && c.generation == gen {
		return c, nil
	}

	s.rebuild.Lock()
	defer s.rebuild.Unlock()
	// Another goroutine may have rebuilt while we waited.
	if c := s.cached.Load(); c != nil && c.generation == s.behavior.Generation() {
		return c, nil
	}

	gen = s.behavior.Generation()
	readAP, err := ToReadPolicy(s.behavior.Settings(OpRead, ShapePoint, ModeAP))
	if err != nil {
		return nil, err
	}
	readSC, err := ToReadPolicy(s.behavior.Settings(OpRead, ShapePoint, ModeSC))
	if err != nil {
		return nil, err
	}
	writeAP, err := ToWritePolicy(s.behavior.Settings(OpWriteNonRetryable, ShapePoint, ModeAP))
	if err != nil {
		return nil, err
	}
	writeSC, err := ToWritePolicy(s.behavior.Settings(OpWriteNonRetryable, ShapePoint, ModeSC))
	if err != nil {
		return nil, err
	}

	c := &cachedPolicies{
		readAP: readAP, readSC: readSC,
		writeAP: writeAP, writeSC: writeSC,
		generation: gen,
	}
	s.cached.Store(c)
	return c, nil
}

// Behavior reports the behavior this session is bound to.
func (s *Session) Behavior() *Behavior { return s.behavior }

// Client reports the SDK client behind the session.
func (s *Session) Client() *Client { return s.client }

// CurrentTransaction reports the ambient transaction, or nil when the session
// is not transactional.
func (s *Session) CurrentTransaction() *as.Txn { return s.txn }

// SessionFor derives a sibling session on the same cluster with a different
// behavior. A nil behavior selects [DefaultBehavior]. The current session is
// unaffected.
func (s *Session) SessionFor(b *Behavior) (*Session, error) {
	if b == nil {
		b = DefaultBehavior()
	}
	return newSession(s.client, b, s.txn)
}

// mode resolves a namespace's consistency mode.
func (s *Session) mode(namespace string) Mode { return s.client.namespaceMode(namespace) }

// readPolicyFor picks the cached read policy for a namespace, deriving a
// transaction-stamped copy when a transaction is active.
func (s *Session) readPolicyFor(namespace string) (*as.BasePolicy, error) {
	c, err := s.policies()
	if err != nil {
		return nil, err
	}
	base := c.readAP
	if s.mode(namespace) == ModeSC {
		base = c.readSC
	}
	if s.txn == nil {
		return base, nil
	}
	stamped := *base
	stamped.Txn = s.txn
	return &stamped, nil
}

// writePolicyFor picks the cached write policy for a namespace, deriving a
// transaction-stamped copy when a transaction is active.
func (s *Session) writePolicyFor(namespace string) (*as.WritePolicy, error) {
	c, err := s.policies()
	if err != nil {
		return nil, err
	}
	base := c.writeAP
	if s.mode(namespace) == ModeSC {
		base = c.writeSC
	}
	if s.txn == nil {
		return base, nil
	}
	stamped := *base
	stamped.Txn = s.txn
	return &stamped, nil
}

// settingsFor resolves the settings for a point of the operation space in the
// given namespace.
func (s *Session) settingsFor(kind OpKind, shape OpShape, namespace string) Settings {
	return s.behavior.Settings(kind, shape, s.mode(namespace))
}

// Get reads one record.
//
// The bin selector accepts [AllBins], [NoBins], or a []string projection --
// one method covers what the other SDKs express as an overload set. A missing
// record is an error on this path, not an empty result.
//
// This is the fast path: it bypasses the builder chain and the record stream
// entirely, so one call reaches the server.
func Get[B BinsArg](s *Session, key *as.Key, bins B) (*as.Record, error) {
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

// Put writes bins to one record.
//
// This is the fast path: it bypasses the builder chain and the record stream.
func (s *Session) Put(key *as.Key, bins as.BinMap) error {
	if key == nil {
		return NewError(KindInvalidArgument, "key must not be nil")
	}
	policy, err := s.writePolicyFor(key.Namespace())
	if err != nil {
		return err
	}
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return err
	}
	return wrapNilError(core.Put(policy, key, bins))
}

// Get is the method form of the package-level [Get].
//
// It uses a method type parameter so that one method accepts every bin
// selector shape, mirroring the Rust SDK's `impl Into<Bins>`.
func (s *Session) Get[B BinsArg](key *as.Key, bins B) (*as.Record, error) {
	return Get(s, key, bins)
}

// Truncate removes every record in a dataset, optionally only those older than
// beforeNanos (a wall-clock time in nanoseconds; zero truncates everything).
func (s *Session) Truncate(ds *DataSet, beforeNanos int64) error {
	if ds == nil {
		return NewError(KindInvalidArgument, "dataset must not be nil")
	}
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return err
	}
	var before *time.Time
	if beforeNanos > 0 {
		t := time.Unix(0, beforeNanos)
		before = &t
	}
	return wrapNilError(core.Truncate(nil, ds.namespace, ds.setName, before))
}

// wrapNilError converts a core error to an SDK error, preserving nil.
func wrapNilError(err as.Error) error {
	if err == nil {
		return nil
	}
	return WrapError(err)
}

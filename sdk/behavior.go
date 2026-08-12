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
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// opKey addresses one point of the operation space.
type opKey struct {
	kind  OpKind
	shape OpShape
	mode  Mode
}

// resolvedMatrix is the eagerly-computed settings for every point of the
// operation space plus the two system scopes. It is swapped wholesale, so
// readers observe a consistent snapshot without locking.
type resolvedMatrix struct {
	ops    map[opKey]Settings
	system map[Scope]Settings
}

// Behavior is an immutable, named bundle of operation policies.
//
// A Behavior resolves settings by scope, inherits from a parent, and caches the
// fully-resolved matrix eagerly, so lookups on the operation path are map reads
// behind a single atomic load.
//
// Behaviors are safe for concurrent use. Deriving never mutates the parent.
type Behavior struct {
	name string

	mu       sync.RWMutex
	patches  map[Scope]Settings
	parent   *Behavior
	children []*Behavior

	// resolved holds a *resolvedMatrix, replaced wholesale on recompute.
	resolved atomic.Pointer[resolvedMatrix]

	// generation increments on every recompute. Sessions compare the value
	// they last resolved against the current one and rebuild their cached
	// policies when it moves. This replaces the Rust implementation's
	// weak-reference push, which Go has no equivalent for.
	generation atomic.Uint64
}

// NewBehavior creates a behavior with the given patches and optional parent,
// registers it by name, and computes its resolution cache.
//
// Passing a nil parent makes a root behavior. The patches map is copied.
func NewBehavior(name string, patches map[Scope]Settings, parent *Behavior) *Behavior {
	b := &Behavior{
		name:    name,
		patches: make(map[Scope]Settings, len(patches)),
		parent:  parent,
	}
	for k, v := range patches {
		b.patches[k] = v
	}
	if parent != nil {
		parent.mu.Lock()
		parent.children = append(parent.children, b)
		parent.mu.Unlock()
	}
	b.buildCache()
	registerBehavior(b)
	return b
}

// Name reports the behavior's registered name.
func (b *Behavior) Name() string { return b.name }

// Parent reports the behavior this one derives from, or nil for a root.
func (b *Behavior) Parent() *Behavior {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.parent
}

// Children reports the behaviors derived from this one.
func (b *Behavior) Children() []*Behavior {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*Behavior, len(b.children))
	copy(out, b.children)
	return out
}

// resolveScopes layers this behavior's patches over the parent's already
// resolved value.
//
// Note the inheritance semantics, which match the Python SDK: a child's patches
// layer over the parent's fully *resolved* matrix, so a child's ScopeAll patch
// beats a parent's more specific scope.
func (b *Behavior) resolveScopes(order []Scope, parentResolved Settings) Settings {
	out := parentResolved
	for _, s := range order {
		if patch, ok := b.patches[s]; ok {
			out = mergeSettings(out, patch)
		}
	}
	return out
}

// buildCache recomputes this behavior's resolved matrix and cascades to
// children.
func (b *Behavior) buildCache() {
	b.mu.RLock()
	parent := b.parent
	b.mu.RUnlock()

	m := &resolvedMatrix{
		ops:    make(map[opKey]Settings, 18),
		system: make(map[Scope]Settings, 2),
	}

	kinds := []OpKind{OpRead, OpWriteRetryable, OpWriteNonRetryable}
	shapes := []OpShape{ShapePoint, ShapeBatch, ShapeQuery}
	modes := []Mode{ModeAP, ModeSC}

	b.mu.RLock()
	for _, k := range kinds {
		for _, s := range shapes {
			for _, mo := range modes {
				var base Settings
				if parent != nil {
					base = parent.Settings(k, s, mo)
				}
				m.ops[opKey{k, s, mo}] = b.resolveScopes(resolutionOrder(k, s, mo), base)
			}
		}
	}
	for _, sc := range []Scope{ScopeSystemTxnVerify, ScopeSystemTxnRoll} {
		var base Settings
		if parent != nil {
			base = parent.SystemSettingsFor(sc)
		}
		m.system[sc] = b.resolveScopes(systemResolutionOrder(sc), base)
	}
	b.mu.RUnlock()

	b.resolved.Store(m)
	b.generation.Add(1)

	for _, c := range b.Children() {
		c.buildCache()
	}
}

// Settings reports the resolved settings for a point of the operation space.
func (b *Behavior) Settings(kind OpKind, shape OpShape, mode Mode) Settings {
	m := b.resolved.Load()
	if m == nil {
		b.buildCache()
		m = b.resolved.Load()
	}
	return m.ops[opKey{kind, shape, mode}]
}

// SystemSettingsFor reports the resolved settings for a system scope. A
// non-system scope yields the zero Settings.
func (b *Behavior) SystemSettingsFor(scope Scope) Settings {
	if !scope.isSystem() {
		return Settings{}
	}
	m := b.resolved.Load()
	if m == nil {
		b.buildCache()
		m = b.resolved.Load()
	}
	return m.system[scope]
}

// Generation reports the behavior's cache generation. Sessions use it to
// detect that their cached policies are stale after a configuration reload.
func (b *Behavior) Generation() uint64 { return b.generation.Load() }

// DeriveWithChanges creates a child behavior with the given per-scope patches.
//
// The child's patches layer over the parent's fully resolved matrix, so a
// ScopeAll patch here overrides even a more specific scope on the parent.
func (b *Behavior) DeriveWithChanges(name string, patches map[Scope]Settings) *Behavior {
	return NewBehavior(name, patches, b)
}

// ReloadPatches replaces this behavior's patches and recomputes the cache,
// cascading to children. Existing holders of the behavior see the new values;
// sessions notice through the generation counter.
func (b *Behavior) ReloadPatches(patches map[Scope]Settings) {
	b.mu.Lock()
	b.patches = make(map[Scope]Settings, len(patches))
	for k, v := range patches {
		b.patches[k] = v
	}
	b.mu.Unlock()
	b.buildCache()
}

// ClearCache recomputes this behavior and cascades to children.
func (b *Behavior) ClearCache() { b.buildCache() }

// patchesSnapshot returns a copy of the behavior's own patches.
func (b *Behavior) patchesSnapshot() map[Scope]Settings {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[Scope]Settings, len(b.patches))
	for k, v := range b.patches {
		out[k] = v
	}
	return out
}

// FindBehavior searches this behavior and its descendants by name.
func (b *Behavior) FindBehavior(name string) *Behavior {
	if b.name == name {
		return b
	}
	for _, c := range b.Children() {
		if found := c.FindBehavior(name); found != nil {
			return found
		}
	}
	return nil
}

// Explain renders a diagnostic dump of the behavior's own patches and its
// resolved matrix. It is intended for debugging, not for parsing.
func (b *Behavior) Explain() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Behavior %q", b.name)
	if p := b.Parent(); p != nil {
		fmt.Fprintf(&sb, " (parent %q)", p.name)
	}
	sb.WriteString("\n  patches:\n")

	patches := b.patchesSnapshot()
	scopes := make([]Scope, 0, len(patches))
	for s := range patches {
		scopes = append(scopes, s)
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
	for _, s := range scopes {
		fmt.Fprintf(&sb, "    %-22s %s\n", s, describeSettings(patches[s]))
	}

	sb.WriteString("  resolved:\n")
	for _, k := range []OpKind{OpRead, OpWriteRetryable, OpWriteNonRetryable} {
		for _, sh := range []OpShape{ShapePoint, ShapeBatch, ShapeQuery} {
			for _, mo := range []Mode{ModeAP, ModeSC} {
				fmt.Fprintf(&sb, "    %s/%s/%s: %s\n",
					opKindName(k), opShapeName(sh), modeName(mo),
					describeSettings(b.Settings(k, sh, mo)))
			}
		}
	}
	return sb.String()
}

func opKindName(k OpKind) string {
	switch k {
	case OpRead:
		return "read"
	case OpWriteRetryable:
		return "write_retryable"
	default:
		return "write_non_retryable"
	}
}

func opShapeName(s OpShape) string {
	switch s {
	case ShapePoint:
		return "point"
	case ShapeBatch:
		return "batch"
	default:
		return "query"
	}
}

func modeName(m Mode) string {
	if m == ModeSC {
		return "sc"
	}
	return "ap"
}

// describeSettings renders only the fields that are set.
func describeSettings(s Settings) string {
	var parts []string
	if s.TotalTimeout != nil {
		parts = append(parts, fmt.Sprintf("total_timeout=%s", *s.TotalTimeout))
	}
	if s.SocketTimeout != nil {
		parts = append(parts, fmt.Sprintf("socket_timeout=%s", *s.SocketTimeout))
	}
	if s.RetryDelay != nil {
		parts = append(parts, fmt.Sprintf("retry_delay=%s", *s.RetryDelay))
	}
	if s.MaxRetries != nil {
		parts = append(parts, fmt.Sprintf("max_retries=%d", *s.MaxRetries))
	}
	if s.SendKey != nil {
		parts = append(parts, fmt.Sprintf("send_key=%t", *s.SendKey))
	}
	if s.DurableDelete != nil {
		parts = append(parts, fmt.Sprintf("durable_delete=%t", *s.DurableDelete))
	}
	if s.UseCompression != nil {
		parts = append(parts, fmt.Sprintf("use_compression=%t", *s.UseCompression))
	}
	if s.CompressionThreshold != nil {
		parts = append(parts, fmt.Sprintf("compression_threshold=%d", *s.CompressionThreshold))
	}
	if s.CommitLevel != nil {
		parts = append(parts, fmt.Sprintf("commit_level=%d", int(*s.CommitLevel)))
	}
	if s.Replica != nil {
		parts = append(parts, "replica="+replicaName(*s.Replica))
	}
	if s.ReadModeAP != nil {
		parts = append(parts, "read_mode_ap="+readModeAPName(*s.ReadModeAP))
	}
	if s.ReadModeSC != nil {
		parts = append(parts, "read_mode_sc="+readModeSCName(*s.ReadModeSC))
	}
	if s.MaxConcurrentNodes != nil {
		parts = append(parts, fmt.Sprintf("max_concurrent_nodes=%d", *s.MaxConcurrentNodes))
	}
	if s.RecordQueueSize != nil {
		parts = append(parts, fmt.Sprintf("record_queue_size=%d", *s.RecordQueueSize))
	}
	if s.AllowInline != nil {
		parts = append(parts, fmt.Sprintf("allow_inline=%t", *s.AllowInline))
	}
	if s.AllowInlineSSD != nil {
		parts = append(parts, fmt.Sprintf("allow_inline_ssd=%t", *s.AllowInlineSSD))
	}
	if s.ReadTouchTTLPercent != nil {
		parts = append(parts, fmt.Sprintf("read_touch_ttl_percent=%d", *s.ReadTouchTTLPercent))
	}
	if s.ErrorDetailVerbosity != nil {
		parts = append(parts, fmt.Sprintf("error_detail_verbosity=%d", *s.ErrorDetailVerbosity))
	}
	if s.SimulateXDRWrite != nil {
		parts = append(parts, fmt.Sprintf("simulate_xdr_write=%t", *s.SimulateXDRWrite))
	}
	if len(parts) == 0 {
		return "(empty)"
	}
	return strings.Join(parts, " ")
}

// The enum-valued settings render by name in Explain, because a raw ordinal
// tells a reader nothing.

func replicaName(r as.ReplicaPolicy) string {
	switch r {
	case as.MASTER:
		return "MASTER"
	case as.MASTER_PROLES:
		return "MASTER_PROLES"
	case as.SEQUENCE:
		return "SEQUENCE"
	case as.PREFER_RACK:
		return "PREFER_RACK"
	case as.RANDOM:
		return "RANDOM"
	default:
		return fmt.Sprintf("ReplicaPolicy(%d)", int(r))
	}
}

func readModeAPName(m as.ReadModeAP) string {
	switch m {
	case as.ReadModeAPOne:
		return "ONE"
	case as.ReadModeAPAll:
		return "ALL"
	default:
		return fmt.Sprintf("ReadModeAP(%d)", int(m))
	}
}

func readModeSCName(m as.ReadModeSC) string {
	switch m {
	case as.ReadModeSCSession:
		return "SESSION"
	case as.ReadModeSCLinearize:
		return "LINEARIZE"
	case as.ReadModeSCAllowReplica:
		return "ALLOW_REPLICA"
	case as.ReadModeSCAllowUnavailable:
		return "ALLOW_UNAVAILABLE"
	default:
		return fmt.Sprintf("ReadModeSC(%d)", int(m))
	}
}

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

// Scope names a slice of the operation space that a [Settings] patch applies
// to. Resolution layers the scopes from least to most specific, so the most
// specific scope that sets a field wins.
type Scope int

// The operation scopes, plus the two system scopes used by transaction
// commit and abort.
const (
	ScopeAll Scope = iota

	ScopeReads
	ScopeReadsPoint
	ScopeReadsBatch
	ScopeReadsQuery
	ScopeReadsAP
	ScopeReadsSC

	ScopeWrites
	ScopeWritesRetryable
	ScopeWritesNonRetryable
	ScopeWritesPoint
	ScopeWritesBatch
	ScopeWritesQuery
	ScopeWritesAP
	ScopeWritesSC

	ScopeSystemTxnVerify
	ScopeSystemTxnRoll
)

var scopeNames = map[Scope]string{
	ScopeAll:                "all",
	ScopeReads:              "reads",
	ScopeReadsPoint:         "reads_point",
	ScopeReadsBatch:         "reads_batch",
	ScopeReadsQuery:         "reads_query",
	ScopeReadsAP:            "reads_ap",
	ScopeReadsSC:            "reads_sc",
	ScopeWrites:             "writes",
	ScopeWritesRetryable:    "writes_retryable",
	ScopeWritesNonRetryable: "writes_non_retryable",
	ScopeWritesPoint:        "writes_point",
	ScopeWritesBatch:        "writes_batch",
	ScopeWritesQuery:        "writes_query",
	ScopeWritesAP:           "writes_ap",
	ScopeWritesSC:           "writes_sc",
	ScopeSystemTxnVerify:    "system_txn_verify",
	ScopeSystemTxnRoll:      "system_txn_roll",
}

// String implements fmt.Stringer, returning the canonical snake_case name.
func (s Scope) String() string {
	if n, ok := scopeNames[s]; ok {
		return n
	}
	return "unknown_scope"
}

// ScopeFromName converts a canonical snake_case scope name back to a [Scope].
func ScopeFromName(name string) (Scope, bool) {
	for s, n := range scopeNames {
		if n == name {
			return s, true
		}
	}
	return ScopeAll, false
}

// isSystem reports whether the scope is one of the transaction system scopes,
// which resolve as [ScopeAll, scope] only.
func (s Scope) isSystem() bool {
	return s == ScopeSystemTxnVerify || s == ScopeSystemTxnRoll
}

// OpKind is the retryability axis of the operation space.
type OpKind int

// The operation kinds.
const (
	OpRead OpKind = iota
	OpWriteRetryable
	OpWriteNonRetryable
)

// OpShape is the cardinality axis of the operation space.
type OpShape int

// The operation shapes.
const (
	ShapePoint OpShape = iota
	ShapeBatch
	ShapeQuery
)

// Mode is the namespace consistency axis of the operation space.
type Mode int

// The consistency modes.
const (
	// ModeAP is an availability-mode namespace.
	ModeAP Mode = iota
	// ModeSC is a strong-consistency namespace.
	ModeSC
)

// ErrorDetailVerbosity levels for [Settings.ErrorDetailVerbosity]. Higher
// levels include the lower ones and require server 8.1.3+.
const (
	// VerbosityNone requests no extended error detail.
	VerbosityNone uint8 = 0
	// VerbositySubCode requests the server error subcode.
	VerbositySubCode uint8 = 1
	// VerbosityMessage additionally requests the server message.
	VerbosityMessage uint8 = 2
	// VerbosityExpressionTrace additionally requests the expression trace.
	VerbosityExpressionTrace uint8 = 3
)

// Settings is a patch of operation policy values. Every field is a pointer;
// nil means "not set here -- inherit".
type Settings struct {
	TotalTimeout  *time.Duration
	SocketTimeout *time.Duration
	RetryDelay    *time.Duration
	MaxRetries    *int

	SendKey        *bool
	DurableDelete  *bool
	UseCompression *bool

	CommitLevel *as.CommitLevel
	Replica     *as.ReplicaPolicy
	ReadModeAP  *as.ReadModeAP
	ReadModeSC  *as.ReadModeSC

	CompressionThreshold *int
	MaxConcurrentNodes   *int
	RecordQueueSize      *int

	AllowInline    *bool
	AllowInlineSSD *bool

	// ReadTouchTTLPercent is -1 (never reset), 0 (server default), or 1..100.
	ReadTouchTTLPercent *int32

	// ErrorDetailVerbosity selects how much error detail the server returns.
	ErrorDetailVerbosity *uint8

	// SimulateXDRWrite sets the XDR bit on point writes and operates so the
	// server accounts them as XDR client traffic. Batch commands deliberately
	// do not carry it, matching the Java client.
	SimulateXDRWrite *bool
}

// mergeSettings layers an override over a base: every non-nil field of the
// override wins. Neither input is mutated.
func mergeSettings(base, override Settings) Settings {
	out := base
	if override.TotalTimeout != nil {
		out.TotalTimeout = override.TotalTimeout
	}
	if override.SocketTimeout != nil {
		out.SocketTimeout = override.SocketTimeout
	}
	if override.RetryDelay != nil {
		out.RetryDelay = override.RetryDelay
	}
	if override.MaxRetries != nil {
		out.MaxRetries = override.MaxRetries
	}
	if override.SendKey != nil {
		out.SendKey = override.SendKey
	}
	if override.DurableDelete != nil {
		out.DurableDelete = override.DurableDelete
	}
	if override.UseCompression != nil {
		out.UseCompression = override.UseCompression
	}
	if override.CommitLevel != nil {
		out.CommitLevel = override.CommitLevel
	}
	if override.Replica != nil {
		out.Replica = override.Replica
	}
	if override.ReadModeAP != nil {
		out.ReadModeAP = override.ReadModeAP
	}
	if override.ReadModeSC != nil {
		out.ReadModeSC = override.ReadModeSC
	}
	if override.CompressionThreshold != nil {
		out.CompressionThreshold = override.CompressionThreshold
	}
	if override.MaxConcurrentNodes != nil {
		out.MaxConcurrentNodes = override.MaxConcurrentNodes
	}
	if override.RecordQueueSize != nil {
		out.RecordQueueSize = override.RecordQueueSize
	}
	if override.AllowInline != nil {
		out.AllowInline = override.AllowInline
	}
	if override.AllowInlineSSD != nil {
		out.AllowInlineSSD = override.AllowInlineSSD
	}
	if override.ReadTouchTTLPercent != nil {
		out.ReadTouchTTLPercent = override.ReadTouchTTLPercent
	}
	if override.ErrorDetailVerbosity != nil {
		out.ErrorDetailVerbosity = override.ErrorDetailVerbosity
	}
	if override.SimulateXDRWrite != nil {
		out.SimulateXDRWrite = override.SimulateXDRWrite
	}
	return out
}

// resolutionOrder reports the scopes to layer, least to most specific, for a
// point in the operation space.
func resolutionOrder(kind OpKind, shape OpShape, mode Mode) []Scope {
	order := make([]Scope, 0, 5)
	order = append(order, ScopeAll)

	if kind == OpRead {
		order = append(order, ScopeReads)
		if mode == ModeAP {
			order = append(order, ScopeReadsAP)
		} else {
			order = append(order, ScopeReadsSC)
		}
		switch shape {
		case ShapePoint:
			order = append(order, ScopeReadsPoint)
		case ShapeBatch:
			order = append(order, ScopeReadsBatch)
		case ShapeQuery:
			order = append(order, ScopeReadsQuery)
		}
		return order
	}

	order = append(order, ScopeWrites)
	if mode == ModeAP {
		order = append(order, ScopeWritesAP)
	} else {
		order = append(order, ScopeWritesSC)
	}
	if kind == OpWriteRetryable {
		order = append(order, ScopeWritesRetryable)
	} else {
		order = append(order, ScopeWritesNonRetryable)
	}
	switch shape {
	case ShapePoint:
		order = append(order, ScopeWritesPoint)
	case ShapeBatch:
		order = append(order, ScopeWritesBatch)
	case ShapeQuery:
		order = append(order, ScopeWritesQuery)
	}
	return order
}

// systemResolutionOrder reports the scopes to layer for a system scope. The
// Reads and Writes families never apply to system operations.
func systemResolutionOrder(scope Scope) []Scope {
	return []Scope{ScopeAll, scope}
}

// Small helpers for building Settings literals without taking addresses of
// temporaries at every call site.

// DurationPtr returns a pointer to d.
func DurationPtr(d time.Duration) *time.Duration { return &d }

// IntPtr returns a pointer to i.
func IntPtr(i int) *int { return &i }

// Int32Ptr returns a pointer to i.
func Int32Ptr(i int32) *int32 { return &i }

// Uint8Ptr returns a pointer to u.
func Uint8Ptr(u uint8) *uint8 { return &u }

// BoolPtr returns a pointer to b.
func BoolPtr(b bool) *bool { return &b }

// ReplicaPtr returns a pointer to r.
func ReplicaPtr(r as.ReplicaPolicy) *as.ReplicaPolicy { return &r }

// CommitLevelPtr returns a pointer to c.
func CommitLevelPtr(c as.CommitLevel) *as.CommitLevel { return &c }

// ReadModeAPPtr returns a pointer to m.
func ReadModeAPPtr(m as.ReadModeAP) *as.ReadModeAP { return &m }

// ReadModeSCPtr returns a pointer to m.
func ReadModeSCPtr(m as.ReadModeSC) *as.ReadModeSC { return &m }

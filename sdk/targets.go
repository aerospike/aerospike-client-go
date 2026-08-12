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
)

// WriteTarget constrains what a write verb addresses: one key or many.
//
// This is the Go spelling of the other SDKs' overload set (Java) or
// Into-conversion (Rust `impl Into<WriteTarget>`): one method per verb accepts
// either cardinality, and anything else fails to compile.
type WriteTarget interface {
	*as.Key | []*as.Key
}

// QueryTarget constrains what a read addresses: one key, many keys, or a
// whole dataset (a set-wide index query or scan).
type QueryTarget interface {
	*as.Key | []*as.Key | *DataSet
}

// Predicate constrains a server-side filter: either a typed expression the
// client compiles, or Aerospike Expression Language source the *server*
// compiles (requires server 8.1.3+).
type Predicate interface {
	*as.Expression | string
}

// Bins selects which bins a read returns. Use [AllBins] or [NoBins]; a
// projection is expressed as a []string of bin names.
type Bins int

const (
	// AllBins reads every bin of the record.
	AllBins Bins = iota
	// NoBins reads only the record header (generation and expiration).
	NoBins
)

// BinsArg constrains the bin selector accepted by the read fast path: either a
// [Bins] sentinel or an explicit projection.
type BinsArg interface {
	Bins | []string
}

// resolveWriteTarget normalizes a write target to a key slice plus a flag
// recording whether the caller addressed a single key. The distinction is
// load-bearing: single-key errors are returned from the terminal by default,
// while batch errors are embedded in the stream.
func resolveWriteTarget[T WriteTarget](target T) (keys []*as.Key, single bool, err error) {
	switch t := any(target).(type) {
	case *as.Key:
		if t == nil {
			return nil, false, NewError(KindInvalidArgument, "write target key must not be nil")
		}
		return []*as.Key{t}, true, nil
	case []*as.Key:
		if len(t) == 0 {
			return nil, false, NewError(KindInvalidArgument, "write target key list must not be empty")
		}
		for i, k := range t {
			if k == nil {
				return nil, false, NewError(KindInvalidArgument, "write target key at index %d is nil", i)
			}
		}
		return t, false, nil
	}
	// Unreachable: the constraint admits no other type.
	return nil, false, NewError(KindInvalidArgument, "unsupported write target")
}

// resolvedQueryTarget is the normalized form of a [QueryTarget].
type resolvedQueryTarget struct {
	keys    []*as.Key
	dataset *DataSet
	single  bool
}

// isDataset reports whether the target is a set-wide query rather than keys.
func (r resolvedQueryTarget) isDataset() bool { return r.dataset != nil }

// resolveQueryTarget normalizes a read target.
func resolveQueryTarget[T QueryTarget](target T) (resolvedQueryTarget, error) {
	switch t := any(target).(type) {
	case *as.Key:
		if t == nil {
			return resolvedQueryTarget{}, NewError(KindInvalidArgument, "query target key must not be nil")
		}
		return resolvedQueryTarget{keys: []*as.Key{t}, single: true}, nil
	case []*as.Key:
		if len(t) == 0 {
			return resolvedQueryTarget{}, NewError(KindInvalidArgument, "query target key list must not be empty")
		}
		for i, k := range t {
			if k == nil {
				return resolvedQueryTarget{}, NewError(KindInvalidArgument, "query target key at index %d is nil", i)
			}
		}
		return resolvedQueryTarget{keys: t}, nil
	case *DataSet:
		if t == nil {
			return resolvedQueryTarget{}, NewError(KindInvalidArgument, "query target dataset must not be nil")
		}
		return resolvedQueryTarget{dataset: t}, nil
	}
	return resolvedQueryTarget{}, NewError(KindInvalidArgument, "unsupported query target")
}

// resolvedPredicate is a filter that is either a compiled expression or AEL
// source text awaiting server-side compilation.
type resolvedPredicate struct {
	expression *as.Expression
	ael        string
}

// isAEL reports whether the predicate is server-compiled AEL text.
func (p resolvedPredicate) isAEL() bool { return p.ael != "" }

// empty reports whether no filter was set.
func (p resolvedPredicate) empty() bool { return p.expression == nil && p.ael == "" }

// resolvePredicate normalizes a filter argument.
func resolvePredicate[P Predicate](pred P) (resolvedPredicate, error) {
	switch p := any(pred).(type) {
	case *as.Expression:
		if p == nil {
			return resolvedPredicate{}, NewError(KindInvalidArgument, "filter expression must not be nil")
		}
		return resolvedPredicate{expression: p}, nil
	case string:
		if p == "" {
			return resolvedPredicate{}, NewError(KindInvalidArgument, "AEL filter text must not be empty")
		}
		return resolvedPredicate{ael: p}, nil
	}
	return resolvedPredicate{}, NewError(KindInvalidArgument, "unsupported filter predicate")
}

// resolveBins normalizes a bin selector into the core client's representation.
// A nil slice means "all bins"; an empty non-nil slice means "header only".
func resolveBins[B BinsArg](bins B) (names []string, headerOnly bool, err error) {
	switch b := any(bins).(type) {
	case Bins:
		switch b {
		case AllBins:
			return nil, false, nil
		case NoBins:
			return nil, true, nil
		}
		return nil, false, NewError(KindInvalidArgument, "unknown Bins selector %d", int(b))
	case []string:
		if len(b) == 0 {
			return nil, true, nil
		}
		for i, n := range b {
			if n == "" {
				return nil, false, NewError(KindInvalidArgument, "bin name at index %d is empty", i)
			}
		}
		return b, false, nil
	}
	return nil, false, NewError(KindInvalidArgument, "unsupported bin selector")
}

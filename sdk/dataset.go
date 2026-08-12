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
	"encoding/hex"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// KeyValue constrains the Go types that are valid Aerospike user keys.
//
// Restricting the key type at compile time is what lets [DataSet.ID] and
// friends be infallible for the value itself: the core client's NewKey accepts
// `any` and must report an error for unsupported types, while every type
// admitted here is guaranteed to convert. Approximation constraints (~) admit
// named types whose underlying type is listed, so a `type UserID int64` works.
type KeyValue interface {
	~string |
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 |
		~[]byte
}

// keyValueToAny normalizes a constrained key value into the representation the
// core client understands. Named types are converted to their underlying kind
// so the core's type switch recognizes them.
func keyValueToAny[T KeyValue](v T) any {
	switch t := any(v).(type) {
	case string:
		return t
	case []byte:
		return t
	case int:
		return t
	case int8:
		return int(t)
	case int16:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return t
	case uint8:
		return int(t)
	case uint16:
		return int(t)
	case uint32:
		return int64(t)
	}

	// A named type whose underlying type is one of the above. Convert through
	// reflection-free generic conversion by switching on the zero value's kind.
	return normalizeNamedKeyValue(v)
}

// DataSet pairs a namespace with a set name and mints keys for it.
//
// It holds no client reference and performs no I/O: it is a key factory. The
// zero value is not usable; construct with [DataSetOf].
type DataSet struct {
	namespace string
	setName   string
}

// DataSetOf constructs a DataSet. Both the namespace and the set name must be
// non-empty.
func DataSetOf(namespace, setName string) (*DataSet, error) {
	if namespace == "" {
		return nil, NewError(KindInvalidArgument, "namespace must not be empty")
	}
	if setName == "" {
		return nil, NewError(KindInvalidArgument, "set name must not be empty")
	}
	return &DataSet{namespace: namespace, setName: setName}, nil
}

// MustDataSetOf is [DataSetOf] for package-level initialization, panicking on
// invalid input.
func MustDataSetOf(namespace, setName string) *DataSet {
	ds, err := DataSetOf(namespace, setName)
	if err != nil {
		panic(err)
	}
	return ds
}

// Namespace reports the namespace.
func (ds *DataSet) Namespace() string { return ds.namespace }

// SetName reports the set name.
func (ds *DataSet) SetName() string { return ds.setName }

// String implements fmt.Stringer.
func (ds *DataSet) String() string {
	return "DataSet(namespace=" + ds.namespace + ", set_name=" + ds.setName + ")"
}

// Equals reports whether two datasets name the same (namespace, set) pair.
func (ds *DataSet) Equals(other *DataSet) bool {
	if ds == nil || other == nil {
		return ds == other
	}
	return ds.namespace == other.namespace && ds.setName == other.setName
}

// ID mints a key from an identifier whose type is only known at run time.
//
// This is the dynamic counterpart of [DataSet.Key], and exists for values that
// arrive as `any` — a user key read back from a mapped entity, or a row
// identifier in a [RowWriteBuilder].
//
// Because the type is unconstrained, an identifier Aerospike does not accept as
// a user key is reported as a [KindInvalidArgument] error. That covers both ways
// the core client rejects one: a value it can represent but cannot use as a key
// (a float, a bool) returns an error, and a type it cannot represent at all (a
// struct, or a raw uint64) panics inside NewValue — which is recovered here, so
// a bad identifier never aborts the caller.
//
// Prefer [DataSet.Key] whenever the identifier's type is known statically: it
// admits neither failure, so it needs no error check.
func (ds *DataSet) ID(identifier any) (key *as.Key, err error) {
	defer func() {
		if r := recover(); r != nil {
			key, err = nil, NewError(KindInvalidArgument,
				"identifier of type %T is not a valid Aerospike user key: %v", identifier, r)
		}
	}()
	k, aerr := as.NewKey(ds.namespace, ds.setName, identifier)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return k, nil
}

// Key mints a key for the given user key.
//
// The type parameter admits exactly the types Aerospike accepts as user keys,
// so unlike [DataSet.ID] this can neither fail nor panic, and returns no error:
//
//	key := users.Key(42)
//	stream, err := session.Upsert(key).SetTo("active", true).Execute()
//
// This is the Go counterpart of the Python and Rust SDKs' DataSet.id, which
// must return a fallible result because their identifier types are unbounded.
//
// Every admitted type round-trips exactly. Aerospike represents an integer user
// key as a signed 64-bit value, which is why [KeyValue] stops at uint32 among
// the unsigned types: uint and uint64 could exceed [math.MaxInt64] and would
// have to wrap. Use a string or []byte identifier for values beyond int64.
func (ds *DataSet) Key[T KeyValue](identifier T) *as.Key {
	key, err := as.NewKey(ds.namespace, ds.setName, keyValueToAny(identifier))
	if err != nil {
		// Unreachable: [KeyValue] admits only integer, string and byte-slice
		// identifiers, every one of which the key writer accepts. A failure
		// here means KeyValue and the key writer have drifted apart.
		panic("aerospike/sdk: key generation failed for a KeyValue identifier: " + err.Error())
	}
	return key
}

// Keys mints keys for many identifiers, preserving input order.
//
// Like [DataSet.Key], it cannot fail.
func (ds *DataSet) Keys[T KeyValue](identifiers []T) []*as.Key {
	keys := make([]*as.Key, 0, len(identifiers))
	for _, id := range identifiers {
		keys = append(keys, ds.Key(id))
	}
	return keys
}

// IDFromDigest addresses a record by digest. The digest is either 20 raw bytes
// or a 40-character hex string.
func (ds *DataSet) IDFromDigest(digest []byte) (*as.Key, error) {
	raw, err := normalizeDigest(digest)
	if err != nil {
		return nil, err
	}
	key, aerr := as.NewKeyWithDigest(ds.namespace, ds.setName, nil, raw)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return key, nil
}

// IDsFromDigests is the bulk form of [DataSet.IDFromDigest].
func (ds *DataSet) IDsFromDigests(digests [][]byte) ([]*as.Key, error) {
	keys := make([]*as.Key, 0, len(digests))
	for _, d := range digests {
		key, err := ds.IDFromDigest(d)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// normalizeDigest accepts 20 raw bytes or 40 hex characters.
func normalizeDigest(digest []byte) ([]byte, error) {
	switch len(digest) {
	case 20:
		out := make([]byte, 20)
		copy(out, digest)
		return out, nil
	case 40:
		out := make([]byte, 20)
		if _, err := hex.Decode(out, digest); err != nil {
			return nil, NewError(KindInvalidArgument,
				"digest must be 20 raw bytes or 40 hex characters: %s", err)
		}
		return out, nil
	default:
		return nil, NewError(KindInvalidArgument,
			"digest must be 20 raw bytes or 40 hex characters, got %d bytes", len(digest))
	}
}

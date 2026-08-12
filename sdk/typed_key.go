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

// TypedKey is a key that remembers the entity type stored under it.
//
// [TypedDataSet] carries the entity type for a whole set; TypedKey carries it
// for one record, which is what lets a read by key produce objects without
// being handed the dataset again:
//
//	key := customers.TypedKey(42)              // TypedKey[Customer]
//	stream, err := session.QueryTypedKey(key)  // *TypedRecordStream[Customer]
//	customer, err := stream.FirstObject()
//
// The zero value carries no key; construct with [TypedKeyOf] or
// [TypedDataSet.TypedKey].
type TypedKey[T any] struct {
	key *as.Key
}

// TypedKeyOf attaches an entity type to an existing key. The type is asserted,
// not verified: nothing about a key records what was written under it, so this
// says "read this as a T" and a mismatch surfaces when the record is mapped.
func TypedKeyOf[T any](key *as.Key) TypedKey[T] {
	return TypedKey[T]{key: key}
}

// Key unwraps to the untyped key, which is how a typed key reaches the untyped
// verbs:
//
//	stream, err := session.Upsert(key.Key()).SetTo("seen", true).Execute()
//
// A write builder carries no entity type -- the other SDKs return an untyped
// builder from a typed key too -- so unwrapping is the whole conversion.
func (k TypedKey[T]) Key() *as.Key { return k.key }

// IsZero reports whether the typed key holds no key.
func (k TypedKey[T]) IsZero() bool { return k.key == nil }

// Namespace reports the namespace, or the empty string for a zero TypedKey.
func (k TypedKey[T]) Namespace() string {
	if k.key == nil {
		return ""
	}
	return k.key.Namespace()
}

// SetName reports the set name, or the empty string for a zero TypedKey.
func (k TypedKey[T]) SetName() string {
	if k.key == nil {
		return ""
	}
	return k.key.SetName()
}

// String implements fmt.Stringer.
func (k TypedKey[T]) String() string {
	if k.key == nil {
		return "TypedKey(<nil>)"
	}
	return "TypedKey(" + k.key.String() + ")"
}

// TypedKeyList is a list of keys sharing one entity type.
type TypedKeyList[T any] []TypedKey[T]

// TypedKeysOf attaches an entity type to existing keys, preserving order.
func TypedKeysOf[T any](keys []*as.Key) TypedKeyList[T] {
	out := make(TypedKeyList[T], 0, len(keys))
	for _, k := range keys {
		out = append(out, TypedKey[T]{key: k})
	}
	return out
}

// Keys unwraps to the untyped keys, preserving order.
func (l TypedKeyList[T]) Keys() []*as.Key {
	out := make([]*as.Key, 0, len(l))
	for _, k := range l {
		out = append(out, k.key)
	}
	return out
}

// --- Minting typed keys from a typed dataset ---

// TypedKey mints a key in this dataset that carries the entity type. Like
// [DataSet.Key], it cannot fail.
func (t *TypedDataSet[T]) TypedKey[K KeyValue](identifier K) TypedKey[T] {
	return TypedKey[T]{key: t.ds.Key(identifier)}
}

// TypedKeys mints typed keys for many identifiers, preserving input order.
func (t *TypedDataSet[T]) TypedKeys[K KeyValue](identifiers []K) TypedKeyList[T] {
	out := make(TypedKeyList[T], 0, len(identifiers))
	for _, id := range identifiers {
		out = append(out, TypedKey[T]{key: t.ds.Key(id)})
	}
	return out
}

// TypedKeyForObject mints the typed key of an entity instance, from its key
// field. It is [TypedDataSet.IDForObject] with the type carried along.
func (t *TypedDataSet[T]) TypedKeyForObject(obj *T) (TypedKey[T], error) {
	key, err := t.IDForObject(obj)
	if err != nil {
		return TypedKey[T]{}, err
	}
	return TypedKey[T]{key: key}, nil
}

// --- Typed reads addressed by typed key ---

// QueryTypedKey opens a typed read of one record.
//
// Because the key carries the entity type, this needs no dataset argument --
// unlike [Session.QueryTypedKeys], which takes one purely to supply the type.
// A one-key read is issued as a point operation, not a one-row batch.
func (s *Session) QueryTypedKey[T any](key TypedKey[T]) *TypedQueryBuilder[T] {
	return &TypedQueryBuilder[T]{inner: s.Query(key.Key())}
}

// QueryTypedKeyList opens a typed read over several typed keys, as one batch.
//
// Go cannot infer the entity type through a union of TypedKey and TypedKeyList,
// so the single-key and many-key forms are separate methods rather than one
// target-polymorphic verb.
func (s *Session) QueryTypedKeyList[T any](keys TypedKeyList[T]) *TypedQueryBuilder[T] {
	return &TypedQueryBuilder[T]{inner: s.Query(keys.Keys())}
}

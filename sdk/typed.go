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
	"reflect"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// RecordMapper lets a type control its own mapping, overriding reflection.
//
// Implement it on the pointer receiver. A type that does not implement it is
// mapped by reflection over its `as` and `asm` struct tags.
type RecordMapper interface {
	// ToBins reports the bins to write.
	ToBins() (as.BinMap, error)
	// SetFromRecord rebuilds the value from a record.
	SetFromRecord(bins as.BinMap, key *as.Key, generation uint32) error
	// ID reports the user key.
	ID() any
}

// TypedDataSet is a [DataSet] that remembers its entity type.
type TypedDataSet[T any] struct {
	ds *DataSet
}

// TypedDataSetOf constructs a typed dataset.
func TypedDataSetOf[T any](namespace, setName string) (*TypedDataSet[T], error) {
	ds, err := DataSetOf(namespace, setName)
	if err != nil {
		return nil, err
	}
	return &TypedDataSet[T]{ds: ds}, nil
}

// TypedDataSetFrom wraps an existing dataset.
func TypedDataSetFrom[T any](ds *DataSet) *TypedDataSet[T] { return &TypedDataSet[T]{ds: ds} }

// DataSet reports the underlying untyped dataset, the bridge into the untyped
// API.
func (t *TypedDataSet[T]) DataSet() *DataSet { return t.ds }

// Namespace reports the namespace.
func (t *TypedDataSet[T]) Namespace() string { return t.ds.namespace }

// SetName reports the set name.
func (t *TypedDataSet[T]) SetName() string { return t.ds.setName }

// ID mints a key for the given identifier.
func (t *TypedDataSet[T]) ID(identifier any) (*as.Key, error) { return t.ds.ID(identifier) }

// IDForObject mints the key of an entity instance, from its key field.
func (t *TypedDataSet[T]) IDForObject(obj *T) (*as.Key, error) {
	id, err := objectID(obj)
	if err != nil {
		return nil, err
	}
	return t.ds.ID(id)
}

// keyTagOption marks the struct field that carries the user key.
const keyTagOption = "key"

// mappingInfo caches the reflection view of an entity type.
type mappingInfo struct {
	keyIndex []int
	genIndex []int
}

// analyzeType finds the key and generation fields of an entity type.
func analyzeType(t reflect.Type) mappingInfo {
	var info mappingInfo
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return info
	}
	for i := range t.NumField() {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := f.Tag.Get("as")
		for _, opt := range strings.Split(tag, ",")[1:] {
			if strings.TrimSpace(opt) == keyTagOption {
				info.keyIndex = f.Index
			}
		}
		if meta := strings.TrimSpace(f.Tag.Get("asm")); meta == "gen" {
			info.genIndex = f.Index
		}
	}
	return info
}

// objectID reports an entity's user key, preferring an explicit
// [RecordMapper] implementation over the `as:",key"` tag.
func objectID[T any](obj *T) (any, error) {
	if m, ok := any(obj).(RecordMapper); ok {
		return m.ID(), nil
	}
	info := analyzeType(reflect.TypeOf(obj))
	if info.keyIndex == nil {
		return nil, NewError(KindInvalidArgument,
			"type %T has no `as:\",key\"` field and does not implement RecordMapper",
			obj)
	}
	v := reflect.ValueOf(obj).Elem().FieldByIndex(info.keyIndex)
	return v.Interface(), nil
}

// objectToBins converts an entity into the bins to write, omitting the key
// field, which travels in the key itself.
func objectToBins[T any](obj *T) (as.BinMap, error) {
	if m, ok := any(obj).(RecordMapper); ok {
		return m.ToBins()
	}

	rv := reflect.ValueOf(obj).Elem()
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return nil, NewError(KindInvalidArgument, "typed writes need a struct, got %s", rt.Kind())
	}
	info := analyzeType(rt)

	bins := as.BinMap{}
	for i := range rt.NumField() {
		f := rt.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if info.keyIndex != nil && f.Index[0] == info.keyIndex[0] {
			continue
		}
		if info.genIndex != nil && f.Index[0] == info.genIndex[0] {
			continue
		}
		if strings.TrimSpace(f.Tag.Get("asm")) != "" {
			continue
		}

		tag := f.Tag.Get("as")
		name := strings.TrimSpace(strings.Split(tag, ",")[0])
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}

		value := rv.Field(i)
		if strings.Contains(tag, ",omitempty") && value.IsZero() {
			continue
		}

		// A pointer field models an optional value. A nil one is absent, so the
		// bin is left unwritten rather than written as nil; a present one writes
		// its pointee, because the core client's value conversion does not
		// accept pointers.
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				continue
			}
			value = value.Elem()
		}

		// The reflection mapper has no encoding for a nested struct, and the
		// core client's value conversion panics on one. Report it as a mapping
		// error naming the field, and point at the way out.
		if value.Kind() == reflect.Struct && !isSupportedStruct(value.Type()) {
			return nil, NewError(KindInvalidArgument,
				"field %s of %T is a nested struct, which the reflection mapper cannot encode; "+
					"implement RecordMapper on the type to control its own mapping",
				f.Name, obj)
		}
		bins[name] = value.Interface()
	}
	return bins, nil
}

// isSupportedStruct reports whether a struct-valued field is one the core
// client already knows how to encode.
func isSupportedStruct(t reflect.Type) bool {
	return t.PkgPath() == "time" && t.Name() == "Time"
}

// objectFromRecord rebuilds an entity from a record.
func objectFromRecord[T any](row *RecordResult) (*T, error) {
	rec, err := row.RecordOrRaise()
	if err != nil {
		return nil, err
	}
	out := new(T)

	if m, ok := any(out).(RecordMapper); ok {
		if err := m.SetFromRecord(rec.Bins, row.Key, rec.Generation); err != nil {
			return nil, err
		}
		return out, nil
	}

	// The core client's reflection reader fills the exported, tagged fields.
	if err := unmarshalRecord(rec, out); err != nil {
		return nil, err
	}

	rv := reflect.ValueOf(out).Elem()
	info := analyzeType(rv.Type())
	if info.keyIndex != nil && row.Key != nil && row.Key.Value() != nil {
		field := rv.FieldByIndex(info.keyIndex)
		if err := assignKeyValue(field, row.Key.Value().GetObject()); err != nil {
			return nil, err
		}
	}
	if info.genIndex != nil {
		field := rv.FieldByIndex(info.genIndex)
		if field.CanSet() && field.Kind() == reflect.Uint32 {
			field.SetUint(uint64(rec.Generation))
		}
	}
	return out, nil
}

// assignKeyValue writes the recovered user key into the key field.
func assignKeyValue(field reflect.Value, v any) error {
	if !field.CanSet() || v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Type().AssignableTo(field.Type()) {
		field.Set(rv)
		return nil
	}
	if rv.Type().ConvertibleTo(field.Type()) {
		field.Set(rv.Convert(field.Type()))
		return nil
	}
	return NewError(KindInvalidArgument,
		"cannot assign key value of type %T to field of type %s", v, field.Type())
}

// ObjectWithMetadata pairs an entity with its record metadata.
type ObjectWithMetadata[T any] struct {
	Object     *T
	Generation uint32
	TimeToLive time.Duration
}

// TypedRecordStream is a [RecordStream] that maps rows to entities.
type TypedRecordStream[T any] struct {
	inner *RecordStream
}

// TypedStreamFrom wraps an untyped stream.
func TypedStreamFrom[T any](s *RecordStream) *TypedRecordStream[T] {
	return &TypedRecordStream[T]{inner: s}
}

// Untyped reports the underlying stream.
func (t *TypedRecordStream[T]) Untyped() *RecordStream { return t.inner }

// Close releases the stream.
func (t *TypedRecordStream[T]) Close() { t.inner.Close() }

// Next reports the next row unmapped.
func (t *TypedRecordStream[T]) Next() (*RecordResult, error) { return t.inner.Next() }

// NextObject reports the next row mapped to an entity. A failed row or a
// mapping failure is an error, and the stream stays usable.
func (t *TypedRecordStream[T]) NextObject() (*T, error) {
	row, err := t.inner.Next()
	if err != nil || row == nil {
		return nil, err
	}
	return objectFromRecord[T](row)
}

// IntoObjects drains the stream into entities, preserving order.
func (t *TypedRecordStream[T]) IntoObjects() ([]*T, error) {
	defer t.Close()
	var out []*T
	for {
		row, err := t.inner.Next()
		if err != nil {
			return out, err
		}
		if row == nil {
			return out, nil
		}
		obj, err := objectFromRecord[T](row)
		if err != nil {
			return out, err
		}
		out = append(out, obj)
	}
}

// FirstObject reports the first mapped row and closes the stream.
func (t *TypedRecordStream[T]) FirstObject() (*T, error) {
	defer t.Close()
	row, err := t.inner.Next()
	if err != nil || row == nil {
		return nil, err
	}
	return objectFromRecord[T](row)
}

// NextObjectWithMetadata reports the next entity with its record metadata.
func (t *TypedRecordStream[T]) NextObjectWithMetadata() (*ObjectWithMetadata[T], error) {
	row, err := t.inner.Next()
	if err != nil || row == nil {
		return nil, err
	}
	obj, err := objectFromRecord[T](row)
	if err != nil {
		return nil, err
	}
	rec, _ := row.RecordOrRaise()
	out := &ObjectWithMetadata[T]{Object: obj}
	if rec != nil {
		out.Generation = rec.Generation
		out.TimeToLive = time.Duration(rec.Expiration) * time.Second
	}
	return out, nil
}

// Failures drains the stream and reports only the failed rows, unmapped.
func (t *TypedRecordStream[T]) Failures() ([]*RecordResult, error) { return t.inner.Failures() }

// TypedQueryBuilder is the typed face of [QueryBuilder].
//
// It reads whole records: there is no per-bin projection, because the mapper
// needs every bin.
type TypedQueryBuilder[T any] struct {
	inner *QueryBuilder
}

// Where sets a server-side filter.
func (t *TypedQueryBuilder[T]) Where[P Predicate](pred P) *TypedQueryBuilder[T] {
	t.inner.Where(pred)
	return t
}

// Limit caps the number of records returned.
func (t *TypedQueryBuilder[T]) Limit(n int64) *TypedQueryBuilder[T] {
	t.inner.Limit(n)
	return t
}

// ChunkSize turns the query into a paged cursor.
func (t *TypedQueryBuilder[T]) ChunkSize(n int64) *TypedQueryBuilder[T] {
	t.inner.ChunkSize(n)
	return t
}

// IncludeMissingKeys emits rows for keys that were not found.
func (t *TypedQueryBuilder[T]) IncludeMissingKeys() *TypedQueryBuilder[T] {
	t.inner.IncludeMissingKeys()
	return t
}

// Untyped exposes the full untyped builder surface.
func (t *TypedQueryBuilder[T]) Untyped() *QueryBuilder { return t.inner }

// Execute runs the query and returns a typed stream.
func (t *TypedQueryBuilder[T]) Execute() (*TypedRecordStream[T], error) {
	s, err := t.inner.Execute()
	if err != nil {
		return nil, err
	}
	return TypedStreamFrom[T](s), nil
}

// ObjectWriteBuilder writes whole entities.
//
// Each object becomes its own write segment, so per-object guards stay
// per-record and rows come back in insertion order. The chain is infallible:
// mapping failures and misplaced guards surface from Execute.
type ObjectWriteBuilder[T any] struct {
	session *Session
	ds      *TypedDataSet[T]
	verb    opType

	objects []objectEntry

	defaultTTL *int64
	txn        *as.Txn
	txnSet     bool

	pendingErr error
}

// objectEntry is one entity plus its per-record guards.
type objectEntry struct {
	key        *as.Key
	bins       as.BinMap
	generation *uint32
	ttlSeconds *int64
}

// deferErr records the first error.
func (b *ObjectWriteBuilder[T]) deferErr(err error) *ObjectWriteBuilder[T] {
	if err != nil && b.pendingErr == nil {
		b.pendingErr = err
	}
	return b
}

// Object adds one entity: its key comes from the key field, its bins from the
// mapping.
func (b *ObjectWriteBuilder[T]) Object(obj *T) *ObjectWriteBuilder[T] {
	key, err := b.ds.IDForObject(obj)
	if err != nil {
		return b.deferErr(err)
	}
	bins, err := objectToBins(obj)
	if err != nil {
		return b.deferErr(err)
	}
	if len(bins) == 0 {
		return b.deferErr(NewError(KindInvalidArgument,
			"object maps to no bins, so there is nothing to write"))
	}
	b.objects = append(b.objects, objectEntry{key: key, bins: bins})
	return b
}

// Objects adds several entities without per-object guards.
func (b *ObjectWriteBuilder[T]) Objects(objs []*T) *ObjectWriteBuilder[T] {
	for _, o := range objs {
		b.Object(o)
	}
	return b
}

// last reports the most recently added entity, recording an error when there
// is none.
func (b *ObjectWriteBuilder[T]) last(what string) *objectEntry {
	if len(b.objects) == 0 {
		b.deferErr(NewError(KindInvalidArgument, "%s must be called after Object", what))
		return nil
	}
	return &b.objects[len(b.objects)-1]
}

// EnsureGenerationIs guards the most recently added entity.
func (b *ObjectWriteBuilder[T]) EnsureGenerationIs(generation uint32) *ObjectWriteBuilder[T] {
	if generation == 0 {
		return b.deferErr(NewError(KindInvalidArgument, "generation guard must not be zero"))
	}
	if e := b.last("EnsureGenerationIs"); e != nil {
		e.generation = &generation
	}
	return b
}

// ExpireRecordAfterSeconds sets the expiration of the most recent entity.
func (b *ObjectWriteBuilder[T]) ExpireRecordAfterSeconds(seconds int64) *ObjectWriteBuilder[T] {
	if e := b.last("ExpireRecordAfterSeconds"); e != nil {
		e.ttlSeconds = &seconds
	}
	return b
}

// NeverExpire keeps the most recent entity forever.
func (b *ObjectWriteBuilder[T]) NeverExpire() *ObjectWriteBuilder[T] {
	return b.ExpireRecordAfterSeconds(ttlNeverExpire)
}

// DefaultExpireRecordAfterSeconds sets the expiration for entities without
// one of their own.
func (b *ObjectWriteBuilder[T]) DefaultExpireRecordAfterSeconds(seconds int64) *ObjectWriteBuilder[T] {
	b.defaultTTL = &seconds
	return b
}

// WithTxn joins a transaction for every entity.
func (b *ObjectWriteBuilder[T]) WithTxn(txn *as.Txn) *ObjectWriteBuilder[T] {
	b.txn, b.txnSet = txn, true
	return b
}

// Execute writes every entity.
func (b *ObjectWriteBuilder[T]) Execute() (*RecordStream, error) {
	if b.pendingErr != nil {
		return nil, b.pendingErr
	}
	if len(b.objects) == 0 {
		return nil, NewError(KindInvalidArgument, "no objects to write")
	}

	c := newChain(b.session)
	if b.txnSet {
		c.txn = b.txn
		c.txnSet = true
		c.txnOptOut = b.txn == nil
	}
	for _, e := range b.objects {
		c.startSegment(b.verb, []*as.Key{e.key}, false)
		for name, v := range e.bins {
			c.pushOp(as.PutOp(as.NewBin(name, v)))
		}
		c.current.generation = e.generation
		if e.ttlSeconds != nil {
			c.current.ttlSeconds = e.ttlSeconds
		} else if b.defaultTTL != nil {
			c.current.ttlSeconds = b.defaultTTL
		}
	}
	return c.execute(nil)
}

// --- Typed session verbs ---

// QueryTyped opens a typed read over a typed dataset.
func (s *Session) QueryTyped[T any](ds *TypedDataSet[T]) *TypedQueryBuilder[T] {
	return &TypedQueryBuilder[T]{inner: s.Query(ds.DataSet())}
}

// QueryTypedKeys opens a typed read over specific keys.
func (s *Session) QueryTypedKeys[T any](ds *TypedDataSet[T], keys []*as.Key) *TypedQueryBuilder[T] {
	return &TypedQueryBuilder[T]{inner: s.Query(keys)}
}

// UpsertTyped writes entities, creating or updating.
func (s *Session) UpsertTyped[T any](ds *TypedDataSet[T]) *ObjectWriteBuilder[T] {
	return &ObjectWriteBuilder[T]{session: s, ds: ds, verb: opUpsert}
}

// InsertTyped writes entities, failing when one already exists.
func (s *Session) InsertTyped[T any](ds *TypedDataSet[T]) *ObjectWriteBuilder[T] {
	return &ObjectWriteBuilder[T]{session: s, ds: ds, verb: opInsert}
}

// UpdateTyped writes entities, failing when one is absent.
func (s *Session) UpdateTyped[T any](ds *TypedDataSet[T]) *ObjectWriteBuilder[T] {
	return &ObjectWriteBuilder[T]{session: s, ds: ds, verb: opUpdate}
}

// ReplaceTyped replaces entities, removing bins that are not written.
func (s *Session) ReplaceTyped[T any](ds *TypedDataSet[T]) *ObjectWriteBuilder[T] {
	return &ObjectWriteBuilder[T]{session: s, ds: ds, verb: opReplace}
}

// ReplaceIfExistsTyped replaces existing entities only.
func (s *Session) ReplaceIfExistsTyped[T any](ds *TypedDataSet[T]) *ObjectWriteBuilder[T] {
	return &ObjectWriteBuilder[T]{session: s, ds: ds, verb: opReplaceIfExists}
}

// BinsOf reports the bins an entity maps to, without writing anything.
//
// The typed write builders map objects internally, but a caller sometimes needs
// the same layout in a hand-built segment — mixing a typed entity into a batch
// alongside other verbs, for instance, which [ObjectWriteBuilder] cannot express
// because it does not chain. Going through this function keeps the two paths
// writing the identical layout.
func BinsOf[T any](obj *T) (as.BinMap, error) { return objectToBins(obj) }

// IDOf reports an entity's user key, the value a typed write would put in the
// key rather than in a bin.
func IDOf[T any](obj *T) (any, error) { return objectID(obj) }

// Key mints a key in this typed dataset. Like [DataSet.Key], it cannot fail.
func (t *TypedDataSet[T]) Key[K KeyValue](identifier K) *as.Key {
	return t.ds.Key(identifier)
}

// Keys mints keys in this typed dataset, preserving input order.
func (t *TypedDataSet[T]) Keys[K KeyValue](identifiers []K) []*as.Key {
	return t.ds.Keys(identifiers)
}

// ObjectFromRecord maps one row to an entity.
//
// The typed streams map rows internally, but a heterogeneous batch — one whose
// rows belong to different entity types — has to decide per row, which needs an
// entry point of its own.
func ObjectFromRecord[T any](row *RecordResult) (*T, error) { return objectFromRecord[T](row) }

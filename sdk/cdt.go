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

// selectorKind classifies a CDT selection, which decides what the resulting
// builder can do.
type selectorKind int

const (
	// selectorSingular addresses one element and has a server-side context
	// form, so navigation can continue through it.
	selectorSingular selectorKind = iota
	// selectorValue addresses elements by value; it has a context form and
	// supports inverted terminals.
	selectorValue
	// selectorAction addresses a range or an explicit list; it has no context
	// form, so navigation cannot continue.
	selectorAction
)

// cdtPath accumulates the bin, the contexts navigated so far, and the current
// selection.
type cdtPath struct {
	bin string
	ctx []*as.CDTContext

	// isMapSelection records whether the current step selected inside a map,
	// which the key-oriented terminals require.
	isMapSelection bool
	// isMapKey records whether the current step was an on-map-key selection,
	// the only form the server can resolve to a single writable slot.
	isMapKey bool
	// key holds the map key of an on-map-key selection.
	key any
	// kind classifies the selection.
	kind selectorKind
}

// descend returns a copy of the path with one more context pushed.
func (p cdtPath) descend(c *as.CDTContext) cdtPath {
	out := p
	out.ctx = append(append([]*as.CDTContext{}, p.ctx...), c)
	return out
}

// --- Navigation entry points on the bin builders ---

// OnMapKey selects a map entry by key.
//
// Pass a non-nil order to create the intermediate map when it is missing,
// instead of failing.
func (b *WriteBinBuilder) OnMapKey(key any, createOrder *as.MapOrderType) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, isMapKey: true, key: key, kind: selectorSingular}
	return &CdtWriteBuilder{parent: b.parent, path: p, createOrder: createOrder}
}

// OnMapIndex selects a map entry by index.
func (b *WriteBinBuilder) OnMapIndex(index int) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorSingular}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxMapIndex(index))}
}

// OnMapRank selects a map entry by rank.
func (b *WriteBinBuilder) OnMapRank(rank int) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorSingular}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxMapRank(rank))}
}

// OnMapValue selects map entries by value.
func (b *WriteBinBuilder) OnMapValue(value any) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorValue}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxMapValue(as.NewValue(value)))}
}

// OnListIndex selects a list element by index.
//
// Pass a non-nil order to create the intermediate list when it is missing;
// pad extends the list to reach the index.
func (b *WriteBinBuilder) OnListIndex(index int, createOrder *as.ListOrderType, pad bool) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, kind: selectorSingular}
	if createOrder != nil {
		return &CdtWriteBuilder{parent: b.parent,
			path: p.descend(as.CtxListIndexCreate(index, *createOrder, pad))}
	}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxListIndex(index))}
}

// OnListRank selects a list element by rank.
func (b *WriteBinBuilder) OnListRank(rank int) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, kind: selectorSingular}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxListRank(rank))}
}

// OnListValue selects list elements by value.
func (b *WriteBinBuilder) OnListValue(value any) *CdtWriteBuilder {
	p := cdtPath{bin: b.bin, kind: selectorValue}
	return &CdtWriteBuilder{parent: b.parent, path: p.descend(as.CtxListValue(as.NewValue(value)))}
}

// OnListIndexRange selects a range of list elements.
//
// A range has no server-side context form, so the returned builder cannot
// navigate further: it offers terminals only.
func (b *WriteBinBuilder) OnListIndexRange(index, count int) *CdtWriteActionBuilder {
	return &CdtWriteActionBuilder{
		parent: b.parent,
		path:   cdtPath{bin: b.bin, kind: selectorAction},
		index:  index, count: count, hasCount: true,
	}
}

// OnMapKey selects a map entry by key, for reading.
func (b *QueryBinBuilder) OnMapKey(key any, createOrder *as.MapOrderType) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, isMapKey: true, key: key, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: p, createOrder: createOrder}
}

// OnMapIndex selects a map entry by index, for reading.
func (b *QueryBinBuilder) OnMapIndex(index int) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxMapIndex(index))}
}

// OnListIndex selects a list element by index, for reading.
func (b *QueryBinBuilder) OnListIndex(index int) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxListIndex(index))}
}

// OnListValue selects list elements by value, for reading.
func (b *QueryBinBuilder) OnListValue(value any) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, kind: selectorValue}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxListValue(as.NewValue(value)))}
}

// --- CdtWriteBuilder: a navigable write selection ---

// CdtWriteBuilder is a navigable selection inside a write segment. It offers
// both reads and writes at the selected path, and can navigate deeper.
type CdtWriteBuilder struct {
	parent      *WriteSegmentBuilder
	path        cdtPath
	createOrder *as.MapOrderType
}

// mapKeyContext materializes an on-map-key selection into a context.
func (b *CdtWriteBuilder) mapKeyContext() *as.CDTContext {
	if b.createOrder != nil {
		return as.CtxMapKeyCreate(as.NewValue(b.path.key), *b.createOrder)
	}
	return as.CtxMapKey(as.NewValue(b.path.key))
}

// contexts reports the full context chain, including a pending map-key step.
func (b *CdtWriteBuilder) contexts() []*as.CDTContext {
	if b.path.isMapKey {
		return append(append([]*as.CDTContext{}, b.path.ctx...), b.mapKeyContext())
	}
	return b.path.ctx
}

// OnMapKey navigates one level deeper by map key.
func (b *CdtWriteBuilder) OnMapKey(key any, createOrder *as.MapOrderType) *CdtWriteBuilder {
	base := cdtPath{bin: b.path.bin, ctx: b.contexts(),
		isMapSelection: true, isMapKey: true, key: key, kind: selectorSingular}
	return &CdtWriteBuilder{parent: b.parent, path: base, createOrder: createOrder}
}

// OnListIndex navigates one level deeper by list index.
func (b *CdtWriteBuilder) OnListIndex(index int, createOrder *as.ListOrderType, pad bool) *CdtWriteBuilder {
	base := cdtPath{bin: b.path.bin, ctx: b.contexts(), kind: selectorSingular}
	if createOrder != nil {
		return &CdtWriteBuilder{parent: b.parent,
			path: base.descend(as.CtxListIndexCreate(index, *createOrder, pad))}
	}
	return &CdtWriteBuilder{parent: b.parent, path: base.descend(as.CtxListIndex(index))}
}

// requireMapKey records an error when a per-key write is attempted off a
// selection the server cannot resolve to one writable slot.
func (b *CdtWriteBuilder) requireMapKey(what string) bool {
	if !b.path.isMapKey {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"%s requires an OnMapKey selection", what))
		return false
	}
	return true
}

// SetTo writes a value at the selected map key, unconditionally.
func (b *CdtWriteBuilder) SetTo(value any) *WriteSegmentBuilder {
	if !b.requireMapKey("SetTo") {
		return b.parent
	}
	b.parent.c.pushOp(as.MapPutOp(as.DefaultMapPolicy(), b.path.bin, b.path.key, value, b.path.ctx...))
	return b.parent
}

// Insert writes a value at the selected map key only when it is absent.
//
// Pass noFail to have the server skip silently instead of reporting that the
// element already exists.
func (b *CdtWriteBuilder) Insert(value any, noFail bool) *WriteSegmentBuilder {
	if !b.requireMapKey("Insert") {
		return b.parent
	}
	policy := createOnlyMapPolicy(noFail)
	b.parent.c.pushOp(as.MapPutOp(policy, b.path.bin, b.path.key, value, b.path.ctx...))
	return b.parent
}

// Update writes a value at the selected map key only when it is present.
func (b *CdtWriteBuilder) Update(value any, noFail bool) *WriteSegmentBuilder {
	if !b.requireMapKey("Update") {
		return b.parent
	}
	policy := updateOnlyMapPolicy(noFail)
	b.parent.c.pushOp(as.MapPutOp(policy, b.path.bin, b.path.key, value, b.path.ctx...))
	return b.parent
}

// Add adds to the numeric value at the selected map key.
func (b *CdtWriteBuilder) Add(delta any) *WriteSegmentBuilder {
	if !b.requireMapKey("Add") {
		return b.parent
	}
	b.parent.c.pushOp(as.MapIncrementOp(as.DefaultMapPolicy(), b.path.bin, b.path.key, delta, b.path.ctx...))
	return b.parent
}

// Remove removes the selected element, reporting nothing.
func (b *CdtWriteBuilder) Remove() *WriteSegmentBuilder {
	return b.removeWith(MapReturnNone)
}

// RemoveAnd removes the selected element and reports what went away. Choose
// the report with a terminal on the returned builder.
func (b *CdtWriteBuilder) RemoveAnd() *CdtRemoveResultBuilder {
	return &CdtRemoveResultBuilder{remove: b.removeWith}
}

// removeWith issues the removal with the chosen report.
func (b *CdtWriteBuilder) removeWith(rt MapReturnType) *WriteSegmentBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapRemoveByKeyOp(b.path.bin, b.path.key, rt, b.path.ctx...))
		return b.parent
	}
	// A selection already folded into the context is removed by clearing the
	// element the context addresses, which the server expresses as a
	// remove-by-index on the parent path.
	b.parent.c.deferErr(NewError(KindInvalidArgument,
		"Remove on this selection is not supported; select with OnMapKey to remove an entry"))
	return b.parent
}

// GetValues reads the values at the selected path.
func (b *CdtWriteBuilder) GetValues() *WriteSegmentBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnValue, b.path.ctx...))
		return b.parent
	}
	b.parent.c.pushOp(as.GetBinOp(b.path.bin))
	return b.parent
}

// MapSize reports the size of the map at the navigated path.
func (b *CdtWriteBuilder) MapSize() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.MapSizeOp(b.path.bin, b.contexts()...))
	return b.parent
}

// ListSize reports the size of the list at the navigated path.
func (b *CdtWriteBuilder) ListSize() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.ListSizeOp(b.path.bin, b.contexts()...))
	return b.parent
}

// ListAppend appends to the list at the navigated path.
func (b *CdtWriteBuilder) ListAppend(value any) *WriteSegmentBuilder {
	b.parent.c.pushOp(as.ListAppendWithPolicyContextOp(
		as.DefaultListPolicy(), b.path.bin, b.contexts(), value))
	return b.parent
}

// ListSet replaces an element of the list at the navigated path.
func (b *CdtWriteBuilder) ListSet(index int, value any) *WriteSegmentBuilder {
	b.parent.c.pushOp(as.ListSetOp(b.path.bin, index, value, b.contexts()...))
	return b.parent
}

// ListClear empties the list at the navigated path.
func (b *CdtWriteBuilder) ListClear() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.ListClearOp(b.path.bin, b.contexts()...))
	return b.parent
}

// MapClear empties the map at the navigated path.
func (b *CdtWriteBuilder) MapClear() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.MapClearOp(b.path.bin, b.contexts()...))
	return b.parent
}

// --- CdtReadBuilder: a navigable read selection ---

// CdtReadBuilder is a navigable selection inside a read segment. It offers
// reads at the selected path and can navigate deeper.
type CdtReadBuilder struct {
	parent      *QueryBuilder
	path        cdtPath
	createOrder *as.MapOrderType
}

// contexts reports the full context chain, including a pending map-key step.
func (b *CdtReadBuilder) contexts() []*as.CDTContext {
	if b.path.isMapKey {
		key := as.CtxMapKey(as.NewValue(b.path.key))
		if b.createOrder != nil {
			key = as.CtxMapKeyCreate(as.NewValue(b.path.key), *b.createOrder)
		}
		return append(append([]*as.CDTContext{}, b.path.ctx...), key)
	}
	return b.path.ctx
}

// OnMapKey navigates one level deeper by map key.
func (b *CdtReadBuilder) OnMapKey(key any, createOrder *as.MapOrderType) *CdtReadBuilder {
	base := cdtPath{bin: b.path.bin, ctx: b.contexts(),
		isMapSelection: true, isMapKey: true, key: key, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: base, createOrder: createOrder}
}

// OnListIndex navigates one level deeper by list index.
func (b *CdtReadBuilder) OnListIndex(index int) *CdtReadBuilder {
	base := cdtPath{bin: b.path.bin, ctx: b.contexts(), kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: base.descend(as.CtxListIndex(index))}
}

// GetValues reads the values at the selected path.
func (b *CdtReadBuilder) GetValues() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnValue, b.path.ctx...))
		return b.parent
	}
	b.parent.c.pushOp(as.GetBinOp(b.path.bin))
	return b.parent
}

// Count reports how many elements the selection matched.
func (b *CdtReadBuilder) Count() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnCount, b.path.ctx...))
		return b.parent
	}
	b.parent.c.pushOp(as.MapSizeOp(b.path.bin, b.contexts()...))
	return b.parent
}

// Exists reports whether the selection matched anything.
func (b *CdtReadBuilder) Exists() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnExists, b.path.ctx...))
		return b.parent
	}
	b.parent.c.deferErr(NewError(KindInvalidArgument,
		"Exists requires an OnMapKey selection"))
	return b.parent
}

// GetKeys reads the map keys of the selection. It is map-only.
func (b *CdtReadBuilder) GetKeys() *QueryBuilder {
	if !b.path.isMapSelection {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetKeys applies to a map selection, not a list selection"))
		return b.parent
	}
	b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnKey, b.path.ctx...))
	return b.parent
}

// MapSize reports the size of the map at the navigated path.
func (b *CdtReadBuilder) MapSize() *QueryBuilder {
	b.parent.c.pushOp(as.MapSizeOp(b.path.bin, b.contexts()...))
	return b.parent
}

// ListSize reports the size of the list at the navigated path.
func (b *CdtReadBuilder) ListSize() *QueryBuilder {
	b.parent.c.pushOp(as.ListSizeOp(b.path.bin, b.contexts()...))
	return b.parent
}

// ListGetRange reads a range of the list at the navigated path.
func (b *CdtReadBuilder) ListGetRange(index, count int) *QueryBuilder {
	b.parent.c.pushOp(as.ListGetRangeOp(b.path.bin, index, count, b.contexts()...))
	return b.parent
}

// --- CdtWriteActionBuilder: a range selection, no further navigation ---

// CdtWriteActionBuilder is a range or list selection inside a write segment.
//
// A range has no server-side context form, so this type deliberately has no
// navigation methods: continuing to navigate is a compile error rather than a
// runtime failure.
type CdtWriteActionBuilder struct {
	parent   *WriteSegmentBuilder
	path     cdtPath
	index    int
	count    int
	hasCount bool

	// sel describes a range or multi-value selection; hasSel distinguishes it
	// from the plain list-index-range form.
	sel    rangeSelection
	hasSel bool
}

// Remove removes the selected range, reporting nothing.
func (b *CdtWriteActionBuilder) Remove() *WriteSegmentBuilder {
	return b.removeWith(MapReturnNone)
}

// RemoveAnd removes the selected range and reports what went away.
func (b *CdtWriteActionBuilder) RemoveAnd() *CdtRemoveResultBuilder {
	return &CdtRemoveResultBuilder{remove: b.removeWith}
}

// removeWith issues the removal with the chosen report.
func (b *CdtWriteActionBuilder) removeWith(rt MapReturnType) *WriteSegmentBuilder {
	if b.hasSel {
		if b.sel.kind.isMapRange() {
			b.parent.c.pushOp(mapRangeRemoveOp(b.path.bin, b.sel, rt, false, b.path.ctx...))
		} else {
			b.parent.c.pushOp(listRangeRemoveOp(b.path.bin, b.sel, rt, false, b.path.ctx...))
		}
		return b.parent
	}
	// The plain index-range form, which reports nothing about what it removed.
	if b.hasCount {
		b.parent.c.pushOp(as.ListRemoveRangeOp(b.path.bin, b.index, b.count, b.path.ctx...))
	} else {
		b.parent.c.pushOp(as.ListRemoveRangeFromOp(b.path.bin, b.index, b.path.ctx...))
	}
	return b.parent
}

// GetValues reads the values in the selection.
func (b *CdtWriteActionBuilder) GetValues() *WriteSegmentBuilder {
	if b.hasSel {
		return b.getRange(MapReturnValue, false)
	}
	b.parent.c.pushOp(as.ListGetRangeOp(b.path.bin, b.index, b.count, b.path.ctx...))
	return b.parent
}

// Count reports how many elements the selection matched.
func (b *CdtWriteActionBuilder) Count() *WriteSegmentBuilder {
	return b.getRange(MapReturnCount, false)
}

// CdtRemoveResultBuilder chooses what a removal reports about the elements it
// removed.
type CdtRemoveResultBuilder struct {
	remove func(MapReturnType) *WriteSegmentBuilder
}

// Count reports how many elements were removed.
func (b *CdtRemoveResultBuilder) Count() *WriteSegmentBuilder { return b.remove(MapReturnCount) }

// GetValues reports the removed values.
func (b *CdtRemoveResultBuilder) GetValues() *WriteSegmentBuilder { return b.remove(MapReturnValue) }

// GetKeys reports the removed keys.
func (b *CdtRemoveResultBuilder) GetKeys() *WriteSegmentBuilder { return b.remove(MapReturnKey) }

// GetKeysAndValues reports the removed entries as interleaved keys and values.
func (b *CdtRemoveResultBuilder) GetKeysAndValues() *WriteSegmentBuilder {
	return b.remove(MapReturnKeyValue)
}

// createOnlyMapPolicy builds a create-only map policy.
//
// The ordering is left UNORDERED deliberately: a write mode is orthogonal to a
// map's key ordering, and imposing an ordering here would silently reorder an
// existing bin (and change the shape the server returns it in). Use
// MapSetPolicy to choose an ordering explicitly.
func createOnlyMapPolicy(noFail bool) *as.MapPolicy {
	flags := as.MapWriteFlagsCreateOnly
	if noFail {
		flags |= as.MapWriteFlagsNoFail
	}
	return as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, flags)
}

// updateOnlyMapPolicy builds an update-only map policy.
//
// As with createOnlyMapPolicy, the ordering is left UNORDERED.
func updateOnlyMapPolicy(noFail bool) *as.MapPolicy {
	flags := as.MapWriteFlagsUpdateOnly
	if noFail {
		flags |= as.MapWriteFlagsNoFail
	}
	return as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, flags)
}

// --- Read-side selectors and terminals that mirror the write side ---

// OnMapRank selects a map entry by rank, for reading.
func (b *QueryBinBuilder) OnMapRank(rank int) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxMapRank(rank))}
}

// OnMapValue selects map entries by value, for reading.
func (b *QueryBinBuilder) OnMapValue(value any) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, isMapSelection: true, kind: selectorValue}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxMapValue(as.NewValue(value)))}
}

// OnListRank selects a list element by rank, for reading.
func (b *QueryBinBuilder) OnListRank(rank int) *CdtReadBuilder {
	p := cdtPath{bin: b.bin, kind: selectorSingular}
	return &CdtReadBuilder{parent: b.parent, path: p.descend(as.CtxListRank(rank))}
}

// GetKeysAndValues reports the selected entries as interleaved keys and values.
// It applies to a map selection.
func (b *CdtReadBuilder) GetKeysAndValues() *QueryBuilder {
	if !b.path.isMapSelection {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetKeysAndValues applies to a map selection, not a list selection"))
		return b.parent
	}
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnKeyValue, b.path.ctx...))
		return b.parent
	}
	// A selection already folded into the context reads the whole collection at
	// that path as an ordered map, which is the entry view the server offers.
	b.parent.c.pushOp(as.MapGetByIndexRangeOp(b.path.bin, 0,
		as.MapReturnType.KEY_VALUE, b.contexts()...))
	return b.parent
}

// GetIndexes reports the indexes of the selected elements.
func (b *CdtReadBuilder) GetIndexes() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnIndex, b.path.ctx...))
		return b.parent
	}
	b.parent.c.deferErr(NewError(KindInvalidArgument, "GetIndexes needs a key selection"))
	return b.parent
}

// GetRanks reports the ranks of the selected elements.
func (b *CdtReadBuilder) GetRanks() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOp(b.path.bin, b.path.key, MapReturnRank, b.path.ctx...))
		return b.parent
	}
	b.parent.c.deferErr(NewError(KindInvalidArgument, "GetRanks needs a key selection"))
	return b.parent
}

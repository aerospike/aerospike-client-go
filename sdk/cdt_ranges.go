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

// Range and multi-value selections have no server-side context form, so they
// land on an action builder: it offers terminals, including the inverted ones,
// but no navigation. Continuing to navigate off a range is therefore a compile
// error rather than a runtime failure.

// --- Write-side range selectors ---

// OnMapKeyRange selects the map entries whose keys fall in a half-open range.
//
// Pass nil for begin to start at the first key, or nil for end to run to the
// last.
func (b *WriteBinBuilder) OnMapKeyRange(begin, end any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapKey, begin: begin, end: end})
}

// OnMapValueRange selects the map entries whose values fall in a half-open
// range.
func (b *WriteBinBuilder) OnMapValueRange(begin, end any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapValue, begin: begin, end: end})
}

// OnMapIndexRange selects count map entries from an index. A negative count
// runs to the end.
func (b *WriteBinBuilder) OnMapIndexRange(index int, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapIndex, position: index, count: count, hasCount: count >= 0,
	})
}

// OnMapRankRange selects count map entries from a rank. A negative count runs
// to the end.
func (b *WriteBinBuilder) OnMapRankRange(rank int, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapRank, position: rank, count: count, hasCount: count >= 0,
	})
}

// OnMapKeyList selects the map entries named by an explicit key list.
func (b *WriteBinBuilder) OnMapKeyList(keys ...any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapKeyList, items: keys})
}

// OnMapValueList selects the map entries holding any of the given values.
func (b *WriteBinBuilder) OnMapValueList(values ...any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapValueList, items: values})
}

// OnListValueRange selects the list elements whose values fall in a half-open
// range.
func (b *WriteBinBuilder) OnListValueRange(begin, end any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeListValue, begin: begin, end: end})
}

// OnListRankRange selects count list elements from a rank. A negative count
// runs to the end.
func (b *WriteBinBuilder) OnListRankRange(rank int, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeListRank, position: rank, count: count, hasCount: count >= 0,
	})
}

// OnListValueList selects the list elements holding any of the given values.
func (b *WriteBinBuilder) OnListValueList(values ...any) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeListValueList, items: values})
}

// rangeAction opens an action builder for a range selection.
func (b *WriteBinBuilder) rangeAction(sel rangeSelection) *CdtWriteActionBuilder {
	return &CdtWriteActionBuilder{
		parent: b.parent,
		path:   cdtPath{bin: b.bin, kind: selectorAction, isMapSelection: sel.kind.isMapRange()},
		sel:    sel,
		hasSel: true,
	}
}

// --- Read-side range selectors ---

// OnMapKeyRange selects the map entries whose keys fall in a half-open range.
func (b *QueryBinBuilder) OnMapKeyRange(begin, end any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapKey, begin: begin, end: end})
}

// OnMapValueRange selects the map entries whose values fall in a half-open
// range.
func (b *QueryBinBuilder) OnMapValueRange(begin, end any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapValue, begin: begin, end: end})
}

// OnMapIndexRange selects count map entries from an index.
func (b *QueryBinBuilder) OnMapIndexRange(index int, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapIndex, position: index, count: count, hasCount: count >= 0,
	})
}

// OnMapRankRange selects count map entries from a rank.
func (b *QueryBinBuilder) OnMapRankRange(rank int, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapRank, position: rank, count: count, hasCount: count >= 0,
	})
}

// OnMapKeyList selects the map entries named by an explicit key list.
func (b *QueryBinBuilder) OnMapKeyList(keys ...any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapKeyList, items: keys})
}

// OnMapValueList selects the map entries holding any of the given values.
func (b *QueryBinBuilder) OnMapValueList(values ...any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeMapValueList, items: values})
}

// OnListIndexRange selects count list elements from an index.
func (b *QueryBinBuilder) OnListIndexRange(index int, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeListIndex, position: index, count: count, hasCount: count >= 0,
	})
}

// OnListValueRange selects the list elements whose values fall in a half-open
// range.
func (b *QueryBinBuilder) OnListValueRange(begin, end any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeListValue, begin: begin, end: end})
}

// OnListRankRange selects count list elements from a rank.
func (b *QueryBinBuilder) OnListRankRange(rank int, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeListRank, position: rank, count: count, hasCount: count >= 0,
	})
}

// OnListValueList selects the list elements holding any of the given values.
func (b *QueryBinBuilder) OnListValueList(values ...any) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{kind: rangeListValueList, items: values})
}

// rangeAction opens a read action builder for a range selection.
func (b *QueryBinBuilder) rangeAction(sel rangeSelection) *CdtReadActionBuilder {
	return &CdtReadActionBuilder{
		parent: b.parent,
		path:   cdtPath{bin: b.bin, kind: selectorAction, isMapSelection: sel.kind.isMapRange()},
		sel:    sel,
	}
}

// CdtReadActionBuilder is a range or multi-value selection inside a read
// segment. It has terminals but, deliberately, no navigation methods.
type CdtReadActionBuilder struct {
	parent *QueryBuilder
	path   cdtPath
	sel    rangeSelection
}

// get issues the read with the chosen report, optionally inverted.
func (b *CdtReadActionBuilder) get(rt MapReturnType, inverted bool) *QueryBuilder {
	if b.sel.kind.isMapRange() {
		b.parent.c.pushOp(mapRangeGetOp(b.path.bin, b.sel, rt, inverted, b.path.ctx...))
	} else {
		b.parent.c.pushOp(listRangeGetOp(b.path.bin, b.sel, rt, inverted, b.path.ctx...))
	}
	return b.parent
}

// GetValues reports the values of the selected elements.
func (b *CdtReadActionBuilder) GetValues() *QueryBuilder { return b.get(MapReturnValue, false) }

// GetKeys reports the keys of the selected entries. It applies to a map
// selection.
func (b *CdtReadActionBuilder) GetKeys() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetKeys applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnKey, false)
}

// GetKeysAndValues reports the selected entries as interleaved keys and values.
func (b *CdtReadActionBuilder) GetKeysAndValues() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetKeysAndValues applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnKeyValue, false)
}

// Count reports how many elements the selection matched.
func (b *CdtReadActionBuilder) Count() *QueryBuilder { return b.get(MapReturnCount, false) }

// GetIndexes reports the indexes of the selected elements.
func (b *CdtReadActionBuilder) GetIndexes() *QueryBuilder { return b.get(MapReturnIndex, false) }

// GetRanks reports the ranks of the selected elements.
func (b *CdtReadActionBuilder) GetRanks() *QueryBuilder { return b.get(MapReturnRank, false) }

// GetAllOtherValues reports the values of the elements the selection did *not*
// match.
func (b *CdtReadActionBuilder) GetAllOtherValues() *QueryBuilder {
	return b.get(MapReturnValue, true)
}

// GetAllOtherKeys reports the keys of the entries the selection did not match.
func (b *CdtReadActionBuilder) GetAllOtherKeys() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetAllOtherKeys applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnKey, true)
}

// GetAllOtherKeysAndValues reports the unmatched entries as interleaved keys
// and values.
func (b *CdtReadActionBuilder) GetAllOtherKeysAndValues() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetAllOtherKeysAndValues applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnKeyValue, true)
}

// CountAllOthers reports how many elements the selection did not match.
func (b *CdtReadActionBuilder) CountAllOthers() *QueryBuilder {
	return b.get(MapReturnCount, true)
}

// GetAllOtherIndexes reports the indexes of the unmatched elements.
func (b *CdtReadActionBuilder) GetAllOtherIndexes() *QueryBuilder {
	return b.get(MapReturnIndex, true)
}

// GetAllOtherRanks reports the ranks of the unmatched elements.
func (b *CdtReadActionBuilder) GetAllOtherRanks() *QueryBuilder {
	return b.get(MapReturnRank, true)
}

// --- Inverted terminals on the value selections ---

// GetAllOtherValues reports the values of the elements the selection did not
// match.
func (b *CdtReadBuilder) GetAllOtherValues() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOpInv(b.path.bin, b.path.key, MapReturnValue, true, b.path.ctx...))
		return b.parent
	}
	b.parent.c.deferErr(NewError(KindInvalidArgument,
		"GetAllOtherValues needs a key or value selection"))
	return b.parent
}

// CountAllOthers reports how many elements the selection did not match.
func (b *CdtReadBuilder) CountAllOthers() *QueryBuilder {
	if b.path.isMapKey {
		b.parent.c.pushOp(mapGetByKeyOpInv(b.path.bin, b.path.key, MapReturnCount, true, b.path.ctx...))
		return b.parent
	}
	b.parent.c.deferErr(NewError(KindInvalidArgument,
		"CountAllOthers needs a key or value selection"))
	return b.parent
}

// RemoveAllOthers removes the elements the selection did not match, reporting
// nothing.
func (b *CdtWriteActionBuilder) RemoveAllOthers() *WriteSegmentBuilder {
	return b.removeInverted(MapReturnNone)
}

// RemoveAllOthersAnd removes the unmatched elements and reports what went away.
func (b *CdtWriteActionBuilder) RemoveAllOthersAnd() *CdtRemoveResultBuilder {
	return &CdtRemoveResultBuilder{remove: b.removeInverted}
}

// removeInverted issues the inverted removal with the chosen report.
func (b *CdtWriteActionBuilder) removeInverted(rt MapReturnType) *WriteSegmentBuilder {
	if !b.hasSel {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"this selection does not support an inverted removal"))
		return b.parent
	}
	if b.sel.kind.isMapRange() {
		b.parent.c.pushOp(mapRangeRemoveOp(b.path.bin, b.sel, rt, true, b.path.ctx...))
	} else {
		b.parent.c.pushOp(listRangeRemoveOp(b.path.bin, b.sel, rt, true, b.path.ctx...))
	}
	return b.parent
}

// GetAllOtherValues reports the values of the elements the selection did not
// match.
func (b *CdtWriteActionBuilder) GetAllOtherValues() *WriteSegmentBuilder {
	return b.getRange(MapReturnValue, true)
}

// CountAllOthers reports how many elements the selection did not match.
func (b *CdtWriteActionBuilder) CountAllOthers() *WriteSegmentBuilder {
	return b.getRange(MapReturnCount, true)
}

// getRange issues the read for this selection.
func (b *CdtWriteActionBuilder) getRange(rt MapReturnType, inverted bool) *WriteSegmentBuilder {
	if !b.hasSel {
		b.parent.c.deferErr(NewError(KindInvalidArgument, "this selection does not support a read"))
		return b.parent
	}
	if b.sel.kind.isMapRange() {
		b.parent.c.pushOp(mapRangeGetOp(b.path.bin, b.sel, rt, inverted, b.path.ctx...))
	} else {
		b.parent.c.pushOp(listRangeGetOp(b.path.bin, b.sel, rt, inverted, b.path.ctx...))
	}
	return b.parent
}

// --- HyperLogLog set operations ---

// HLLSetUnion replaces the bin with the union of the given HyperLogLog values.
func (b *WriteBinBuilder) HLLSetUnion(p *as.HLLPolicy, values ...as.HLLValue) *WriteSegmentBuilder {
	return b.push(as.HLLSetUnionOp(p, b.bin, values))
}

// HLLFold reduces the bin's index bit count.
func (b *WriteBinBuilder) HLLFold(indexBitCount int) *WriteSegmentBuilder {
	return b.push(as.HLLFoldOp(b.bin, indexBitCount))
}

// HLLRefreshCount recalculates and reports the bin's cached count.
func (b *WriteBinBuilder) HLLRefreshCount() *WriteSegmentBuilder {
	return b.push(as.HLLRefreshCountOp(b.bin))
}

// HLLGetUnion reports the union of the bin and the given values.
func (b *WriteBinBuilder) HLLGetUnion(values ...as.HLLValue) *WriteSegmentBuilder {
	return b.push(as.HLLGetUnionOp(b.bin, values))
}

// HLLGetUnionCount reports the estimated cardinality of that union.
func (b *WriteBinBuilder) HLLGetUnionCount(values ...as.HLLValue) *WriteSegmentBuilder {
	return b.push(as.HLLGetUnionCountOp(b.bin, values))
}

// HLLGetIntersectCount reports the estimated cardinality of the intersection.
func (b *WriteBinBuilder) HLLGetIntersectCount(values ...as.HLLValue) *WriteSegmentBuilder {
	return b.push(as.HLLGetIntersectCountOp(b.bin, values))
}

// HLLGetSimilarity reports the estimated Jaccard similarity.
func (b *WriteBinBuilder) HLLGetSimilarity(values ...as.HLLValue) *WriteSegmentBuilder {
	return b.push(as.HLLGetSimilarityOp(b.bin, values))
}

// HLLGetUnion reports the union of the bin and the given values.
func (b *QueryBinBuilder) HLLGetUnion(values ...as.HLLValue) *QueryBuilder {
	return b.push(as.HLLGetUnionOp(b.bin, values))
}

// HLLGetUnionCount reports the estimated cardinality of that union.
func (b *QueryBinBuilder) HLLGetUnionCount(values ...as.HLLValue) *QueryBuilder {
	return b.push(as.HLLGetUnionCountOp(b.bin, values))
}

// HLLGetIntersectCount reports the estimated cardinality of the intersection.
func (b *QueryBinBuilder) HLLGetIntersectCount(values ...as.HLLValue) *QueryBuilder {
	return b.push(as.HLLGetIntersectCountOp(b.bin, values))
}

// HLLGetSimilarity reports the estimated Jaccard similarity.
func (b *QueryBinBuilder) HLLGetSimilarity(values ...as.HLLValue) *QueryBuilder {
	return b.push(as.HLLGetSimilarityOp(b.bin, values))
}

// HLLRefreshCount recalculates and reports the bin's cached count.
func (b *QueryBinBuilder) HLLRefreshCount() *QueryBuilder {
	return b.push(as.HLLRefreshCountOp(b.bin))
}

// --- Relative range selectors ---
//
// A relative selection anchors on a value and then walks an index or rank
// offset from wherever that value sorts, which is how you express "the three
// entries after this key" without knowing the key's position. The anchor need
// not be present in the collection: the server uses where it *would* sort.
//
// A negative count runs to the end of the collection.

// OnMapKeyRelativeIndexRange selects count map entries starting at an index
// offset from where the anchor key sorts.
func (b *WriteBinBuilder) OnMapKeyRelativeIndexRange(key any, index, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapKeyRelativeIndex, anchor: key, offset: index,
		count: count, hasCount: count >= 0,
	})
}

// OnMapValueRelativeRankRange selects count map entries starting at a rank
// offset from where the anchor value sorts.
func (b *WriteBinBuilder) OnMapValueRelativeRankRange(value any, rank, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapValueRelativeRank, anchor: value, offset: rank,
		count: count, hasCount: count >= 0,
	})
}

// OnListValueRelativeRankRange selects count list elements starting at a rank
// offset from where the anchor value sorts.
func (b *WriteBinBuilder) OnListValueRelativeRankRange(value any, rank, count int) *CdtWriteActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeListValueRelativeRank, anchor: value, offset: rank,
		count: count, hasCount: count >= 0,
	})
}

// OnMapKeyRelativeIndexRange selects count map entries starting at an index
// offset from where the anchor key sorts.
func (b *QueryBinBuilder) OnMapKeyRelativeIndexRange(key any, index, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapKeyRelativeIndex, anchor: key, offset: index,
		count: count, hasCount: count >= 0,
	})
}

// OnMapValueRelativeRankRange selects count map entries starting at a rank
// offset from where the anchor value sorts.
func (b *QueryBinBuilder) OnMapValueRelativeRankRange(value any, rank, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeMapValueRelativeRank, anchor: value, offset: rank,
		count: count, hasCount: count >= 0,
	})
}

// OnListValueRelativeRankRange selects count list elements starting at a rank
// offset from where the anchor value sorts.
func (b *QueryBinBuilder) OnListValueRelativeRankRange(value any, rank, count int) *CdtReadActionBuilder {
	return b.rangeAction(rangeSelection{
		kind: rangeListValueRelativeRank, anchor: value, offset: rank,
		count: count, hasCount: count >= 0,
	})
}

// --- Ordered and unordered map reports on range selections ---

// GetAsOrderedMap reports the selected entries as an ordered map.
func (b *CdtReadActionBuilder) GetAsOrderedMap() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetAsOrderedMap applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnOrderedMap, false)
}

// GetAsUnorderedMap reports the selected entries as an unordered map.
func (b *CdtReadActionBuilder) GetAsUnorderedMap() *QueryBuilder {
	if !b.sel.kind.isMapRange() {
		b.parent.c.deferErr(NewError(KindInvalidArgument,
			"GetAsUnorderedMap applies to a map selection, not a list selection"))
		return b.parent
	}
	return b.get(MapReturnUnorderedMap, false)
}

// GetExists reports whether the selection matched anything.
func (b *CdtReadActionBuilder) GetExists() *QueryBuilder { return b.get(MapReturnExists, false) }

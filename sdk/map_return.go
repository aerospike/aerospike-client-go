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

// MapReturnType selects what a map operation reports about the entries it
// touched.
//
// The core client exposes these as fields of an unexported type, which cannot
// be named from another package; this enum mirrors them and is translated at
// the call site.
type MapReturnType int

// The map return types.
const (
	// MapReturnNone reports nothing.
	MapReturnNone MapReturnType = iota
	// MapReturnIndex reports the entries' indexes.
	MapReturnIndex
	// MapReturnReverseIndex reports the entries' indexes from the end.
	MapReturnReverseIndex
	// MapReturnRank reports the entries' ranks.
	MapReturnRank
	// MapReturnReverseRank reports the entries' ranks from the end.
	MapReturnReverseRank
	// MapReturnCount reports how many entries were touched.
	MapReturnCount
	// MapReturnKey reports the entries' keys.
	MapReturnKey
	// MapReturnValue reports the entries' values.
	MapReturnValue
	// MapReturnKeyValue reports keys and values interleaved.
	MapReturnKeyValue
	// MapReturnExists reports whether any entry matched.
	MapReturnExists
	// MapReturnUnorderedMap reports the entries as an unordered map.
	MapReturnUnorderedMap
	// MapReturnOrderedMap reports the entries as an ordered map.
	MapReturnOrderedMap
)

// orFlag ORs a flag into an enumeration value without naming its type.
//
// The core client's map return type is unexported and its list return type is
// only ever obtained from constants, so this package cannot declare either as a
// parameter or a result. Inferring the type parameter from the arguments lets
// the inverted bit be applied generically, instead of duplicating every
// dispatch for the inverted case.
//
// Note what this does *not* buy: a function that merely *returns* the core
// value is still impossible, because its result type would have to be named.
// That is why each operation family below carries its own switch — the core
// constant can only appear at the call site of the operation constructor.
func orFlag[T ~int](base, flag T, on bool) T {
	if on {
		return base | flag
	}
	return base
}

// coreListReturn translates an SDK return type into the core list return type,
// which unlike the map one is an exported named type.
func coreListReturn(rt MapReturnType) as.ListReturnType {
	switch rt {
	case MapReturnNone:
		return as.ListReturnTypeNone
	case MapReturnIndex:
		return as.ListReturnTypeIndex
	case MapReturnReverseIndex:
		return as.ListReturnTypeReverseIndex
	case MapReturnRank:
		return as.ListReturnTypeRank
	case MapReturnReverseRank:
		return as.ListReturnTypeReverseRank
	case MapReturnCount:
		return as.ListReturnTypeCount
	case MapReturnExists:
		return as.ListReturnTypeExists
	default:
		return as.ListReturnTypeValue
	}
}

// mapGetByKeyOp builds a map get-by-key operation.
func mapGetByKeyOp(bin string, key any, rt MapReturnType, ctx ...*as.CDTContext) *as.Operation {
	return mapGetByKeyOpInv(bin, key, rt, false, ctx...)
}

// mapGetByKeyOpInv builds a map get-by-key operation, optionally inverted so it
// reports the entries it did *not* select.
func mapGetByKeyOpInv(bin string, key any, rt MapReturnType, inv bool, ctx ...*as.CDTContext) *as.Operation {
	f := as.MapReturnType.INVERTED
	switch rt {
	case MapReturnNone:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
	case MapReturnIndex:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.INDEX, f, inv), ctx...)
	case MapReturnReverseIndex:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.REVERSE_INDEX, f, inv), ctx...)
	case MapReturnRank:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.RANK, f, inv), ctx...)
	case MapReturnReverseRank:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.REVERSE_RANK, f, inv), ctx...)
	case MapReturnCount:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
	case MapReturnKey:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
	case MapReturnKeyValue:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
	case MapReturnExists:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.EXISTS, f, inv), ctx...)
	case MapReturnUnorderedMap:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.UNORDERED_MAP, f, inv), ctx...)
	case MapReturnOrderedMap:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.ORDERED_MAP, f, inv), ctx...)
	default:
		return as.MapGetByKeyOp(bin, key, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
	}
}

// mapRemoveByKeyOp builds a map remove-by-key operation.
func mapRemoveByKeyOp(bin string, key any, rt MapReturnType, ctx ...*as.CDTContext) *as.Operation {
	f := as.MapReturnType.INVERTED
	switch rt {
	case MapReturnNone:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.NONE, f, false), ctx...)
	case MapReturnIndex:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.INDEX, f, false), ctx...)
	case MapReturnRank:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.RANK, f, false), ctx...)
	case MapReturnCount:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.COUNT, f, false), ctx...)
	case MapReturnKey:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.KEY, f, false), ctx...)
	case MapReturnKeyValue:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.KEY_VALUE, f, false), ctx...)
	default:
		return as.MapRemoveByKeyOp(bin, key, orFlag(as.MapReturnType.VALUE, f, false), ctx...)
	}
}

// mapOpKind names a map operation shape for the value, index and rank dispatch.
type mapOpKind int

const (
	mapOpGetByValue mapOpKind = iota
	mapOpRemoveByValue
	mapOpGetByIndex
	mapOpGetByRank
)

// mapDispatch builds a map operation with the chosen return type.
func mapDispatch(kind mapOpKind, bin string, value any, position int, rt MapReturnType) *as.Operation {
	return mapDispatchInv(kind, bin, value, position, rt, false)
}

// mapDispatchInv builds a map operation, optionally inverted.
func mapDispatchInv(kind mapOpKind, bin string, value any, position int, rt MapReturnType, inv bool) *as.Operation {
	f := as.MapReturnType.INVERTED
	switch kind {
	case mapOpGetByValue:
		switch rt {
		case MapReturnCount:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.COUNT, f, inv))
		case MapReturnKey:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.KEY, f, inv))
		case MapReturnKeyValue:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.KEY_VALUE, f, inv))
		case MapReturnIndex:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.INDEX, f, inv))
		case MapReturnRank:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.RANK, f, inv))
		case MapReturnExists:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.EXISTS, f, inv))
		default:
			return as.MapGetByValueOp(bin, value, orFlag(as.MapReturnType.VALUE, f, inv))
		}
	case mapOpRemoveByValue:
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByValueOp(bin, value, orFlag(as.MapReturnType.COUNT, f, inv))
		case MapReturnKey:
			return as.MapRemoveByValueOp(bin, value, orFlag(as.MapReturnType.KEY, f, inv))
		case MapReturnKeyValue:
			return as.MapRemoveByValueOp(bin, value, orFlag(as.MapReturnType.KEY_VALUE, f, inv))
		case MapReturnNone:
			return as.MapRemoveByValueOp(bin, value, orFlag(as.MapReturnType.NONE, f, inv))
		default:
			return as.MapRemoveByValueOp(bin, value, orFlag(as.MapReturnType.VALUE, f, inv))
		}
	case mapOpGetByIndex:
		switch rt {
		case MapReturnKey:
			return as.MapGetByIndexOp(bin, position, orFlag(as.MapReturnType.KEY, f, inv))
		case MapReturnKeyValue:
			return as.MapGetByIndexOp(bin, position, orFlag(as.MapReturnType.KEY_VALUE, f, inv))
		case MapReturnCount:
			return as.MapGetByIndexOp(bin, position, orFlag(as.MapReturnType.COUNT, f, inv))
		default:
			return as.MapGetByIndexOp(bin, position, orFlag(as.MapReturnType.VALUE, f, inv))
		}
	default: // mapOpGetByRank
		switch rt {
		case MapReturnKey:
			return as.MapGetByRankOp(bin, position, orFlag(as.MapReturnType.KEY, f, inv))
		case MapReturnKeyValue:
			return as.MapGetByRankOp(bin, position, orFlag(as.MapReturnType.KEY_VALUE, f, inv))
		case MapReturnCount:
			return as.MapGetByRankOp(bin, position, orFlag(as.MapReturnType.COUNT, f, inv))
		default:
			return as.MapGetByRankOp(bin, position, orFlag(as.MapReturnType.VALUE, f, inv))
		}
	}
}

// --- Range and multi-value selections ---

// rangeKind names a range or multi-value selection. A selection of this shape
// has no server-side context form, so it can only terminate, never nest.
type rangeKind int

const (
	rangeMapKey rangeKind = iota
	rangeMapValue
	rangeMapIndex
	rangeMapRank
	rangeMapKeyList
	rangeMapValueList
	rangeMapKeyRelativeIndex
	rangeMapValueRelativeRank
	rangeListValue
	rangeListIndex
	rangeListRank
	rangeListValueList
	rangeListValueRelativeRank
)

// isMapRange reports whether the selection addresses a map.
func (k rangeKind) isMapRange() bool { return k <= rangeMapValueRelativeRank }

// rangeSelection describes a range or multi-value selection.
type rangeSelection struct {
	kind rangeKind

	begin any
	end   any

	position int
	count    int
	hasCount bool

	items []any

	// anchor and offset describe a relative selection: the elements at an
	// index or rank offset from wherever the anchor value sorts.
	anchor any
	offset int
}

// mapRangeGetOp builds the read for a map range selection.
func mapRangeGetOp(bin string, sel rangeSelection, rt MapReturnType, inv bool, ctx ...*as.CDTContext) *as.Operation {
	f := as.MapReturnType.INVERTED
	// The return type is chosen per branch because the core constant can only
	// appear as an argument to the operation constructor.
	switch sel.kind {
	case rangeMapKey:
		switch rt {
		case MapReturnOrderedMap:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.ORDERED_MAP, f, inv), ctx...)
		case MapReturnUnorderedMap:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.UNORDERED_MAP, f, inv), ctx...)
		case MapReturnExists:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.EXISTS, f, inv), ctx...)
		case MapReturnCount:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		case MapReturnIndex:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.INDEX, f, inv), ctx...)
		case MapReturnRank:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.RANK, f, inv), ctx...)
		default:
			return as.MapGetByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapValue:
		switch rt {
		case MapReturnOrderedMap:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.ORDERED_MAP, f, inv), ctx...)
		case MapReturnUnorderedMap:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.UNORDERED_MAP, f, inv), ctx...)
		case MapReturnExists:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.EXISTS, f, inv), ctx...)
		case MapReturnCount:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapIndex:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapGetByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnKey:
				return as.MapGetByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
			case MapReturnKeyValue:
				return as.MapGetByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
			default:
				return as.MapGetByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapGetByIndexRangeOp(bin, sel.position, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByIndexRangeOp(bin, sel.position, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByIndexRangeOp(bin, sel.position, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapRank:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapGetByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnKey:
				return as.MapGetByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
			case MapReturnKeyValue:
				return as.MapGetByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
			default:
				return as.MapGetByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapGetByRankRangeOp(bin, sel.position, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByRankRangeOp(bin, sel.position, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByRankRangeOp(bin, sel.position, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapKeyList:
		switch rt {
		case MapReturnCount:
			return as.MapGetByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapValueList:
		switch rt {
		case MapReturnCount:
			return as.MapGetByValueListOp(bin, sel.items, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapGetByValueListOp(bin, sel.items, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByValueListOp(bin, sel.items, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByValueListOp(bin, sel.items, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapKeyRelativeIndex:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapGetByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnKey:
				return as.MapGetByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
			case MapReturnKeyValue:
				return as.MapGetByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
			default:
				return as.MapGetByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapGetByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapGetByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapGetByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		default:
			return as.MapGetByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	default: // rangeMapValueRelativeRank
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapGetByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnKey:
				return as.MapGetByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
			case MapReturnKeyValue:
				return as.MapGetByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
			default:
				return as.MapGetByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapGetByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapGetByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		default:
			return as.MapGetByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	}
}

// mapRangeRemoveOp builds the removal for a map range selection.
func mapRangeRemoveOp(bin string, sel rangeSelection, rt MapReturnType, inv bool, ctx ...*as.CDTContext) *as.Operation {
	f := as.MapReturnType.INVERTED
	switch sel.kind {
	case rangeMapKey:
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapRemoveByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnKeyValue:
			return as.MapRemoveByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY_VALUE, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByKeyRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapValue:
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapRemoveByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByValueRangeOp(bin, sel.begin, sel.end, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapIndex:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapRemoveByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnNone:
				return as.MapRemoveByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
			default:
				return as.MapRemoveByIndexRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByIndexRangeOp(bin, sel.position, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		default:
			return as.MapRemoveByIndexRangeOp(bin, sel.position, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapRank:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapRemoveByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnNone:
				return as.MapRemoveByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
			default:
				return as.MapRemoveByRankRangeCountOp(bin, sel.position, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByRankRangeOp(bin, sel.position, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		default:
			return as.MapRemoveByRankRangeOp(bin, sel.position, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapKeyList:
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByKeyListOp(bin, sel.items, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapValueList:
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByValueListOp(bin, sel.items, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnKey:
			return as.MapRemoveByValueListOp(bin, sel.items, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByValueListOp(bin, sel.items, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByValueListOp(bin, sel.items, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	case rangeMapKeyRelativeIndex:
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapRemoveByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnKey:
				return as.MapRemoveByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.KEY, f, inv), ctx...)
			case MapReturnNone:
				return as.MapRemoveByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
			default:
				return as.MapRemoveByKeyRelativeIndexRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByKeyRelativeIndexRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	default: // rangeMapValueRelativeRank
		if sel.hasCount {
			switch rt {
			case MapReturnCount:
				return as.MapRemoveByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
			case MapReturnNone:
				return as.MapRemoveByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
			default:
				return as.MapRemoveByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
			}
		}
		switch rt {
		case MapReturnCount:
			return as.MapRemoveByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.COUNT, f, inv), ctx...)
		case MapReturnNone:
			return as.MapRemoveByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.NONE, f, inv), ctx...)
		default:
			return as.MapRemoveByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, orFlag(as.MapReturnType.VALUE, f, inv), ctx...)
		}
	}
}

// listRangeGetOp builds the read for a list range selection.
//
// The list return type is an exported named type, so unlike the map case this
// needs no per-branch switch.
func listRangeGetOp(bin string, sel rangeSelection, rt MapReturnType, inv bool, ctx ...*as.CDTContext) *as.Operation {
	core := orFlag(coreListReturn(rt), as.ListReturnTypeInverted, inv)
	switch sel.kind {
	case rangeListValue:
		return as.ListGetByValueRangeOp(bin, sel.begin, sel.end, core, ctx...)
	case rangeListIndex:
		if sel.hasCount {
			return as.ListGetByIndexRangeCountOp(bin, sel.position, sel.count, core, ctx...)
		}
		return as.ListGetByIndexRangeOp(bin, sel.position, core, ctx...)
	case rangeListRank:
		if sel.hasCount {
			return as.ListGetByRankRangeCountOp(bin, sel.position, sel.count, core, ctx...)
		}
		return as.ListGetByRankRangeOp(bin, sel.position, core, ctx...)
	case rangeListValueList:
		return as.ListGetByValueListOp(bin, sel.items, core, ctx...)
	default: // rangeListValueRelativeRank
		if sel.hasCount {
			return as.ListGetByValueRelativeRankRangeCountOp(bin, sel.anchor, sel.offset, sel.count, core, ctx...)
		}
		return as.ListGetByValueRelativeRankRangeOp(bin, sel.anchor, sel.offset, core, ctx...)
	}
}

// listRangeRemoveOp builds the removal for a list range selection.
func listRangeRemoveOp(bin string, sel rangeSelection, rt MapReturnType, inv bool, ctx ...*as.CDTContext) *as.Operation {
	core := orFlag(coreListReturn(rt), as.ListReturnTypeInverted, inv)
	switch sel.kind {
	case rangeListValue:
		return as.ListRemoveByValueRangeOp(bin, core, sel.begin, sel.end)
	case rangeListIndex:
		if sel.hasCount {
			return as.ListRemoveByIndexRangeCountOp(bin, sel.position, sel.count, core, ctx...)
		}
		return as.ListRemoveByIndexRangeOp(bin, sel.position, core, ctx...)
	case rangeListRank:
		if sel.hasCount {
			return as.ListRemoveByRankRangeCountOp(bin, sel.position, sel.count, core, ctx...)
		}
		return as.ListRemoveByRankRangeOp(bin, sel.position, core, ctx...)
	case rangeListValueList:
		return as.ListRemoveByValueListOp(bin, sel.items, core, ctx...)
	default: // rangeListValueRelativeRank
		// The list removals take the return type *before* the anchor, unlike
		// their get counterparts and unlike the map removals.
		if sel.hasCount {
			return as.ListRemoveByValueRelativeRankRangeCountOp(bin, core, sel.anchor, sel.offset, sel.count, ctx...)
		}
		return as.ListRemoveByValueRelativeRankRangeOp(bin, core, sel.anchor, sel.offset, ctx...)
	}
}

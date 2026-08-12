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

// --- List: creation, ordering, and value-based access ---

// ListCreate creates the list with an ordering, padding to reach an index.
func (b *WriteBinBuilder) ListCreate(order as.ListOrderType, pad bool) *WriteSegmentBuilder {
	return b.push(as.ListCreateOp(b.bin, order, pad))
}

// ListSetOrder applies an ordering to the list.
func (b *WriteBinBuilder) ListSetOrder(order as.ListOrderType) *WriteSegmentBuilder {
	return b.push(as.ListSetOrderOp(b.bin, order))
}

// ListInsertItems inserts several values at an index.
func (b *WriteBinBuilder) ListInsertItems(index int, items []any) *WriteSegmentBuilder {
	return b.push(as.ListInsertOp(b.bin, index, items...))
}

// ListPopRange removes and reports a range of values.
func (b *WriteBinBuilder) ListPopRange(index, count int) *WriteSegmentBuilder {
	return b.push(as.ListPopRangeOp(b.bin, index, count))
}

// ListRemoveByValue removes elements equal to a value, reporting per the
// return type.
func (b *WriteBinBuilder) ListRemoveByValue(value any, rt as.ListReturnType) *WriteSegmentBuilder {
	return b.push(as.ListRemoveByValueOp(b.bin, value, rt))
}

// ListGetByValue reports elements equal to a value.
func (b *QueryBinBuilder) ListGetByValue(value any, rt as.ListReturnType) *QueryBuilder {
	return b.push(as.ListGetByValueOp(b.bin, value, rt))
}

// --- Map: creation and index, rank and value access ---

// MapCreate creates the map with an ordering.
func (b *WriteBinBuilder) MapCreate(order as.MapOrderType) *WriteSegmentBuilder {
	return b.push(as.MapCreateOp(b.bin, order, nil))
}

// MapRemoveByValue removes entries whose value matches.
func (b *WriteBinBuilder) MapRemoveByValue(value any, rt MapReturnType) *WriteSegmentBuilder {
	return b.push(mapDispatch(mapOpRemoveByValue, b.bin, value, 0, rt))
}

// MapGetByValue reports entries whose value matches.
func (b *QueryBinBuilder) MapGetByValue(value any, rt MapReturnType) *QueryBuilder {
	return b.push(mapDispatch(mapOpGetByValue, b.bin, value, 0, rt))
}

// MapGetByIndex reports the entry at an index.
func (b *QueryBinBuilder) MapGetByIndex(index int, rt MapReturnType) *QueryBuilder {
	return b.push(mapDispatch(mapOpGetByIndex, b.bin, nil, index, rt))
}

// MapGetByRank reports the entry at a rank.
func (b *QueryBinBuilder) MapGetByRank(rank int, rt MapReturnType) *QueryBuilder {
	return b.push(mapDispatch(mapOpGetByRank, b.bin, nil, rank, rt))
}

// --- Bitwise: the remaining family ---

// BitResize resizes the byte slice.
func (b *WriteBinBuilder) BitResize(p *as.BitPolicy, byteSize int, flags as.BitResizeFlags) *WriteSegmentBuilder {
	return b.push(as.BitResizeOp(bitPolicy(p), b.bin, byteSize, flags))
}

// BitInsert inserts bytes at an offset.
func (b *WriteBinBuilder) BitInsert(p *as.BitPolicy, byteOffset int, value []byte) *WriteSegmentBuilder {
	return b.push(as.BitInsertOp(bitPolicy(p), b.bin, byteOffset, value))
}

// BitRemove removes bytes at an offset.
func (b *WriteBinBuilder) BitRemove(p *as.BitPolicy, byteOffset, byteSize int) *WriteSegmentBuilder {
	return b.push(as.BitRemoveOp(bitPolicy(p), b.bin, byteOffset, byteSize))
}

// BitOr applies a bitwise or.
func (b *WriteBinBuilder) BitOr(p *as.BitPolicy, offset, size int, value []byte) *WriteSegmentBuilder {
	return b.push(as.BitOrOp(bitPolicy(p), b.bin, offset, size, value))
}

// BitXor applies a bitwise exclusive or.
func (b *WriteBinBuilder) BitXor(p *as.BitPolicy, offset, size int, value []byte) *WriteSegmentBuilder {
	return b.push(as.BitXorOp(bitPolicy(p), b.bin, offset, size, value))
}

// BitAnd applies a bitwise and.
func (b *WriteBinBuilder) BitAnd(p *as.BitPolicy, offset, size int, value []byte) *WriteSegmentBuilder {
	return b.push(as.BitAndOp(bitPolicy(p), b.bin, offset, size, value))
}

// BitNot inverts a bit range.
func (b *WriteBinBuilder) BitNot(p *as.BitPolicy, offset, size int) *WriteSegmentBuilder {
	return b.push(as.BitNotOp(bitPolicy(p), b.bin, offset, size))
}

// BitLShift shifts a bit range left.
func (b *WriteBinBuilder) BitLShift(p *as.BitPolicy, offset, size, shift int) *WriteSegmentBuilder {
	return b.push(as.BitLShiftOp(bitPolicy(p), b.bin, offset, size, shift))
}

// BitRShift shifts a bit range right.
func (b *WriteBinBuilder) BitRShift(p *as.BitPolicy, offset, size, shift int) *WriteSegmentBuilder {
	return b.push(as.BitRShiftOp(bitPolicy(p), b.bin, offset, size, shift))
}

// BitSubtract subtracts from an integer stored in a bit range.
func (b *WriteBinBuilder) BitSubtract(p *as.BitPolicy, offset, size int, value int64, signed bool, action as.BitOverflowAction) *WriteSegmentBuilder {
	return b.push(as.BitSubtractOp(bitPolicy(p), b.bin, offset, size, value, signed, action))
}

// BitSetInt writes an integer into a bit range.
func (b *WriteBinBuilder) BitSetInt(p *as.BitPolicy, offset, size int, value int64) *WriteSegmentBuilder {
	return b.push(as.BitSetIntOp(bitPolicy(p), b.bin, offset, size, value))
}

// BitLScan reports the first bit matching value, scanning from the left.
func (b *WriteBinBuilder) BitLScan(offset, size int, value bool) *WriteSegmentBuilder {
	return b.push(as.BitLScanOp(b.bin, offset, size, value))
}

// BitRScan reports the first bit matching value, scanning from the right.
func (b *WriteBinBuilder) BitRScan(offset, size int, value bool) *WriteSegmentBuilder {
	return b.push(as.BitRScanOp(b.bin, offset, size, value))
}

// BitGetInt reads an integer from a bit range.
func (b *WriteBinBuilder) BitGetInt(offset, size int, signed bool) *WriteSegmentBuilder {
	return b.push(as.BitGetIntOp(b.bin, offset, size, signed))
}

// BitLScan reports the first bit matching value, scanning from the left.
func (b *QueryBinBuilder) BitLScan(offset, size int, value bool) *QueryBuilder {
	return b.push(as.BitLScanOp(b.bin, offset, size, value))
}

// BitRScan reports the first bit matching value, scanning from the right.
func (b *QueryBinBuilder) BitRScan(offset, size int, value bool) *QueryBuilder {
	return b.push(as.BitRScanOp(b.bin, offset, size, value))
}

// BitGetInt reads an integer from a bit range.
func (b *QueryBinBuilder) BitGetInt(offset, size int, signed bool) *QueryBuilder {
	return b.push(as.BitGetIntOp(b.bin, offset, size, signed))
}

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

// WriteBinBuilder addresses one bin inside a write segment.
//
// Every terminal returns the parent segment, so chains flow back naturally:
//
//	session.Upsert(key).
//	    Bin("counter").SetTo(100).
//	    Bin("counter").Add(11).
//	    Execute()
type WriteBinBuilder struct {
	parent *WriteSegmentBuilder
	bin    string
}

// push appends an operation and returns the parent.
func (b *WriteBinBuilder) push(op *as.Operation) *WriteSegmentBuilder {
	b.parent.c.pushOp(op)
	return b.parent
}

// Bin starts the next bin without leaving the segment.
func (b *WriteBinBuilder) Bin(name string) *WriteBinBuilder {
	return &WriteBinBuilder{parent: b.parent, bin: name}
}

// --- Scalar ---

// SetTo sets the bin to a value.
func (b *WriteBinBuilder) SetTo(v any) *WriteSegmentBuilder {
	return b.push(as.PutOp(as.NewBin(b.bin, v)))
}

// SetToGeoJSON sets the bin to a GeoJSON document.
func (b *WriteBinBuilder) SetToGeoJSON(doc string) *WriteSegmentBuilder {
	return b.push(as.PutOp(as.NewBin(b.bin, as.NewGeoJSONValue(doc))))
}

// Add performs a numeric add.
func (b *WriteBinBuilder) Add(v any) *WriteSegmentBuilder {
	return b.push(as.AddOp(as.NewBin(b.bin, v)))
}

// IncrementBy is an alias for [WriteBinBuilder.Add].
func (b *WriteBinBuilder) IncrementBy(v any) *WriteSegmentBuilder { return b.Add(v) }

// Append appends to a string bin.
func (b *WriteBinBuilder) Append(v string) *WriteSegmentBuilder {
	return b.push(as.AppendOp(as.NewBin(b.bin, v)))
}

// Prepend prepends to a string bin.
func (b *WriteBinBuilder) Prepend(v string) *WriteSegmentBuilder {
	return b.push(as.PrependOp(as.NewBin(b.bin, v)))
}

// Remove deletes the bin.
func (b *WriteBinBuilder) Remove() *WriteSegmentBuilder {
	return b.push(as.PutOp(as.NewBin(b.bin, nil)))
}

// Get reads the bin back within the same operation.
func (b *WriteBinBuilder) Get() *WriteSegmentBuilder {
	return b.push(as.GetBinOp(b.bin))
}

// --- Expression operations ---

// SelectFrom reads a computed value as a virtual bin.
func (b *WriteBinBuilder) SelectFrom(exp *as.Expression, ignoreEvalFailure bool) *WriteSegmentBuilder {
	flags := as.ExpReadFlagDefault
	if ignoreEvalFailure {
		flags = as.ExpReadFlagEvalNoFail
	}
	return b.push(as.ExpReadOp(b.bin, exp, flags))
}

// WriteFrom writes a computed value into the bin.
func (b *WriteBinBuilder) WriteFrom(exp *as.Expression, flags as.ExpWriteFlags) *WriteSegmentBuilder {
	return b.push(as.ExpWriteOp(b.bin, exp, flags))
}

// --- List ---

// ListAppend appends one value.
func (b *WriteBinBuilder) ListAppend(v any) *WriteSegmentBuilder {
	return b.push(as.ListAppendOp(b.bin, v))
}

// ListAppendWithPolicy appends one value under an explicit list policy.
func (b *WriteBinBuilder) ListAppendWithPolicy(p *as.ListPolicy, v any) *WriteSegmentBuilder {
	return b.push(as.ListAppendWithPolicyOp(p, b.bin, v))
}

// ListAppendItems appends several values.
func (b *WriteBinBuilder) ListAppendItems(items []any) *WriteSegmentBuilder {
	return b.push(as.ListAppendOp(b.bin, items...))
}

// ListInsert inserts a value at an index.
func (b *WriteBinBuilder) ListInsert(index int, v any) *WriteSegmentBuilder {
	return b.push(as.ListInsertOp(b.bin, index, v))
}

// ListSet replaces the value at an index.
func (b *WriteBinBuilder) ListSet(index int, v any) *WriteSegmentBuilder {
	return b.push(as.ListSetOp(b.bin, index, v))
}

// ListIncrement adds to the value at an index.
func (b *WriteBinBuilder) ListIncrement(index int, v any) *WriteSegmentBuilder {
	return b.push(as.ListIncrementOp(b.bin, index, v))
}

// ListRemove removes the value at an index.
func (b *WriteBinBuilder) ListRemove(index int) *WriteSegmentBuilder {
	return b.push(as.ListRemoveOp(b.bin, index))
}

// ListRemoveRange removes a range of values.
func (b *WriteBinBuilder) ListRemoveRange(index, count int) *WriteSegmentBuilder {
	return b.push(as.ListRemoveRangeOp(b.bin, index, count))
}

// ListPop removes and returns the value at an index.
func (b *WriteBinBuilder) ListPop(index int) *WriteSegmentBuilder {
	return b.push(as.ListPopOp(b.bin, index))
}

// ListTrim keeps only the given range.
func (b *WriteBinBuilder) ListTrim(index, count int) *WriteSegmentBuilder {
	return b.push(as.ListTrimOp(b.bin, index, count))
}

// ListClear empties the list.
func (b *WriteBinBuilder) ListClear() *WriteSegmentBuilder {
	return b.push(as.ListClearOp(b.bin))
}

// ListSort sorts the list.
func (b *WriteBinBuilder) ListSort(flags as.ListSortFlags) *WriteSegmentBuilder {
	return b.push(as.ListSortOp(b.bin, flags))
}

// ListSize reports the list's length.
func (b *WriteBinBuilder) ListSize() *WriteSegmentBuilder {
	return b.push(as.ListSizeOp(b.bin))
}

// --- Map ---

// MapPut sets one entry.
func (b *WriteBinBuilder) MapPut(p *as.MapPolicy, key, value any) *WriteSegmentBuilder {
	return b.push(as.MapPutOp(p, b.bin, key, value))
}

// MapPutItems sets several entries.
func (b *WriteBinBuilder) MapPutItems(p *as.MapPolicy, items map[any]any) *WriteSegmentBuilder {
	return b.push(as.MapPutItemsOp(p, b.bin, items))
}

// MapIncrement adds to an entry's value.
func (b *WriteBinBuilder) MapIncrement(p *as.MapPolicy, key, delta any) *WriteSegmentBuilder {
	return b.push(as.MapIncrementOp(p, b.bin, key, delta))
}

// MapRemoveByKey removes one entry.
func (b *WriteBinBuilder) MapRemoveByKey(key any, rt MapReturnType) *WriteSegmentBuilder {
	return b.push(mapRemoveByKeyOp(b.bin, key, rt))
}

// MapClear empties the map.
func (b *WriteBinBuilder) MapClear() *WriteSegmentBuilder {
	return b.push(as.MapClearOp(b.bin))
}

// MapSize reports the map's size.
func (b *WriteBinBuilder) MapSize() *WriteSegmentBuilder {
	return b.push(as.MapSizeOp(b.bin))
}

// MapSetPolicy applies a map policy, for example an ordering.
func (b *WriteBinBuilder) MapSetPolicy(p *as.MapPolicy) *WriteSegmentBuilder {
	return b.push(as.MapSetPolicyOp(p, b.bin))
}

// --- HyperLogLog ---

// HLLInit initializes an HLL bin.
func (b *WriteBinBuilder) HLLInit(p *as.HLLPolicy, cfg HLLConfig) *WriteSegmentBuilder {
	return b.push(as.HLLInitOp(p, b.bin, int(cfg.IndexBitCount), int(cfg.MinHashBitCount)))
}

// HLLAdd adds values to an HLL bin.
func (b *WriteBinBuilder) HLLAdd(p *as.HLLPolicy, values ...as.Value) *WriteSegmentBuilder {
	return b.push(as.HLLAddOp(p, b.bin, values, -1, -1))
}

// HLLGetCount reports the estimated cardinality.
func (b *WriteBinBuilder) HLLGetCount() *WriteSegmentBuilder {
	return b.push(as.HLLGetCountOp(b.bin))
}

// HLLDescribe reports the bin's index and min-hash bit counts. Read the result
// with [RecordResult.GetHLLConfig].
func (b *WriteBinBuilder) HLLDescribe() *WriteSegmentBuilder {
	return b.push(as.HLLDescribeOp(b.bin))
}

// --- Bitwise ---

// bitPolicy defaults a nil bit policy. The core client dereferences the policy
// unguarded, so passing nil through would be a crash rather than an error.
func bitPolicy(p *as.BitPolicy) *as.BitPolicy {
	if p == nil {
		return as.DefaultBitPolicy()
	}
	return p
}

// BitSet writes bits at an offset.
func (b *WriteBinBuilder) BitSet(p *as.BitPolicy, offset, size int, value []byte) *WriteSegmentBuilder {
	return b.push(as.BitSetOp(bitPolicy(p), b.bin, offset, size, value))
}

// BitGet reads bits at an offset.
func (b *WriteBinBuilder) BitGet(offset, size int) *WriteSegmentBuilder {
	return b.push(as.BitGetOp(b.bin, offset, size))
}

// BitCount counts the set bits in a range.
func (b *WriteBinBuilder) BitCount(offset, size int) *WriteSegmentBuilder {
	return b.push(as.BitCountOp(b.bin, offset, size))
}

// BitAdd adds to an integer stored in a bit range.
func (b *WriteBinBuilder) BitAdd(p *as.BitPolicy, offset, size int, value int64, signed bool, action as.BitOverflowAction) *WriteSegmentBuilder {
	return b.push(as.BitAddOp(bitPolicy(p), b.bin, offset, size, value, signed, action))
}

// QueryBinBuilder addresses one bin inside a read segment. It exposes the
// read-only subset of the write builder's surface.
type QueryBinBuilder struct {
	parent *QueryBuilder
	bin    string
}

// push appends an operation and returns the parent.
func (b *QueryBinBuilder) push(op *as.Operation) *QueryBuilder {
	b.parent.c.pushOp(op)
	return b.parent
}

// Bin starts the next bin without leaving the segment.
func (b *QueryBinBuilder) Bin(name string) *QueryBinBuilder {
	return &QueryBinBuilder{parent: b.parent, bin: name}
}

// Get reads the bin.
func (b *QueryBinBuilder) Get() *QueryBuilder { return b.push(as.GetBinOp(b.bin)) }

// SelectFrom reads a computed value as a virtual bin.
func (b *QueryBinBuilder) SelectFrom(exp *as.Expression, ignoreEvalFailure bool) *QueryBuilder {
	flags := as.ExpReadFlagDefault
	if ignoreEvalFailure {
		flags = as.ExpReadFlagEvalNoFail
	}
	return b.push(as.ExpReadOp(b.bin, exp, flags))
}

// ListSize reports the list's length.
func (b *QueryBinBuilder) ListSize() *QueryBuilder { return b.push(as.ListSizeOp(b.bin)) }

// ListGet reads the value at an index.
func (b *QueryBinBuilder) ListGet(index int) *QueryBuilder {
	return b.push(as.ListGetOp(b.bin, index))
}

// ListGetRange reads a range of values.
func (b *QueryBinBuilder) ListGetRange(index, count int) *QueryBuilder {
	return b.push(as.ListGetRangeOp(b.bin, index, count))
}

// MapSize reports the map's size.
func (b *QueryBinBuilder) MapSize() *QueryBuilder { return b.push(as.MapSizeOp(b.bin)) }

// MapGetByKey reads one entry.
func (b *QueryBinBuilder) MapGetByKey(key any, rt MapReturnType) *QueryBuilder {
	return b.push(mapGetByKeyOp(b.bin, key, rt))
}

// HLLGetCount reports the estimated cardinality.
func (b *QueryBinBuilder) HLLGetCount() *QueryBuilder { return b.push(as.HLLGetCountOp(b.bin)) }

// HLLDescribe reports the bin's bit counts.
func (b *QueryBinBuilder) HLLDescribe() *QueryBuilder { return b.push(as.HLLDescribeOp(b.bin)) }

// BitGet reads bits at an offset.
func (b *QueryBinBuilder) BitGet(offset, size int) *QueryBuilder {
	return b.push(as.BitGetOp(b.bin, offset, size))
}

// BitCount counts the set bits in a range.
func (b *QueryBinBuilder) BitCount(offset, size int) *QueryBuilder {
	return b.push(as.BitCountOp(b.bin, offset, size))
}

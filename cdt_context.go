// Copyright 2014-2022 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

import (
	"encoding/base64"
	"fmt"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	ctxTypeExpression = 0x04 // Expression-based context
	ctxTypeListIndex  = 0x10
	ctxTypeListRank   = 0x11
	ctxTypeListValue  = 0x13
	ctxTypeMapIndex   = 0x20
	ctxTypeMapRank    = 0x21
	ctxTypeMapKey     = 0x22
	ctxTypeMapValue   = 0x23
)

// CDTContext defines Nested CDT context. Identifies the location of nested list/map to apply the operation.
// for the current level.
// An array of CTX identifies location of the list/map on multiple
// levels on nesting.
type CDTContext struct {
	Id         int
	Value      Value
	Expression *Expression
}

// CDTContextToBase64 converts a []*CDTContext into a base64 encoded string.
func CDTContextToBase64(ctxl []*CDTContext) (string, Error) {
	ctx := cdtContextList(ctxl)
	sz, err := ctx.packArray(nil)
	if err != nil {
		return "", err
	}

	buf := newBuffer(sz)
	_, err = ctx.packArray(buf)
	if err != nil {
		return "", err
	}

	b64 := base64.StdEncoding.EncodeToString(buf.dataBuffer)
	return b64, nil
}

// Base64ToCDTContext converts a b64 encoded string back into a []*CDTContext.
func Base64ToCDTContext(b64 string) ([]*CDTContext, Error) {
	msg, err1 := base64.StdEncoding.DecodeString(b64)
	if err1 != nil {
		return nil, newError(types.PARSE_ERROR, err1.Error())
	}

	unpacker := newUnpacker(msg, 0, len(msg))
	list, err := unpacker.UnpackList()
	if err != nil {
		return nil, err
	}

	if len(list)%2 != 0 {
		return nil, newError(types.PARSE_ERROR, "List count must be even")
	}

	res := make([]*CDTContext, 0, len(list)/2)
	for i := 0; i < len(list); i += 2 {
		id := list[i].(int)
		if id == ctxTypeExpression {
			res = append(res, &CDTContext{Id: id, Expression: newExpression(list[i+1])})
		} else {
			res = append(res, &CDTContext{Id: id, Value: NewValue(list[i+1])})
		}
	}

	return res, nil
}

// String implements the Stringer interface for CDTContext
func (ctx *CDTContext) String() string {
	return fmt.Sprintf("CDTContext{id: %d, value: %s}", ctx.Id, ctx.Value.String())
}

func (ctx *CDTContext) pack(cmd BufferEx) (int, Error) {
	size := 0
	sz, err := packAInt64(cmd, int64(ctx.Id))
	size += sz
	if err != nil {
		return size, err
	}

	// For expression-based contexts, pack the expression bytes
	if ctx.Expression != nil {
		// Get the expression bytes
		expSize, err := ctx.Expression.size()
		if err != nil {
			return size, err
		}
		expBuf := newBuffer(expSize)
		_, err = ctx.Expression.pack(expBuf)
		if err != nil {
			return size, err
		}
		expBytes := expBuf.Bytes()

		// Pack the expression bytes as a byte array and not as a blob!!
		sz, err = packByteArray(cmd, expBytes)
		size += sz
		return size, err
	}

	// For value-based contexts, pack the value
	if ctx.Value != nil {
		sz, err = ctx.Value.pack(cmd)
		size += sz
	}

	return size, err
}

// cdtContextList is used in FilterExpression API
type cdtContextList []*CDTContext

func (ctxl cdtContextList) pack(cmd BufferEx) (int, Error) {
	size := 0
	for i := range ctxl {
		sz, err := ctxl[i].pack(cmd)
		size += sz
		if err != nil {
			return size, err
		}
	}

	return size, nil
}

// used in CreateComplexIndex
func (ctxl cdtContextList) packArray(cmd BufferEx) (int, Error) {
	size, err := packArrayBegin(cmd, len(ctxl)*2)
	if err != nil {
		return size, err
	}

	for i := range ctxl {
		sz, err := ctxl[i].pack(cmd)
		size += sz
		if err != nil {
			return size, err
		}
	}

	return size, nil
}

// CtxListIndex creates a context to lookup a list by index offset.
// If the index is negative, the resolved index starts backwards from end of list.
// If an index is out of bounds, a parameter error will be returned.
//
// Index examples:
// - 0: First item.
// - 4: Fifth item.
// - -1: Last item.
// - -3: Third to last item.
//
// Parameters:
//   - index: List index position
func CtxListIndex(index int) *CDTContext {
	return &CDTContext{ctxTypeListIndex, IntegerValue(index), nil}
}

// CtxListIndexCreate list with given type at index offset, given an order and pad.
func CtxListIndexCreate(index int, order ListOrderType, pad bool) *CDTContext {
	return &CDTContext{ctxTypeListIndex | cdtListOrderFlag(order, pad), IntegerValue(index), nil}
}

// CtxListRank creates a context to lookup a list by rank.
// 0 = smallest value
// N = Nth smallest value
// -1 = largest value
//
// Parameters:
//   - rank: Rank position (0 = smallest, -1 = largest)
func CtxListRank(rank int) *CDTContext {
	return &CDTContext{ctxTypeListRank, IntegerValue(rank), nil}
}

// CtxListValue defines Lookup list by value.
func CtxListValue(key Value) *CDTContext {
	return &CDTContext{ctxTypeListValue, key, nil}
}

// CtxMapIndex defines Lookup map by index offset.
// If the index is negative, the resolved index starts backwards from end of list.
// If an index is out of bounds, a parameter error will be returned.
// Examples:
// 0: First item.
// 4: Fifth item.
// -1: Last item.
// -3: Third to last item.
func CtxMapIndex(index int) *CDTContext {
	return &CDTContext{ctxTypeMapIndex, IntegerValue(index), nil}
}

// CtxMapRank creates a context to lookup a map by rank.
// 0 = smallest value
// N = Nth smallest value
// -1 = largest value
//
// Parameters:
//   - rank: Rank position (0 = smallest, -1 = largest)
func CtxMapRank(rank int) *CDTContext {
	return &CDTContext{ctxTypeMapRank, IntegerValue(rank), nil}
}

// CtxMapKey creates a context to lookup a map by key.
//
// Parameters:
//   - key: The map key to navigate to (use StringValue, IntegerValue, etc.)
func CtxMapKey(key Value) *CDTContext {
	return &CDTContext{ctxTypeMapKey, key, nil}
}

// CtxMapKeyCreate creates map with given type at map key.
func CtxMapKeyCreate(key Value, order mapOrderType) *CDTContext {
	return &CDTContext{ctxTypeMapKey | order.flag, key, nil}
}

// CtxMapValue defines Lookup map by value.
func CtxMapValue(value Value) *CDTContext {
	return &CDTContext{ctxTypeMapValue, value, nil}
}

// CtxAllChildren creates a context that selects all children in a collection.
// This is useful for applying operations to all elements in a list or map.
// Equivalent to CTX.allChildren() in Java client.
//
// Example:
//
//	ctx := []*CDTContext{
//	    CtxMapKey(StringValue("books")),
//	    CtxAllChildren(), // Select all books
//	}
func CtxAllChildren() *CDTContext {
	// Create an expression that always evaluates to true
	expression := ExpBoolVal(true)
	return &CDTContext{
		Id:         ctxTypeExpression,
		Value:      nil,
		Expression: expression,
	}
}

// CtxAllChildrenWithFilter creates a context that selects all children in a collection
// that match the given filter expression.
// Equivalent to CTX.allChildrenWithFilter() in Java client.
//
// Parameters:
//   - exp: Filter expression to apply to each child element
//
// Example:
//
//	// Select all books with price <= 10.0
//	filterExp := ExpLessEq(
//	    ExpMapBin("price"),
//	    ExpFloatVal(10.0),
//	)
//	ctx := []*CDTContext{
//	    CtxMapKey(StringValue("books")),
//	    CtxAllChildrenWithFilter(filterExp),
//	}
func CtxAllChildrenWithFilter(exp *Expression) *CDTContext {
	return &CDTContext{
		Id:         ctxTypeExpression,
		Value:      nil,
		Expression: exp,
	}
}

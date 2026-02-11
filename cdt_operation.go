// Copyright 2014-2025 Aerospike, Inc.
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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

type SelectFlag int
type ModifyFlag int

const (
	// Return a tree from the root (bin) level to the bottom of the tree,
	// with only non-filtered out nodes.
	EXP_PATH_SELECT_MATCHING_TREE SelectFlag = 0

	// Return the list of the values of the nodes finally selected by the context.
	// For maps, this returns the value of each (key, value) pair.
	EXP_PATH_SELECT_VALUE SelectFlag = 1

	// Return the list of the values of the nodes finally selected by the context.
	// This is a synonym for EXP_PATH_SELECT_VALUE to make it clear in your
	// source code that you're expecting a list.
	EXP_PATH_SELECT_LIST_VALUE SelectFlag = 1

	// Return the list of map values of the nodes finally selected by the context.
	// This is a synonym for EXP_PATH_SELECT_VALUE to make it clear in your
	// source code that you're expecting a map.  See also
	// EXP_PATH_SELECT_MAP_KEY_VALUE.
	EXP_PATH_SELECT_MAP_VALUE SelectFlag = 1

	// Return the list of map keys of the nodes finally selected by the context.
	EXP_PATH_SELECT_MAP_KEY SelectFlag = 2

	// Returns the list of map (key, value) pairs of the nodes finally selected
	// by the context.  This is a synonym for setting both
	// EXP_PATH_SELECT_MAP_KEY and EXP_PATH_SELECT_MAP_VALUE bits together.
	EXP_PATH_SELECT_MAP_KEY_VALUE SelectFlag = EXP_PATH_SELECT_MAP_KEY | EXP_PATH_SELECT_MAP_VALUE

	// If the expression in the context hits an invalid type (e.g., selects
	// as an integer when the value is a string), do not fail the operation;
	// just ignore those elements.  Interpret UNKNOWN as false instead.
	EXP_PATH_SELECT_NO_FAIL SelectFlag = 0x10
)

const (
	// If the expression in the context hits an invalid type, the operation
	// will fail.  This is the default behavior.
	EXP_PATH_MODIFY_DEFAULT ModifyFlag = 0x00

	// This flag is set when leaf values are to be modified.
	EXP_PATH_MODIFY_APPLY ModifyFlag = 0x04

	// If the expression in the context hits an invalid type (e.g., selects
	// as an integer when the value is a string), do not fail the operation;
	// just ignore those elements.  Interpret UNKNOWN as false instead.
	EXP_PATH_MODIFY_NO_FAIL ModifyFlag = 0x10
)

// SelectByPath creates CDT select operation with context.
// Equivalent to as_operations_cdt_select in C client.
//
// Parameters:
//   - binName: bin name
//   - flags: select flags
//   - ctx: optional path to nested CDT. If not defined, the top-level CDT is used.
//
// Returns nil if ctx is nil.
func SelectByPath(binName string, flag SelectFlag, ctx ...*CDTContext) *Operation {
	if len(ctx) == 0 {
		return &Operation{
			opType:    _CDT_READ,
			ctx:       nil,
			binName:   binName,
			opSubType: cdtOperationTypeSELECT,
			binValue:  IntegerValue(flag),
			encoder:   newCDTCreateSelectEncoder,
		}
	}

	return &Operation{
		opType:    _CDT_READ,
		ctx:       ctx,
		binName:   binName,
		opSubType: cdtOperationTypeSELECT,
		binValue:  IntegerValue(flag),
		encoder:   newCDTCreateSelectEncoder,
	}
}

// ModifyByPath creates CDT apply operation with context and modify expression.
// Equivalent to as_operations_cdt_apply in C client.
//
// Parameters:
//   - binName: bin name
//   - flags: select flags
//   - modifyExp: modify expression
//   - ctx: optional path to nested CDT. If not defined, the top-level CDT is used.
//
// Returns nil if ctx is nil.
func ModifyByPath(binName string, flag ModifyFlag, modifyExp *Expression, ctx ...*CDTContext) *Operation {
	if len(ctx) == 0 {
		return &Operation{
			opType:    _CDT_MODIFY,
			ctx:       ctx,
			binName:   binName,
			opSubType: cdtOperationTypeSELECT,
			binValue:  ListValue([]any{flag, modifyExp}),
			encoder:   newCDTCreateModifyEncoder,
		}
	}

	return &Operation{
		opType:    _CDT_MODIFY,
		ctx:       ctx,
		binName:   binName,
		opSubType: cdtOperationTypeSELECT,
		binValue:  ListValue([]any{flag, modifyExp}),
		encoder:   newCDTCreateModifyEncoder,
	}
}

func newCDTCreateSelectEncoder(op *Operation, packer BufferEx) (int, Error) {
	return packIfCDTSelect(packer, *op.opSubType, op.ctx, op.binValue.(IntegerValue))
}

func newCDTCreateModifyEncoder(op *Operation, packer BufferEx) (int, Error) {
	return packIfCDTModify(packer, *op.opSubType, op.ctx, op.binValue.(ListValue))
}

func packIfCDTModify(packer BufferEx, opType int, ctx []*CDTContext, params ListValue) (int, Error) {
	// Initialize size tracking
	size := 0
	n := 0
	var err Error

	// Pack the outer array with 4 elements: [opType, context_array, params_array, <reserved>]
	if n, err = packArrayBegin(packer, 4); err != nil {
		return size + n, err
	}
	size += n

	// Pack the operation type as the first element
	if n, err = packAInt(packer, opType); err != nil {
		return size + n, err
	}
	size += n

	if n, err = packArrayBegin(packer, len(ctx)*2); err != nil {
		return size + n, err
	}
	size += n

	for _, c := range ctx {
		// Pack the context id
		if n, err = packAInt64(packer, int64(c.Id)); err != nil {
			return size + n, err
		}
		size += n

		// Pack the context value or expression
		// Each CDTContext must have either a Value (literal) or an Expression (computed)
		if c.Value != nil {
			if n, err = c.Value.pack(packer); err != nil {
				return size + n, err
			}
			size += n
		} else if c.Expression != nil {
			if n, err = c.Expression.pack(packer); err != nil {
				return size + n, err
			}
			size += n
		} else {
			// Error: context must have either a Value or Expression defined
			return size, newError(types.PARAMETER_ERROR, "CDTContext must have either a Value or an Expression")
		}
	}

	// Extract flag and expression from params
	// params should be [flag, modifyExp]
	if len(params) != 2 {
		return size, newError(types.PARAMETER_ERROR, "CDTModifyByPath requires flag and expression")
	}

	flag, ok := params[0].(ModifyFlag)
	if !ok {
		return size, newError(types.PARAMETER_ERROR, "First parameter must be a SelectFlag")
	}

	modifyExp, ok := params[1].(*Expression)
	if !ok {
		return size, newError(types.PARAMETER_ERROR, "Second parameter must be an Expression")
	}

	// Element 3: Pack flags | EXP_PATH_MODIFY_APPLY (ensure apply flag is set)
	if n, err = packAInt64(packer, int64(flag|EXP_PATH_MODIFY_APPLY)); err != nil {
		return size + n, err
	}
	size += n

	// Element 4: Pack expression bytes directly
	expSize, err := modifyExp.size()
	if err != nil {
		return size, err
	}
	expBuf := newBuffer(expSize)
	_, err = modifyExp.pack(expBuf)
	if err != nil {
		return size, err
	}
	expBytes := expBuf.Bytes()

	if n, err = packByteArray(packer, expBytes); err != nil {
		return size + n, err
	}
	size += n

	return size, nil
}

func packIfCDTSelect(packer BufferEx, opType int, ctx []*CDTContext, flag IntegerValue) (int, Error) {
	// Initialize size tracking
	size := 0
	n := 0
	var err Error

	// Pack the outer array with 3 elements: [opType, context_array, flag]
	if n, err = packArrayBegin(packer, 3); err != nil {
		return size + n, err
	}
	size += n

	// Pack the operation type as the first element
	if n, err = packAInt(packer, opType); err != nil {
		return size + n, err
	}
	size += n

	if n, err = packArrayBegin(packer, len(ctx)*2); err != nil {
		return size + n, err
	}
	size += n

	for _, c := range ctx {
		// Pack the context id
		if n, err = packAInt64(packer, int64(c.Id)); err != nil {
			return size + n, err
		}
		size += n

		// Pack the context value or expression
		// Each CDTContext must have either a Value (literal) or an Expression (computed)
		if c.Value != nil {
			if n, err = c.Value.pack(packer); err != nil {
				return size + n, err
			}
			size += n
		} else if c.Expression != nil {
			if n, err = c.Expression.pack(packer); err != nil {
				return size + n, err
			}
			size += n
		} else {
			// Error: context must have either a Value or Expression defined
			return size, newError(types.PARAMETER_ERROR, "CDTContext must have either a Value or an Expression")
		}
	}

	// Pack the select flag as the third and final element
	// Clear the MODIFY_APPLY flag (bit 4) to match Java client behavior
	if n, err = packAInt64(packer, int64(flag)&^4); err != nil {
		return size + n, err
	}
	size += n

	return size, nil
}

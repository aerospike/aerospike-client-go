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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	cdtOperationTypeSELECT = 0xfe
	cdtOperationTypeMODIFY = 0xff
)

// CDTSelectByPath creates CDT select operation with context.
// Equivalent to as_operations_cdt_select in C client.
//
// Parameters:
//   - binName: bin name
//   - flags: select flags
//   - ctx: optional path to nested CDT. If not defined, the top-level CDT is used.
//
// Returns nil if ctx is nil.
func CDTSelectByPath(binName string, flags int, ctx ...*CDTContext) *Operation {
	if ctx == nil {
		return nil
	}

	return &Operation{
		opType: _CDT_READ,
		ctx: ctx,
		binName: binName,
		encoder: packCDTSelect,
	}
}

// CDTModifyByPath creates CDT apply operation with context and modify expression.
// Equivalent to as_operations_cdt_apply in C client.
//
// Parameters:
//   - binName: bin name
//   - flags: select flags
//   - modifyExp: modify expression
//   - ctx: optional path to nested CDT. If not defined, the top-level CDT is used.
//
// Returns nil if ctx is nil.
func CDTModifyByPath(binName string, flags int, modifyExp *Expression, ctx ...*CDTContext) *Operation {
	if ctx == nil {
		return nil
	}

	return &Operation{
		opType:   _CDT_MODIFY,
		ctx: ctx,
		binName:  binName,
		encoder: packCDTApply,
	}
}

// packCDTSelectBuffer packs a CDT select operation into the provided buffer.
// If packer is nil, only calculates and returns the size and expression bytes.
// If packer is not nil, ctxExpBytes must be provided from a previous call with nil packer.
func packCDTSelectBuffer(packer BufferEx, flags int, opType int, ctxExpBytes [][]byte, ctx ...*CDTContext) (int, [][]byte, Error) {
	size := 0
	var err Error
	var n int

	// Pack array begin with 3 elements
	n, err = packArrayBegin(packer, 3)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pack operation type
	n, err = packAInt(packer, opType)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pack context array (length * 2 because each context has id and value)
	n, err = packArrayBegin(packer, len(ctx)*2)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pre-pack expression bytes if needed (only once, when packer is nil)
	if packer == nil {
		ctxExpBytes = make([][]byte, len(ctx))
	}

	// Pack each context
	for i, c := range ctx {
		n, err = packAInt(packer, c.Id)
		if err != nil {
			return size + n, ctxExpBytes, err
		}
		size += n

		// Pack expression if present, otherwise pack value
		if c.Expression != nil {
			// Get expression bytes (cache them on first pass when packer is nil)
			if packer == nil {
				expSize, err := c.Expression.size()
				if err != nil {
					return size, ctxExpBytes, err
				}
				expBuf := newBuffer(expSize)
				_, err = c.Expression.pack(expBuf)
				if err != nil {
					return size, ctxExpBytes, err
				}
				ctxExpBytes[i] = expBuf.Bytes()
			}

			n, err = packBytes(packer, ctxExpBytes[i])
			if err != nil {
				return size + n, ctxExpBytes, err
			}
			size += n
		} else if c.Value != nil {
			n, err = c.Value.pack(packer)
			if err != nil {
				return size + n, ctxExpBytes, err
			}
			size += n
		} else {
			return size, ctxExpBytes, newError(types.PARAMETER_ERROR, "CDTContext must have either a Value or an Expression")
		}
	}

	// Pack flags
	n, err = packAInt(packer, flags)
	if err != nil {
		return size + n, ctxExpBytes, err
	}
	size += n

	return size, ctxExpBytes, nil
}

// packCDTSelect packs a CDT select operation.
func packCDTSelect(flags int, opType int, ctx ...*CDTContext) ([]byte, Error) {
	// First pass: calculate size and pre-pack expressions
	size, ctxExpBytes, err := packCDTSelectBuffer(nil, flags, opType, nil, ctx...)
	if err != nil {
		return nil, err
	}

	// Second pass: allocate buffer and pack
	packer := newBuffer(size)
	_, _, err = packCDTSelectBuffer(packer, flags, opType, ctxExpBytes, ctx...)
	if err != nil {
		return nil, err
	}

	return packer.Bytes(), nil
}

// packCDTApplyBuffer packs a CDT apply operation into the provided buffer.
// If packer is nil, only calculates and returns the size and expression bytes.
// If packer is not nil, ctxExpBytes must be provided from a previous call with nil packer.
func packCDTApplyBuffer(packer BufferEx, flags int, opType int, modifyExpBytes []byte, ctxExpBytes [][]byte, ctx ...*CDTContext) (int, [][]byte, Error) {
	size := 0
	var err Error
	var n int

	// Pack array begin with 4 elements
	n, err = packArrayBegin(packer, 4)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pack operation type
	n, err = packAInt(packer, opType)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pack context array (length * 2 because each context has id and value)
	n, err = packArrayBegin(packer, len(ctx)*2)
	if err != nil {
		return size + n, nil, err
	}
	size += n

	// Pre-pack expression bytes if needed (only once, when packer is nil)
	if packer == nil {
		ctxExpBytes = make([][]byte, len(ctx))
	}

	// Pack each context
	for i, c := range ctx {
		n, err = packAInt(packer, c.Id)
		if err != nil {
			return size + n, ctxExpBytes, err
		}
		size += n

		// Pack expression if present, otherwise pack value
		if c.Expression != nil {
			// Get expression bytes (cache them on first pass when packer is nil)
			if packer == nil {
				ctxExpSize, err := c.Expression.size()
				if err != nil {
					return size, ctxExpBytes, err
				}
				ctxExpBuf := newBuffer(ctxExpSize)
				_, err = c.Expression.pack(ctxExpBuf)
				if err != nil {
					return size, ctxExpBytes, err
				}
				ctxExpBytes[i] = ctxExpBuf.Bytes()
			}

			n, err = packBytes(packer, ctxExpBytes[i])
			if err != nil {
				return size + n, ctxExpBytes, err
			}
			size += n
		} else if c.Value != nil {
			n, err = c.Value.pack(packer)
			if err != nil {
				return size + n, ctxExpBytes, err
			}
			size += n
		} else {
			return size, ctxExpBytes, newError(types.PARAMETER_ERROR, "CDTContext must have either a Value or an Expression")
		}
	}

	// Pack flags
	n, err = packAInt(packer, flags)
	if err != nil {
		return size + n, ctxExpBytes, err
	}
	size += n

	// Pack modify expression bytes
	n, err = packBytes(packer, modifyExpBytes)
	if err != nil {
		return size + n, ctxExpBytes, err
	}
	size += n

	return size, ctxExpBytes, nil
}

// packCDTApply packs a CDT apply operation with modify expression.
func packCDTApply(flags int, opType int, modifyExp *Expression, ctx ...*CDTContext) ([]byte, Error) {
	// Get expression bytes
	expSize, err := modifyExp.size()
	if err != nil {
		return nil, err
	}
	expBuf := newBuffer(expSize)
	_, err = modifyExp.pack(expBuf)
	if err != nil {
		return nil, err
	}
	expBytes := expBuf.Bytes()

	// First pass: calculate size and pre-pack context expressions
	size, ctxExpBytes, err := packCDTApplyBuffer(nil, flags, opType, expBytes, nil, ctx...)
	if err != nil {
		return nil, err
	}

	// Second pass: allocate buffer and pack
	packer := newBuffer(size)
	_, _, err = packCDTApplyBuffer(packer, flags, opType, expBytes, ctxExpBytes, ctx...)
	if err != nil {
		return nil, err
	}

	return packer.Bytes(), nil
}

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

var (
	// Opcode used to encode a CDT select and apply operation.
	// If flag bit 2 is clear, the operation will be interpreted as a select
	// operation; otherwise, as an apply operation.
	selectVal = int(0xfe)

	// Opcode used to encode the calling of a virtual operation.
	contextEval = int(0xff)

	cdtOperationTypeSELECT  = operationSubType(&selectVal)
	cdtOperationContextEVAL = operationSubType(&contextEval)
)

func newCDTCreateOperationEncoder(op *Operation, packer BufferEx) (int, Error) {
	if op.binValue != nil {
		if params := op.binValue.(ListValue); len(params) > 0 {
			return packCDTIfcParamsAsArray(packer, *op.opSubType, op.ctx, op.binValue.(ListValue))
		}
	}
	return packCDTParamsAsArray(packer, *op.opSubType, op.ctx)
}

func newCDTCreateOperationValues2(command int, attributes mapOrderType, binName string, ctx []*CDTContext, value1 any, value2 any) *Operation {
	return &Operation{
		opType:    _MAP_MODIFY,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{value1, value2, IntegerValue(attributes.attr)}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValues0(command int, typ OperationType, binName string, ctx []*CDTContext) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		// binValue: NewNullValue(),
		encoder: newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValuesN(command int, typ OperationType, binName string, ctx []*CDTContext, values []any, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), ListValue(values)}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValue1(command int, typ OperationType, binName string, ctx []*CDTContext, value any, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), value}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationIndex(command int, typ OperationType, binName string, ctx []*CDTContext, index int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), index}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationIndexCount(command int, typ OperationType, binName string, ctx []*CDTContext, index int, count int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), index, count}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTMapCreateOperationRelativeIndex(command int, typ OperationType, binName string, ctx []*CDTContext, key Value, index int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), key, index}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTMapCreateOperationRelativeIndexCount(command int, typ OperationType, binName string, ctx []*CDTContext, key Value, index int, count int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), key, index, count}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateRangeOperation(command int, typ OperationType, binName string, ctx []*CDTContext, begin any, end any, returnType mapReturnType) *Operation {
	if end == nil {
		return &Operation{
			opType:    typ,
			opSubType: &command,
			ctx:       ctx,
			binName:   binName,
			binValue:  ListValue([]any{IntegerValue(returnType), begin}),
			encoder:   newCDTCreateOperationEncoder,
		}
	}

	return &Operation{
		opType:    typ,
		opSubType: &command,
		ctx:       ctx,
		binName:   binName,
		binValue:  ListValue([]any{IntegerValue(returnType), begin, end}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

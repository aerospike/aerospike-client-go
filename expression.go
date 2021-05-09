// Copyright 2014-2021 Aerospike, Inc.
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
// limitations under the License.package aerospike

package aerospike

import (
	"encoding/base64"

	ParticleType "github.com/aerospike/aerospike-client-go/internal/particle_type"
)

// ExpressionArgument is used for passing arguments to filter expressions.
// The accptable arguments are:
// Value, ExpressionFilter, []*CDTContext
type ExpressionArgument interface {
	pack(BufferEx) (int, Error)
}

// ExpType defines the expression's data type.
type ExpType uint

var (
	// ExpTypeNIL is NIL Expression Type
	ExpTypeNIL ExpType = 0
	// ExpTypeBOOL is BOOLEAN Expression Type
	ExpTypeBOOL ExpType = 1
	// ExpTypeINT is INTEGER Expression Type
	ExpTypeINT ExpType = 2
	// ExpTypeSTRING is STRING Expression Type
	ExpTypeSTRING ExpType = 3
	// ExpTypeLIST is LIST Expression Type
	ExpTypeLIST ExpType = 4
	// ExpTypeMAP is MAP Expression Type
	ExpTypeMAP ExpType = 5
	// ExpTypeBLOB is BLOB Expression Type
	ExpTypeBLOB ExpType = 6
	// ExpTypeFLOAT is FLOAT Expression Type
	ExpTypeFLOAT ExpType = 7
	// ExpTypeGEO is GEO String Expression Type
	ExpTypeGEO ExpType = 8
	// ExpTypeHLL is HLL Expression Type
	ExpTypeHLL ExpType = 9
)

type expOp uint

var (
	expOpUnknown       expOp = 0
	expOpEQ            expOp = 1
	expOpNE            expOp = 2
	expOpGT            expOp = 3
	expOpGE            expOp = 4
	expOpLT            expOp = 5
	expOpLE            expOp = 6
	expOpREGEX         expOp = 7
	expOpGEO           expOp = 8
	expOpAND           expOp = 16
	expOpOR            expOp = 17
	expOpNOT           expOp = 18
	expOpExclusive     expOp = 19
	expOpAdd           expOp = 20
	expOpSub           expOp = 21
	expOpMul           expOp = 22
	expOpDiv           expOp = 23
	expOpPow           expOp = 24
	expOpLog           expOp = 25
	expOpMod           expOp = 26
	expOpAbs           expOp = 27
	expOpFloor         expOp = 28
	expOpCeil          expOp = 29
	expOpToInt         expOp = 30
	expOpToFloat       expOp = 31
	expOpIntAnd        expOp = 32
	expOpIntOr         expOp = 33
	expOpIntXor        expOp = 34
	expOpIntNot        expOp = 35
	expOpIntLShift     expOp = 36
	expOpIntRShift     expOp = 37
	expOpIntARShift    expOp = 38
	expOpIntCount      expOp = 39
	expOpIntLscan      expOp = 40
	expOpIntRscan      expOp = 41
	expOpMin           expOp = 50
	expOpMax           expOp = 51
	expOpDIGEST_MODULO expOp = 64
	expOpDEVICE_SIZE   expOp = 65
	expOpLAST_UPDATE   expOp = 66
	expOpSINCE_UPDATE  expOp = 67
	expOpVOID_TIME     expOp = 68
	expOpTTL           expOp = 69
	expOpSET_NAME      expOp = 70
	expOpKEY_EXISTS    expOp = 71
	expOpIS_TOMBSTONE  expOp = 72
	expOpMEMORY_SIZE   expOp = 73
	expOpKEY           expOp = 80
	expOpBIN           expOp = 81
	expOpBIN_TYPE      expOp = 82
	expOpCond          expOp = 123
	expOpVar           expOp = 124
	expOpLet           expOp = 125
	expOpQUOTED        expOp = 126
	expOpCALL          expOp = 127
)

const _MODIFY = 0x40

// ExpRegexFlags is used to change the Regex Mode in Expression Filters.
type ExpRegexFlags int

const (
	// ExpRegexFlagNONE uses regex defaults.
	ExpRegexFlagNONE ExpRegexFlags = 0

	// ExpRegexFlagEXTENDED uses POSIX Extended Regular Expression syntax when interpreting regex.
	ExpRegexFlagEXTENDED ExpRegexFlags = 1 << 0

	// ExpRegexFlagICASE does not differentiate cases.
	ExpRegexFlagICASE ExpRegexFlags = 1 << 1

	// ExpRegexFlagNOSUB does not report position of matches.
	ExpRegexFlagNOSUB ExpRegexFlags = 1 << 2

	// ExpRegexFlagNEWLINE does not Match-any-character operators don't match a newline.
	ExpRegexFlagNEWLINE ExpRegexFlags = 1 << 3
)

// FilterExpression which can be applied to most commands, to control which records are
// affected by the command.
type FilterExpression struct {
	// The Operation code
	cmd *expOp
	// The Primary Value of the Operation
	val Value
	// The Bin to use it on (REGEX for example)
	bin *FilterExpression
	// The additional flags for the Operation (REGEX or return_type of Module for example)
	flags *int64
	// The optional Module flag for Module operations or Bin Types
	module *ExpType
	// Sub commands for the CmdExp operation
	exps []*FilterExpression

	arguments []ExpressionArgument
}

func newFilterExpression(
	cmd *expOp,
	val Value,
	bin *FilterExpression,
	flags *int64,
	module *ExpType,
	exps []*FilterExpression,
) *FilterExpression {
	return &FilterExpression{
		cmd:       cmd,
		val:       val,
		bin:       bin,
		flags:     flags,
		module:    module,
		exps:      exps,
		arguments: nil,
	}
}

func (fe *FilterExpression) packExpression(
	exps []*FilterExpression,
	buf BufferEx,
) (int, Error) {
	size := 0

	if fe.val != nil {
		// DEF expression
		sz, err := packRawString(buf, fe.val.String())
		size += sz
		if err != nil {
			return size, err
		}

		sz, err = exps[0].pack(buf)
		size += sz
		if err != nil {
			return size, err
		}
	} else {
		if fe.cmd == &expOpLet {
			// Let wire format: LET <defname1>, <defexp1>, <defname2>, <defexp2>, ..., <scope exp>
			count := (len(exps)-1)*2 + 2
			sz, err := packArrayBegin(buf, count)
			size += sz
			if err != nil {
				return size, err
			}
		} else {
			sz, err := packArrayBegin(buf, len(exps)+1)
			size += sz
			if err != nil {
				return size, err
			}
		}

		sz, err := packAInt64(buf, int64(*fe.cmd))
		size += sz
		if err != nil {
			return size, err
		}

		for _, exp := range exps {
			sz, err = exp.pack(buf)
			size += sz
			if err != nil {
				return size, err
			}
		}
	}
	return size, nil
}

func (fe *FilterExpression) packCommand(cmd *expOp, buf BufferEx) (int, Error) {
	size := 0

	switch cmd {
	case &expOpREGEX:
		sz, err := packArrayBegin(buf, 4)
		if err != nil {
			return size, err
		}
		size += sz
		// The Operation
		sz, err = packAInt64(buf, int64(*cmd))
		if err != nil {
			return size, err
		}
		size += sz
		// Regex Flags
		sz, err = packAInt64(buf, *fe.flags)
		if err != nil {
			return size, err
		}
		size += sz
		// Raw String is needed instead of the msgpack String that the pack_value method would use.
		sz, err = packRawString(buf, fe.val.String())
		if err != nil {
			return size, err
		}
		size += sz
		// The Bin
		sz, err = fe.bin.pack(buf)
		if err != nil {
			return size, err
		}
		size += sz
	case &expOpCALL:
		// Packing logic for Module
		sz, err := packArrayBegin(buf, 5)
		if err != nil {
			return size, err
		}
		size += sz
		// The Operation
		sz, err = packAInt64(buf, int64(*cmd))
		if err != nil {
			return size, err
		}
		size += sz
		// The Module Operation
		sz, err = packAInt64(buf, int64(*fe.module))
		if err != nil {
			return size, err
		}
		size += sz
		// The Module (List/Map or Bitwise)
		sz, err = packAInt64(buf, *fe.flags)
		if err != nil {
			return size, err
		}
		size += sz
		// Encoding the Arguments
		if args := fe.arguments; len(args) > 0 {
			argLen := 0
			for _, arg := range args {
				// First match to estimate the Size and write the Context
				switch v := arg.(type) {
				case Value, *FilterExpression:
					argLen++
				case cdtContextList:
					if len(v) > 0 {
						sz, err = packArrayBegin(buf, 3)
						if err != nil {
							return size, err
						}
						size += sz

						sz, err = packAInt64(buf, 0xff)
						if err != nil {
							return size, err
						}
						size += sz

						sz, err = packArrayBegin(buf, len(v)*2)
						if err != nil {
							return size, err
						}
						size += sz

						for _, c := range v {
							sz, err = c.pack(buf)
							if err != nil {
								return size, err
							}
							size += sz
						}
					}
				default:
					panic("Value `%v` is not acceptable in Expression Filters as an argument")
				}
			}
			sz, err = packArrayBegin(buf, argLen)
			if err != nil {
				return size, err
			}
			size += sz
			// Second match to write the real values
			for _, arg := range args {
				switch val := arg.(type) {
				case Value:
					sz, err = val.pack(buf)
					if err != nil {
						return size, err
					}
					size += sz
				case *FilterExpression:
					sz, err = val.pack(buf)
					if err != nil {
						return size, err
					}
					size += sz
				default:
				}
			}
		} else {
			// No Arguments
			sz, err = fe.val.pack(buf)
			if err != nil {
				return size, err
			}
			size += sz
		}
		// Write the Bin
		sz, err = fe.bin.pack(buf)
		if err != nil {
			return size, err
		}
		size += sz
	case &expOpBIN:
		// Bin Encoder
		sz, err := packArrayBegin(buf, 3)
		if err != nil {
			return size, err
		}
		size += sz
		// The Bin Operation
		sz, err = packAInt64(buf, int64(*cmd))
		if err != nil {
			return size, err
		}
		size += sz
		// The Bin Type (INT/String etc.)
		sz, err = packAInt64(buf, int64(*fe.module))
		if err != nil {
			return size, err
		}
		size += sz
		// The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
		sz, err = packRawString(buf, fe.val.String())
		if err != nil {
			return size, err
		}
		size += sz
	case &expOpVar:
		fallthrough
	case &expOpBIN_TYPE:
		// BinType encoder
		sz, err := packArrayBegin(buf, 2)
		if err != nil {
			return size, err
		}
		size += sz
		// BinType Operation
		sz, err = packAInt64(buf, int64(*cmd))
		if err != nil {
			return size, err
		}
		size += sz
		// The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
		sz, err = packRawString(buf, fe.val.String())
		if err != nil {
			return size, err
		}
		size += sz
	default:
		// Packing logic for all other Ops
		if value := fe.val; value != nil {
			// Operation has a Value
			sz, err := packArrayBegin(buf, 2)
			if err != nil {
				return size, err
			}
			size += sz
			// Write the Operation
			sz, err = packAInt64(buf, int64(*cmd))
			if err != nil {
				return size, err
			}
			size += sz
			// Write the Value
			sz, err = value.pack(buf)
			if err != nil {
				return size, err
			}
			size += sz
		} else {
			// Operation has no Value
			sz, err := packArrayBegin(buf, 1)
			if err != nil {
				return size, err
			}
			size += sz
			// Write the Operation
			sz, err = packAInt64(buf, int64(*cmd))
			if err != nil {
				return size, err
			}
			size += sz
		}
	}

	return size, nil
}

func (fe *FilterExpression) packValue(buf BufferEx) (int, Error) {
	// Packing logic for Value based Ops
	return fe.val.pack(buf)
}

func (fe *FilterExpression) pack(buf BufferEx) (int, Error) {
	if len(fe.exps) > 0 {
		return fe.packExpression(fe.exps, buf)
	} else if fe.cmd != nil {
		return fe.packCommand(fe.cmd, buf)
	}
	return fe.packValue(buf)
}

func (fe *FilterExpression) base64() (string, Error) {
	sz, err := fe.pack(nil)
	if err != nil {
		return "", err
	}

	input := newBuffer(sz)
	_, err = fe.pack(input)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(input.dataBuffer[:input.dataOffset]), nil
}

// ExpKey creates a record key expression of specified type.
func ExpKey(expType ExpType) *FilterExpression {
	return newFilterExpression(
		&expOpKEY,
		IntegerValue(int64(expType)),
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpKeyExists creates a function that returns if the primary key is stored in the record meta data
// as a boolean expression. This would occur when `send_key` is true on record write.
func ExpKeyExists() *FilterExpression {
	return newFilterExpression(&expOpKEY_EXISTS, nil, nil, nil, nil, nil)
}

// ExpIntBin creates a 64 bit int bin expression.
func ExpIntBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeINT,
		nil,
	)
}

// ExpStringBin creates a string bin expression.
func ExpStringBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeSTRING,
		nil,
	)
}

// ExpBlobBin creates a blob bin expression.
func ExpBlobBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeBLOB,
		nil,
	)
}

// ExpFloatBin creates a 64 bit float bin expression.
func ExpFloatBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeFLOAT,
		nil,
	)
}

// ExpGeoBin creates a geo bin expression.
func ExpGeoBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeGEO,
		nil,
	)
}

// ExpListBin creates a list bin expression.
func ExpListBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeLIST,
		nil,
	)
}

// ExpMapBin creates a map bin expression.
func ExpMapBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeMAP,
		nil,
	)
}

// ExpHLLBin creates a a HLL bin expression
func ExpHLLBin(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN,
		StringValue(name),
		nil,
		nil,
		&ExpTypeHLL,
		nil,
	)
}

// ExpBinExists creates a function that returns if bin of specified name exists.
func ExpBinExists(name string) *FilterExpression {
	return ExpNotEq(ExpBinType(name), ExpIntVal(ParticleType.NULL))
}

// ExpBinType creates a function that returns bin's integer particle type.
func ExpBinType(name string) *FilterExpression {
	return newFilterExpression(
		&expOpBIN_TYPE,
		StringValue(name),
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpSetName creates a function that returns record set name string.
func ExpSetName() *FilterExpression {
	return newFilterExpression(&expOpSET_NAME, nil, nil, nil, nil, nil)
}

// ExpDeviceSize creates a function that returns record size on disk.
// If server storage-engine is memory, then zero is returned.
func ExpDeviceSize() *FilterExpression {
	return newFilterExpression(&expOpDEVICE_SIZE, nil, nil, nil, nil, nil)
}

// ExpMemorySize creates expression that returns record size in memory. If server storage-engine is
// not memory nor data-in-memory, then zero is returned. This expression usually evaluates
// quickly because record meta data is cached in memory.
func ExpMemorySize() *FilterExpression {
	return newFilterExpression(&expOpMEMORY_SIZE, nil, nil, nil, nil, nil)
}

// ExpLastUpdate creates a function that returns record last update time expressed as 64 bit integer
// nanoseconds since 1970-01-01 epoch.
func ExpLastUpdate() *FilterExpression {
	return newFilterExpression(&expOpLAST_UPDATE, nil, nil, nil, nil, nil)
}

// ExpSinceUpdate creates a expression that returns milliseconds since the record was last updated.
// This expression usually evaluates quickly because record meta data is cached in memory.
func ExpSinceUpdate() *FilterExpression {
	return newFilterExpression(&expOpSINCE_UPDATE, nil, nil, nil, nil, nil)
}

// ExpVoidTime creates a function that returns record expiration time expressed as 64 bit integer
// nanoseconds since 1970-01-01 epoch.
func ExpVoidTime() *FilterExpression {
	return newFilterExpression(&expOpVOID_TIME, nil, nil, nil, nil, nil)
}

// ExpTTL creates a function that returns record expiration time (time to live) in integer seconds.
func ExpTTL() *FilterExpression {
	return newFilterExpression(&expOpTTL, nil, nil, nil, nil, nil)
}

// ExpIsTombstone creates a expression that returns if record has been deleted and is still in tombstone state.
// This expression usually evaluates quickly because record meta data is cached in memory.
func ExpIsTombstone() *FilterExpression {
	return newFilterExpression(&expOpIS_TOMBSTONE, nil, nil, nil, nil, nil)
}

// ExpDigestModulo creates a function that returns record digest modulo as integer.
func ExpDigestModulo(modulo int64) *FilterExpression {
	return newFilterExpression(
		&expOpDIGEST_MODULO,
		NewValue(modulo),
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpRegexCompare creates a function like regular expression string operation.
func ExpRegexCompare(regex string, flags ExpRegexFlags, bin *FilterExpression) *FilterExpression {
	iflags := int64(flags)
	return newFilterExpression(
		&expOpREGEX,
		StringValue(regex),
		bin,
		&iflags,
		nil,
		nil,
	)
}

// ExpGeoCompare creates a compare geospatial operation.
func ExpGeoCompare(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return newFilterExpression(
		&expOpGEO,
		nil,
		nil,
		nil,
		nil,
		[]*FilterExpression{left, right},
	)
}

// ExpIntVal creates a 64 bit integer value
func ExpIntVal(val int64) *FilterExpression {
	return newFilterExpression(nil, IntegerValue(val), nil, nil, nil, nil)
}

// ExpBoolVal creates a Boolean value
func ExpBoolVal(val bool) *FilterExpression {
	return newFilterExpression(nil, BoolValue(val), nil, nil, nil, nil)
}

// ExpStringVal creates a String bin value
func ExpStringVal(val string) *FilterExpression {
	return newFilterExpression(nil, StringValue(val), nil, nil, nil, nil)
}

// ExpFloatVal creates a 64 bit float bin value
func ExpFloatVal(val float64) *FilterExpression {
	return newFilterExpression(nil, FloatValue(val), nil, nil, nil, nil)
}

// ExpBlobVal creates a Blob bin value
func ExpBlobVal(val []byte) *FilterExpression {
	return newFilterExpression(nil, BytesValue(val), nil, nil, nil, nil)
}

// ExpListVal creates a List bin Value
func ExpListVal(val ...Value) *FilterExpression {
	return newFilterExpression(
		&expOpQUOTED,
		ValueArray(val),
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpValueArrayVal creates a List bin Value
func ExpValueArrayVal(val ValueArray) *FilterExpression {
	return newFilterExpression(
		&expOpQUOTED,
		val,
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpListValueVal creates a List bin Value
func ExpListValueVal(val ...interface{}) *FilterExpression {
	return newFilterExpression(
		&expOpQUOTED,
		NewListValue(val),
		nil,
		nil,
		nil,
		nil,
	)
}

// ExpMapVal creates a Map bin Value
func ExpMapVal(val MapValue) *FilterExpression {
	return newFilterExpression(nil, val, nil, nil, nil, nil)
}

// ExpGeoVal creates a geospatial json string value.
func ExpGeoVal(val string) *FilterExpression {
	return newFilterExpression(nil, GeoJSONValue(val), nil, nil, nil, nil)
}

// ExpNilValue creates a a Nil Value
func ExpNilValue() *FilterExpression {
	return newFilterExpression(nil, nullValue, nil, nil, nil, nil)
}

// ExpNot creates a "not" operator expression.
func ExpNot(exp *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpNOT,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{exp},
		arguments: nil,
	}
}

// ExpAnd creates a "and" (&&) operator that applies to a variable number of expressions.
func ExpAnd(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpAND,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpOr creates a "or" (||) operator that applies to a variable number of expressions.
func ExpOr(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpOR,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpExclusive creates an expression that returns true if only one of the expressions are true.
// Requires server version 5.6.0+.
func ExpExclusive(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpExclusive,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpEq creates a equal (==) expression.
func ExpEq(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpEQ,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpNotEq creates a not equal (!=) expression
func ExpNotEq(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpNE,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpGreater creates a greater than (>) operation.
func ExpGreater(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpGT,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpGreaterEq creates a greater than or equal (>=) operation.
func ExpGreaterEq(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpGE,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpLess creates a less than (<) operation.
func ExpLess(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpLT,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpLessEq creates a less than or equals (<=) operation.
func ExpLessEq(left *FilterExpression, right *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpLE,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{left, right},
		arguments: nil,
	}
}

// ExpNumAdd creates "add" (+) operator that applies to a variable number of expressions.
// Return sum of all `FilterExpressions` given. All arguments must resolve to the same type (integer or float).
// Requires server version 5.6.0+.
func ExpNumAdd(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpAdd,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpNumSub creates "subtract" (-) operator that applies to a variable number of expressions.
// If only one `FilterExpressions` is provided, return the negation of that argument.
// Otherwise, return the sum of the 2nd to Nth `FilterExpressions` subtracted from the 1st
// `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
// Requires server version 5.6.0+.
func ExpNumSub(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpSub,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpNumMul creates "multiply" (*) operator that applies to a variable number of expressions.
// Return the product of all `FilterExpressions`. If only one `FilterExpressions` is supplied, return
// that `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
// Requires server version 5.6.0+.
func ExpNumMul(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpMul,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpNumDiv creates "divide" (/) operator that applies to a variable number of expressions.
// If there is only one `FilterExpressions`, returns the reciprocal for that `FilterExpressions`.
// Otherwise, return the first `FilterExpressions` divided by the product of the rest.
// All `FilterExpressions` must resolve to the same type (integer or float).
// Requires server version 5.6.0+.
func ExpNumDiv(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpDiv,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpNumPow creates "power" operator that raises a "base" to the "exponent" power.
// All arguments must resolve to floats.
// Requires server version 5.6.0+.
func ExpNumPow(base *FilterExpression, exponent *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpPow,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{base, exponent},
		arguments: nil,
	}
}

// ExpNumLog creates "log" operator for logarithm of "num" with base "base".
// All arguments must resolve to floats.
// Requires server version 5.6.0+.
func ExpNumLog(num *FilterExpression, base *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpLog,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{num, base},
		arguments: nil,
	}
}

// ExpNumMod creates "modulo" (%) operator that determines the remainder of "numerator"
// divided by "denominator". All arguments must resolve to integers.
// Requires server version 5.6.0+.
func ExpNumMod(numerator *FilterExpression, denominator *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpMod,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{numerator, denominator},
		arguments: nil,
	}
}

// ExpNumAbs creates operator that returns absolute value of a number.
// All arguments must resolve to integer or float.
// Requires server version 5.6.0+.
func ExpNumAbs(value *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpAbs,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value},
		arguments: nil,
	}
}

// ExpNumFloor creates expression that rounds a floating point number down to the closest integer value.
// The return type is float.
// Requires server version 5.6.0+.
func ExpNumFloor(num *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpFloor,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{num},
		arguments: nil,
	}
}

// ExpNumCeil creates expression that rounds a floating point number up to the closest integer value.
// The return type is float.
// Requires server version 5.6.0+.
func ExpNumCeil(num *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpCeil,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{num},
		arguments: nil,
	}
}

// ExpToInt creates expression that converts an integer to a float.
// Requires server version 5.6.0+.
func ExpToInt(num *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpToInt,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{num},
		arguments: nil,
	}
}

// ExpToFloat creates expression that converts a float to an integer.
// Requires server version 5.6.0+.
func ExpToFloat(num *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpToFloat,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{num},
		arguments: nil,
	}
}

// ExpIntAnd creates integer "and" (&) operator that is applied to two or more integers.
// All arguments must resolve to integers.
// Requires server version 5.6.0+.
func ExpIntAnd(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntAnd,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpIntOr creates integer "or" (|) operator that is applied to two or more integers.
// All arguments must resolve to integers.
// Requires server version 5.6.0+.
func ExpIntOr(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntOr,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpIntXor creates integer "xor" (^) operator that is applied to two or more integers.
// All arguments must resolve to integers.
// Requires server version 5.6.0+.
func ExpIntXor(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntXor,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpIntNot creates integer "not" (~) operator.
// Requires server version 5.6.0+.
func ExpIntNot(exp *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntNot,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{exp},
		arguments: nil,
	}
}

// ExpIntLShift creates integer "left shift" (<<) operator.
// Requires server version 5.6.0+.
func ExpIntLShift(value *FilterExpression, shift *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntLShift,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value, shift},
		arguments: nil,
	}
}

// ExpIntRShift creates integer "logical right shift" (>>>) operator.
// Requires server version 5.6.0+.
func ExpIntRShift(value *FilterExpression, shift *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntRShift,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value, shift},
		arguments: nil,
	}
}

// ExpIntARShift creates integer "arithmetic right shift" (>>) operator.
// The sign bit is preserved and not shifted.
// Requires server version 5.6.0+.
func ExpIntARShift(value *FilterExpression, shift *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntARShift,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value, shift},
		arguments: nil,
	}
}

// ExpIntCount creates expression that returns count of integer bits that are set to 1.
// Requires server version 5.6.0+.
func ExpIntCount(exp *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntCount,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{exp},
		arguments: nil,
	}
}

// ExpIntLScan creates expression that scans integer bits from left (most significant bit) to
// right (least significant bit), looking for a search bit value. When the
// search value is found, the index of that bit (where the most significant bit is
// index 0) is returned. If "search" is true, the scan will search for the bit
// value 1. If "search" is false it will search for bit value 0.
// Requires server version 5.6.0+.
func ExpIntLScan(value *FilterExpression, search *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntLscan,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value, search},
		arguments: nil,
	}
}

// ExpIntRScan creates expression that scans integer bits from right (least significant bit) to
// left (most significant bit), looking for a search bit value. When the
// search value is found, the index of that bit (where the most significant bit is
// index 0) is returned. If "search" is true, the scan will search for the bit
// value 1. If "search" is false it will search for bit value 0.
// Requires server version 5.6.0+.
func ExpIntRScan(value *FilterExpression, search *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpIntRscan,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value, search},
		arguments: nil,
	}
}

// ExpMin creates expression that returns the minimum value in a variable number of expressions.
// All arguments must be the same type (integer or float).
// Requires server version 5.6.0+.
func ExpMin(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpMin,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpMax creates expression that returns the maximum value in a variable number of expressions.
// All arguments must be the same type (integer or float).
// Requires server version 5.6.0+.
func ExpMax(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpMax,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

//--------------------------------------------------
// Variables
//--------------------------------------------------

// ExpCond will conditionally select an expression from a variable number of expression pairs
// followed by default expression action.
// Requires server version 5.6.0+.
func ExpCond(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpCond,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpLet will define variables and expressions in scope.
// Requires server version 5.6.0+.
func ExpLet(exps ...*FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpLet,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      exps,
		arguments: nil,
	}
}

// ExpDef will assign variable to an expression that can be accessed later.
// Requires server version 5.6.0+.
func ExpDef(name string, value *FilterExpression) *FilterExpression {
	return &FilterExpression{
		cmd:       nil,
		val:       StringValue(name),
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      []*FilterExpression{value},
		arguments: nil,
	}
}

// ExpVar will retrieve expression value from a variable.
// Requires server version 5.6.0+.
func ExpVar(name string) *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpVar,
		val:       StringValue(name),
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      nil,
		arguments: nil,
	}
}

// ExpUnknown creates unknown value. Used to intentionally fail an expression.
// The failure can be ignored with `ExpWriteFlags` `EVAL_NO_FAIL`
// or `ExpReadFlags` `EVAL_NO_FAIL`.
// Requires server version 5.6.0+.
func ExpUnknown() *FilterExpression {
	return &FilterExpression{
		cmd:       &expOpUnknown,
		val:       nil,
		bin:       nil,
		flags:     nil,
		module:    nil,
		exps:      nil,
		arguments: nil,
	}
}

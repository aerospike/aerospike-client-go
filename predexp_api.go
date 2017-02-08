// Copyright 2017 Aerospike, Inc.
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

package aerospike

import "fmt"

type expression struct {
	predicate   predExp
	left, right *expression
}

func (e *expression) String() string {
	if e.left != nil && e.right != nil {
		return e.left.String() + " " + e.predicate.String() + " " + e.right.String()
	} else if e.left != nil {
		return e.left.String() + " " + e.predicate.String()
	} else if e.right != nil {
		return e.predicate.String() + " " + e.right.String()
	}
	return e.predicate.String()
}

func (e *expression) predicates() (res []predExp) {
	if e.right != nil {
		res = append(res, e.right.predicates()...)
	}
	if e.left != nil {
		res = append(res, e.left.predicates()...)
	}
	res = append(res, e.predicate)

	return res
}

func (e *expression) marshaledSize() int {
	sz := 0
	if e.left != nil {
		sz += e.left.marshaledSize()
	}
	if e.right != nil {
		sz += e.right.marshaledSize()
	}
	sz += e.predicate.marshaledSize()

	return sz
}

func (e *expression) marshal(cmd *baseCommand) error {
	if e.right != nil {
		if err := e.right.marshal(cmd); err != nil {
			return err
		}
	}
	if e.left != nil {
		if err := e.left.marshal(cmd); err != nil {
			return err
		}
	}
	if err := e.predicate.marshal(cmd); err != nil {
		return err
	}
	return nil
}

type boolExpression expression

// *NewPredExpAnd(nexpr uint16)
// *NewPredExpOr(nexpr uint16)
// *NewPredExpNot()

// *NewPredExpIntegerValue(val int64)
// *NewPredExpStringValue(val string)
// *NewPredExpGeoJSONValue(val string)

// NewPredExpUnknownBin(name string)
// NewPredExpIntegerBin(name string)
// NewPredExpStringBin(name string)
// NewPredExpGeoJSONBin(name string)

// *NewPredExpRecSize()
// *NewPredExpLastUpdate()
// *NewPredExpVoidTime()

// *NewPredExpIntegerEqual()
// *NewPredExpIntegerUnequal()
// *NewPredExpIntegerGreater()
// *NewPredExpIntegerGreaterEq()
// *NewPredExpIntegerLess()
// *NewPredExpIntegerLessEq()

// *NewPredExpStringEqual()
// *NewPredExpStringUnequal()
// *NewPredExpStringRegex(cflags uint32)

// *NewPredExpGeoJSONWithin()
// *NewPredExpGeoJSONContains()

func BinValue(name string) *expression {
	return &expression{predicate: NewPredExpUnknownBin(name)}
}

func RecExpiration() *expression {
	return &expression{predicate: NewPredExpVoidTime()}
}

func RecLastUpdate() *expression {
	return &expression{predicate: NewPredExpLastUpdate()}
}

func RecSize() *expression {
	return &expression{predicate: NewPredExpRecSize()}
}

func (e *expression) Regexp(val string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}

	exp := &boolExpression{predicate: NewPredExpStringRegex(0), left: e, right: &expression{predicate: NewPredExpStringValue(val)}}
	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_STRING_BIN)

	return exp
}

func (e *expression) GeoContains(point string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}
	exp := &boolExpression{predicate: NewPredExpGeoJSONContains(), left: e, right: &expression{predicate: NewPredExpGeoJSONValue(point)}}
	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_GEOJSON_BIN)

	return exp
}

func (e *expression) GeoWithin(region string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}
	exp := &boolExpression{predicate: NewPredExpGeoJSONWithin(), left: e, right: &expression{predicate: NewPredExpGeoJSONValue(region)}}
	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_GEOJSON_BIN)

	return exp
}

func (e *boolExpression) And(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpAnd(2), left: (*expression)(e), right: (*expression)(exp)}
}

func (e *boolExpression) Or(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpOr(2), left: (*expression)(e), right: (*expression)(exp)}
}

func (e *expression) NotEqual(val interface{}) *boolExpression {
	exp := e.Equal(val)

	p := exp.predicate.(*predExpCompare)
	if p.tag == _AS_PREDEXP_INTEGER_EQUAL {
		p.tag = _AS_PREDEXP_INTEGER_UNEQUAL
	} else if p.tag == _AS_PREDEXP_STRING_EQUAL {
		p.tag = _AS_PREDEXP_STRING_UNEQUAL
	}

	return exp
}

func Not_(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpNot(), left: nil, right: (*expression)(exp)}
}

func setBinType(predExp predExp, binType uint16) predExp {
	if p, ok := predExp.(*predExpBin); ok {
		if p.tag == _AS_PREDEXP_UNKNOWN_BIN {
			p.tag = binType
			return p
		}
	}
	return predExp
}

func (e *expression) Equal(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	case string:
		exp = &boolExpression{predicate: NewPredExpStringEqual(), left: e, right: &expression{predicate: NewPredExpStringValue(v)}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_STRING_BIN)
	case StringValue:
		exp = &boolExpression{predicate: NewPredExpStringEqual(), left: e, right: &expression{predicate: NewPredExpStringValue(string(v))}}
		exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_STRING_BIN)
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	return exp
}

func (e *expression) EqualOrLessThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		return &boolExpression{predicate: NewPredExpIntegerLessEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

func (e *expression) EqualOrGreaterThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

func (e *expression) GreaterThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

func (e *expression) LessThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		return &boolExpression{predicate: NewPredExpIntegerGreater(), left: e, right: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.left.predicate = setBinType(exp.left.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

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
	if e.right != nil && e.left != nil {
		return e.right.String() + " " + e.predicate.String() + " " + e.left.String()
	} else if e.right != nil {
		return e.right.String() + " " + e.predicate.String()
	} else if e.left != nil {
		return e.predicate.String() + " " + e.left.String()
	}
	return e.predicate.String()
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

// BinValue creates a predicate of type Bin
func BinValue(name string) *expression {
	return &expression{predicate: NewPredExpUnknownBin(name)}
}

// RecExpiration creates a predicate specifying the record's Expiration
func RecExpiration() *expression {
	return &expression{predicate: NewPredExpRecVoidTime()}
}

// RecLastUpdate creates a predicate specifying record's last update timestamp
func RecLastUpdate() *expression {
	return &expression{predicate: NewPredExpRecLastUpdate()}
}

// RecDeviceSize creates a predicate specifying record's size in bytes
func RecDeviceSize() *expression {
	return &expression{predicate: NewPredExpRecDeviceSize()}
}

// Regexp creates a predicate value specifying a regular expression to be evaluated against a bin's string value.
func (e *expression) Regexp(val string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}

	exp := &boolExpression{predicate: NewPredExpStringRegex(0), right: e, left: &expression{predicate: NewPredExpStringValue(val)}}
	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_STRING_BIN)

	return exp
}

// GeoContains creates a predicate value specifying regions containing a geojson point.
func (e *expression) GeoContains(point string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}
	exp := &boolExpression{predicate: NewPredExpGeoJSONContains(), right: e, left: &expression{predicate: NewPredExpGeoJSONValue(point)}}
	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_GEOJSON_BIN)

	return exp
}

// GeoWithin creates a predicate value specifying points or regions contained within a region.
func (e *expression) GeoWithin(region string) *boolExpression {
	if _, ok := e.predicate.(*predExpBin); !ok {
		panic("Only bin names can be used for regexp predicates.")
	}
	exp := &boolExpression{predicate: NewPredExpGeoJSONWithin(), right: e, left: &expression{predicate: NewPredExpGeoJSONValue(region)}}
	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_GEOJSON_BIN)

	return exp
}

// And creates an `and` predicate.
func (e *boolExpression) And(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpAnd(2), right: (*expression)(e), left: (*expression)(exp)}
}

// Or creates an `or` predicate.
func (e *boolExpression) Or(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpOr(2), right: (*expression)(e), left: (*expression)(exp)}
}

// NotEqual creates an `not equal` predicate.
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

// Not creates a `not` predicate.
func Not(exp *boolExpression) *boolExpression {
	return &boolExpression{predicate: NewPredExpNot(), right: nil, left: (*expression)(exp)}
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

// Equal creates a `=` predicate.
func (e *expression) Equal(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerEqual(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	case string:
		exp = &boolExpression{predicate: NewPredExpStringEqual(), right: e, left: &expression{predicate: NewPredExpStringValue(v)}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_STRING_BIN)
	case StringValue:
		exp = &boolExpression{predicate: NewPredExpStringEqual(), right: e, left: &expression{predicate: NewPredExpStringValue(string(v))}}
		exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_STRING_BIN)
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	return exp
}

// EqualOrLess creates a `<=` predicate.
func (e *expression) EqualOrLessThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerLessEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

// EqualOrGreaterThan creates a `>=` predicate.
func (e *expression) EqualOrGreaterThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreaterEq(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

// GreaterThan creates a `>` predicate.
func (e *expression) GreaterThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

// LessThan creates a `<` predicate.
func (e *expression) LessThan(val interface{}) *boolExpression {
	var exp *boolExpression

	switch v := val.(type) {
	case int8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case int64:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint8:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint16:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint32:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint64:
		panic("uint64 is not supported as a value in expressions")
	case int:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case uint:
		panic("uint is not supported as a value in expressions")
	case LongValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	case IntegerValue:
		exp = &boolExpression{predicate: NewPredExpIntegerGreater(), right: e, left: &expression{predicate: NewPredExpIntegerValue(int64(v))}}
	default:
		panic(fmt.Sprintf("type of value %#v is not supported as a value in expressions", v))
	}

	exp.right.predicate = setBinType(exp.right.predicate, _AS_PREDEXP_INTEGER_BIN)
	return exp
}

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

package aerospike

import (
	"fmt"
	"unicode/utf8"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// validateUTF8Bins checks every Bin value for valid UTF-8 in any string it
// carries (directly or nested in lists/maps). Returns nil on success, or a
// PARAMETER_ERROR identifying the first bin with an invalid string.
//
// Only invoked when ClientPolicy.ValidateUTF8 is true; callers must
// short-circuit otherwise.
func validateUTF8Bins(bins []*Bin) Error {
	for _, b := range bins {
		if b == nil {
			continue
		}
		if err := validateUTF8Value(b.Value, b.Name); err != nil {
			return err
		}
	}
	return nil
}

// validateUTF8Operations checks every Operation's binValue for valid UTF-8.
// Operations whose value carries pre-packed bytes (RawBlobValue, e.g. the
// new Str*Op builders) are not recursed into — their string args are not
// reachable here. Validating those is a separate concern and is not
// addressed in this pass.
func validateUTF8Operations(ops []*Operation) Error {
	for _, op := range ops {
		if op == nil || op.binValue == nil {
			continue
		}
		if err := validateUTF8Value(op.binValue, op.binName); err != nil {
			return err
		}
	}
	return nil
}

// validateUTF8Value walks a Value tree, returning a PARAMETER_ERROR if any
// StringValue or string-typed leaf contains invalid UTF-8. The error message
// references binName (an empty name is fine for operation values).
func validateUTF8Value(v Value, binName string) Error {
	switch val := v.(type) {
	case nil:
		return nil
	case StringValue:
		return checkUTF8String(string(val), binName)
	case GeoJSONValue:
		return checkUTF8String(string(val), binName)
	case ValueArray:
		for _, elem := range val {
			if err := validateUTF8Value(elem, binName); err != nil {
				return err
			}
		}
	case ListValue:
		for _, elem := range val {
			if err := validateUTF8Any(elem, binName); err != nil {
				return err
			}
		}
	case MapValue:
		for k, v2 := range val {
			if err := validateUTF8Any(k, binName); err != nil {
				return err
			}
			if err := validateUTF8Any(v2, binName); err != nil {
				return err
			}
		}
	case JsonValue:
		for k, v2 := range val {
			if err := checkUTF8String(k, binName); err != nil {
				return err
			}
			if err := validateUTF8Any(v2, binName); err != nil {
				return err
			}
		}
	}
	// Other Value subtypes (IntegerValue, LongValue, FloatValue, BytesValue,
	// HLLValue, RawBlobValue, BoolValue, NullValue, InfinityValue,
	// WildCardValue, MapperValue, ListerValue) either carry no string or
	// hold pre-packed/opaque bytes outside this validator's contract.
	return nil
}

// validateUTF8Any walks an `any` that originated from a user-facing ListValue
// or MapValue. The element may be a raw Go value (string, int, []any, etc.)
// or an already-wrapped Value.
func validateUTF8Any(a any, binName string) Error {
	switch x := a.(type) {
	case nil:
		return nil
	case string:
		return checkUTF8String(x, binName)
	case Value:
		return validateUTF8Value(x, binName)
	case []any:
		for _, elem := range x {
			if err := validateUTF8Any(elem, binName); err != nil {
				return err
			}
		}
	case map[string]any:
		for k, v := range x {
			if err := checkUTF8String(k, binName); err != nil {
				return err
			}
			if err := validateUTF8Any(v, binName); err != nil {
				return err
			}
		}
	case map[any]any:
		for k, v := range x {
			if err := validateUTF8Any(k, binName); err != nil {
				return err
			}
			if err := validateUTF8Any(v, binName); err != nil {
				return err
			}
		}
	case []string:
		for _, s := range x {
			if err := checkUTF8String(s, binName); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateUTF8BinMap walks every entry of a BinMap and reports the first
// invalid-UTF-8 string.
func validateUTF8BinMap(m BinMap) Error {
	for name, v := range m {
		if err := validateUTF8Any(v, name); err != nil {
			return err
		}
	}
	return nil
}

// utf8ValidationEnabled reports whether ValidateUTF8 is set on the active
// ClientPolicy. Safe to call on a nil Client (returns false).
func (clnt *Client) utf8ValidationEnabled() bool {
	if clnt == nil || clnt.cluster == nil {
		return false
	}
	p := clnt.cluster.clientPolicy.Load()
	return p != nil && p.ValidateUTF8
}

func checkUTF8String(s, binName string) Error {
	if utf8.ValidString(s) {
		return nil
	}
	msg := "non-UTF-8 string value"
	if binName != "" {
		msg = fmt.Sprintf("non-UTF-8 string value for bin %q", binName)
	}
	return newError(types.PARAMETER_ERROR, msg)
}

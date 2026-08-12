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
	"fmt"
)

// HLLConfig describes a HyperLogLog bin's precision.
//
// Index bits are 4..16; min-hash bits are 4..51 or -1 to disable, and the sum
// must not exceed 64. The server validates the ranges.
type HLLConfig struct {
	IndexBitCount   int64
	MinHashBitCount int64
}

// HLLConfigOf builds a configuration with min-hash disabled.
func HLLConfigOf(indexBits int64) HLLConfig {
	return HLLConfig{IndexBitCount: indexBits, MinHashBitCount: -1}
}

// HLLConfigWithMinHash builds a configuration with both bit counts set.
func HLLConfigWithMinHash(indexBits, minHashBits int64) HLLConfig {
	return HLLConfig{IndexBitCount: indexBits, MinHashBitCount: minHashBits}
}

// OperationResult wraps a single value with typed, non-silent accessors.
//
// Numeric and boolean accessors report the zero value for a nil result;
// reference accessors report absence. Any other type mismatch is an error
// rather than a silent zero.
type OperationResult struct {
	value any
}

// NewOperationResult wraps a value.
func NewOperationResult(v any) *OperationResult { return &OperationResult{value: v} }

// Value returns the raw value.
func (o *OperationResult) Value() any { return o.value }

// mismatch builds the standard type-mismatch error.
func (o *OperationResult) mismatch(want string) error {
	return NewError(KindInvalidArgument,
		"operation result is %T (%v), not %s", o.value, o.value, want)
}

// GetInt reports an integer result. A nil result is 0; a boolean is 0 or 1.
func (o *OperationResult) GetInt() (int64, error) {
	switch t := o.value.(type) {
	case nil:
		return 0, nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	}
	return 0, o.mismatch("an integer")
}

// GetFloat reports a floating-point result. A nil result is 0.
func (o *OperationResult) GetFloat() (float64, error) {
	switch t := o.value.(type) {
	case nil:
		return 0, nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	}
	return 0, o.mismatch("a float")
}

// GetBool reports a boolean result. A nil result is false; an integer is
// compared against zero, for compatibility with clients that stored booleans
// as longs.
func (o *OperationResult) GetBool() (bool, error) {
	switch t := o.value.(type) {
	case nil:
		return false, nil
	case bool:
		return t, nil
	case int:
		return t != 0, nil
	case int64:
		return t != 0, nil
	}
	return false, o.mismatch("a boolean")
}

// GetString reports a string result, or absence for a nil result.
func (o *OperationResult) GetString() (string, bool, error) {
	switch t := o.value.(type) {
	case nil:
		return "", false, nil
	case string:
		return t, true, nil
	}
	return "", false, o.mismatch("a string")
}

// GetBytes reports a blob result, or absence for a nil result.
func (o *OperationResult) GetBytes() ([]byte, bool, error) {
	switch t := o.value.(type) {
	case nil:
		return nil, false, nil
	case []byte:
		return t, true, nil
	}
	return nil, false, o.mismatch("a blob")
}

// GetList reports a list result, or absence for a nil result.
func (o *OperationResult) GetList() ([]any, bool, error) {
	switch t := o.value.(type) {
	case nil:
		return nil, false, nil
	case []any:
		return t, true, nil
	}
	return nil, false, o.mismatch("a list")
}

// GetMap reports a map result, or absence for a nil result.
func (o *OperationResult) GetMap() (map[any]any, bool, error) {
	switch t := o.value.(type) {
	case nil:
		return nil, false, nil
	case map[any]any:
		return t, true, nil
	case map[string]any:
		out := make(map[any]any, len(t))
		for k, v := range t {
			out[k] = v
		}
		return out, true, nil
	}
	return nil, false, o.mismatch("a map")
}

// String implements fmt.Stringer.
func (o *OperationResult) String() string { return fmt.Sprintf("%v", o.value) }

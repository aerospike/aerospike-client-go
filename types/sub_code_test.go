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

package types_test

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// SubCode.String renders the number (subcodes are not globally unique, so a
// value-only Stringer cannot resolve a name); SubCodeNone renders as "none".
func TestSubCodeString(t *testing.T) {
	cases := []struct {
		sc   types.SubCode
		want string
	}{
		{types.SubCodeNone, "none"},
		{types.SubCodeParamBitsResizeExceeded, "4"},
		{types.SubCode(5001), "5001"},
	}
	for _, c := range cases {
		if got := c.sc.String(); got != c.want {
			t.Errorf("SubCode(%d).String() = %q, want %q", int(c.sc), got, c.want)
		}
	}
}

// SubCodeToString resolves the symbolic name only with the parent ResultCode,
// because the same subcode value names different failures under different
// statuses. It falls back to a numeric rendering for undeclared pairs.
func TestSubCodeToString(t *testing.T) {
	cases := []struct {
		name string
		rc   types.ResultCode
		sc   types.SubCode
		want string
	}{
		{"none is universal", types.OK, types.SubCodeNone, "SubCodeNone"},
		{"none ignores result code", types.PARAMETER_ERROR, types.SubCodeNone, "SubCodeNone"},
		{"param pair", types.PARAMETER_ERROR, types.SubCodeParamBitsResizeExceeded, "SubCodeParamBitsResizeExceeded"},
		{"op pair", types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLIndexBitsUnset, "SubCodeOpNotHLLIndexBitsUnset"},
		{"mrt pair", types.MRT_BLOCKED, types.SubCodeMRTBlockedIDMismatch, "SubCodeMRTBlockedIDMismatch"},
		// The crux: value 1 disambiguates on the parent result code.
		{"value 1 under param", types.PARAMETER_ERROR, 1, "SubCodeParamTTLInvalid"},
		{"value 1 under op", types.OP_NOT_APPLICABLE, 1, "SubCodeOpNotCDTIndexOutOfBounds"},
		{"value 1 under forbidden", types.FAIL_FORBIDDEN, 1, "SubCodeForbidXDRFilterBlocked"},
		// Undeclared pairs fall back rather than returning "".
		{"unknown subcode under known rc", types.PARAMETER_ERROR, 999, "SubCode(4/999)"},
		{"result code that carries no subcodes", types.OK, 1, "SubCode(0/1)"},
	}
	for _, c := range cases {
		if got := types.SubCodeToString(c.rc, c.sc); got != c.want {
			t.Errorf("%s: SubCodeToString(%d, %d) = %q, want %q", c.name, int(c.rc), int(c.sc), got, c.want)
		}
	}
}

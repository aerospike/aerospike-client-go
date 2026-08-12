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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// expOpAEL is the opcode that introduces Aerospike Expression Language source
// text. The server, not the client, compiles the text that follows.
const expOpAEL int64 = 128

// ExpAEL creates a filter expression from Aerospike Expression Language source
// text.
//
// Unlike every other expression constructor, the client does not compile the
// filter: it packs the source and the server parses and compiles it. That
// requires server 8.1.3 or newer on the node that runs the command; older
// servers reject the expression. Syntax errors therefore surface as a server
// error at execution time rather than at construction.
//
// Use the typed constructors (ExpEq, ExpGreater, and so on) when the filter is
// written in code and must work on older servers; use ExpAEL when the filter
// arrives as text, from configuration or user input.
//
//	stmt := as.NewStatement("test", "users")
//	policy := as.NewQueryPolicy()
//	policy.FilterExpression = as.ExpAEL(`$.age >= 25 and $.status == "active"`)
//
// An empty source string yields an expression that the server rejects; the
// caller is expected to validate that the text is non-empty.
func ExpAEL(source string) *Expression {
	// The wire form is the two-element array [128, "<source>"]. Pre-packing it
	// into the raw byte form lets the expression travel through the normal
	// filter-expression path without a dedicated command opcode.
	packer := newPacker()
	if _, err := packArrayBegin(packer, 2); err != nil {
		panic(newError(types.SERIALIZE_ERROR, "cannot pack AEL expression header: "+err.Error()))
	}
	if _, err := packAInt64(packer, expOpAEL); err != nil {
		panic(newError(types.SERIALIZE_ERROR, "cannot pack AEL expression opcode: "+err.Error()))
	}
	// A raw string: the server expects plain MessagePack UTF-8 here, without
	// the particle-type byte that packString prefixes onto bin values.
	if _, err := packRawString(packer, source); err != nil {
		panic(newError(types.SERIALIZE_ERROR, "cannot pack AEL expression source: "+err.Error()))
	}
	return &Expression{bytes: packer.Bytes()}
}

// IsAEL reports whether the expression carries Aerospike Expression Language
// source text rather than a client-compiled expression tree.
//
// Clients use this to gate on server support before sending: an AEL expression
// needs server 8.1.3 or newer.
func (fe *Expression) IsAEL() bool {
	if fe == nil || len(fe.bytes) == 0 {
		return false
	}
	// The packed form begins with a two-element array header followed by the
	// AEL opcode. A one-byte fixarray header of length 2 is 0x92.
	const fixArray2 = 0x92
	if fe.bytes[0] != fixArray2 || len(fe.bytes) < 2 {
		return false
	}
	// 128 does not fit a positive fixint, so it is packed as uint8 (0xcc)
	// followed by the value.
	const uint8Marker = 0xcc
	return fe.bytes[1] == uint8Marker && len(fe.bytes) > 2 && fe.bytes[2] == byte(expOpAEL)
}

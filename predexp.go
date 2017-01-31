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

import (
)

const (
	AS_PREDEXP_AND					uint16 = 1
	AS_PREDEXP_OR					uint16 = 2
	AS_PREDEXP_NOT					uint16 = 3

	AS_PREDEXP_INTEGER_VALUE		uint16 = 10
	AS_PREDEXP_STRING_VALUE			uint16 = 11
	AS_PREDEXP_GEOJSON_VALUE		uint16 = 12

	AS_PREDEXP_INTEGER_BIN			uint16 = 100
	AS_PREDEXP_STRING_BIN			uint16 = 101
	AS_PREDEXP_GEOJSON_BIN			uint16 = 102

	AS_PREDEXP_RECSIZE				uint16 = 150
	AS_PREDEXP_LAST_UPDATE			uint16 = 151
	AS_PREDEXP_VOID_TIME			uint16 = 152

	AS_PREDEXP_INTEGER_EQUAL		uint16 = 200
	AS_PREDEXP_INTEGER_UNEQUAL		uint16 = 201
	AS_PREDEXP_INTEGER_GREATER		uint16 = 202
	AS_PREDEXP_INTEGER_GREATEREQ	uint16 = 203
	AS_PREDEXP_INTEGER_LESS			uint16 = 204
	AS_PREDEXP_INTEGER_LESSEQ		uint16 = 205

	AS_PREDEXP_STRING_EQUAL			uint16 = 210
	AS_PREDEXP_STRING_UNEQUAL		uint16 = 211
	AS_PREDEXP_STRING_REGEX			uint16 = 212

	AS_PREDEXP_GEOJSON_WITHIN		uint16 = 220
	AS_PREDEXP_GEOJSON_CONTAINS		uint16 = 221
)	

// ----------------

type PredExp interface {
	marshaledSize() int
	marshal(cmd *baseCommand) error
}

type predExpBase struct {
}

func (self *predExpBase) marshaledSize() int {
	return 2 + 4	// sizeof(TAG) + sizeof(LEN)
}

func (self *predExpBase) marshalTL(cmd *baseCommand, tag uint16, len uint32) {
	cmd.WriteUint16(tag)
	cmd.WriteUint32(len)
}

// ---------------- predExpAnd

type predExpAnd struct {
	predExpBase
	nexpr uint16	// number of child expressions
}

func NewPredExpAnd(nexpr uint16) *predExpAnd {
	return &predExpAnd{ nexpr: nexpr }
}

func (self *predExpAnd) marshaledSize() int {
	return self.predExpBase.marshaledSize() + 2
}

func (self *predExpAnd) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_AND, 2)
	cmd.WriteUint16(self.nexpr)
	return nil
}

// ---------------- predExpOr

type predExpOr struct {
	predExpBase
	nexpr uint16	// number of child expressions
}

func NewPredExpOr(nexpr uint16) *predExpOr {
	return &predExpOr{ nexpr: nexpr }
}

func (self *predExpOr) marshaledSize() int {
	return self.predExpBase.marshaledSize() + 2
}

func (self *predExpOr) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_OR, 2)
	cmd.WriteUint16(self.nexpr)
	return nil
}

// ---------------- predExpNot

type predExpNot struct {
	predExpBase
}

func NewPredExpNot() *predExpNot {
	return &predExpNot{ }
}

func (self *predExpNot) marshaledSize() int {
	return self.predExpBase.marshaledSize()
}

func (self *predExpNot) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_NOT, 0)
	return nil
}

// ---------------- predExpIntegerValue

type predExpIntegerValue struct {
	predExpBase
	val int64
}

func NewPredExpIntegerValue(val int64) *predExpIntegerValue {
	return &predExpIntegerValue{ val: val }
}

func (self *predExpIntegerValue) marshaledSize() int {
	return self.predExpBase.marshaledSize() + 8
}

func (self *predExpIntegerValue) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_INTEGER_VALUE, 8)
	cmd.WriteInt64(self.val)
	return nil
}

// ---------------- predExpStringValue

type predExpStringValue struct {
	predExpBase
	val string
}

func NewPredExpStringValue(val string) *predExpStringValue {
	return &predExpStringValue{ val: val }
}

func (self *predExpStringValue) marshaledSize() int {
	return self.predExpBase.marshaledSize() + len(self.val)
}

func (self *predExpStringValue) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_STRING_VALUE, uint32(len(self.val)))
	cmd.WriteString(self.val)
	return nil
}

// ---------------- predExpGeoJSONValue

type predExpGeoJSONValue struct {
	predExpBase
	val string
}

func NewPredExpGeoJSONValue(val string) *predExpGeoJSONValue {
	return &predExpGeoJSONValue{ val: val }
}

func (self *predExpGeoJSONValue) marshaledSize() int {
	return self.predExpBase.marshaledSize() +
		1 +				// flags
		2 + 			// ncells
		len(self.val)	// strlen value
}

func (self *predExpGeoJSONValue) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_GEOJSON_VALUE, uint32(1 + 2 + len(self.val)))
	cmd.WriteByte(uint8(0))
	cmd.WriteUint16(0)
	cmd.WriteString(self.val)
	return nil
}

// ---------------- predExp???Bin

type predExpBin struct {
	predExpBase
	name string
	tag uint16	// not marshaled
}

func NewPredExpIntegerBin(name string) *predExpBin {
	return &predExpBin{ name: name, tag: AS_PREDEXP_INTEGER_BIN, }
}

func NewPredExpStringBin(name string) *predExpBin {
	return &predExpBin{ name: name, tag: AS_PREDEXP_STRING_BIN, }
}

func NewPredExpGeoJSONBin(name string) *predExpBin {
	return &predExpBin{ name: name, tag: AS_PREDEXP_GEOJSON_BIN, }
}

func (self *predExpBin) marshaledSize() int {
	return self.predExpBase.marshaledSize() + 1 + len(self.name)
}

func (self *predExpBin) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, self.tag, uint32(1 + len(self.name)))
	cmd.WriteByte(uint8(len(self.name)))
	cmd.WriteString(self.name)
	return nil
}

// ---------------- predExpMD (RecSize, LastUpdate, VoidTime)

type predExpMD struct {
	predExpBase
	tag uint16	// not marshaled
}

func (self *predExpMD) marshaledSize() int {
	return self.predExpBase.marshaledSize()
}

func (self *predExpMD) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, self.tag, 0)
	return nil
}

func NewPredExpRecSize() *predExpMD {
	return &predExpMD{ tag: AS_PREDEXP_RECSIZE }
}

func NewPredExpLastUpdate() *predExpMD {
	return &predExpMD{ tag: AS_PREDEXP_LAST_UPDATE }
}

func NewPredExpVoidTime() *predExpMD {
	return &predExpMD{ tag: AS_PREDEXP_VOID_TIME }
}

// ---------------- predExpCompare 

type predExpCompare struct {
	predExpBase
	tag uint16	// not marshaled
}

func (self *predExpCompare) marshaledSize() int {
	return self.predExpBase.marshaledSize()
}

func (self *predExpCompare) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, self.tag, 0)
	return nil
}

func NewPredExpIntegerEqual() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_EQUAL }
}

func NewPredExpIntegerUnequal() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_UNEQUAL }
}

func NewPredExpIntegerGreater() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_GREATER }
}

func NewPredExpIntegerGreaterEq() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_GREATEREQ }
}

func NewPredExpIntegerLess() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_LESS }
}

func NewPredExpIntegerLessEq() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_INTEGER_LESSEQ }
}

func NewPredExpStringEqual() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_STRING_EQUAL }
}

func NewPredExpStringUnequal() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_STRING_UNEQUAL }
}

func NewPredExpGeoJSONWithin() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_GEOJSON_WITHIN }
}

func NewPredExpGeoJSONContains() *predExpCompare {
	return &predExpCompare{ tag: AS_PREDEXP_GEOJSON_CONTAINS }
}

// ---------------- predExpStringRegex

type predExpStringRegex struct {
	predExpBase
	cflags uint32		// cflags
}

func NewPredExpStringRegex(cflags uint32) *predExpStringRegex {
	return &predExpStringRegex{ cflags: cflags }
}

func (self *predExpStringRegex) marshaledSize() int {
	return self.predExpBase.marshaledSize() + 4
}

func (self *predExpStringRegex) marshal(cmd *baseCommand) error {
	self.marshalTL(cmd, AS_PREDEXP_STRING_REGEX, 4)
	cmd.WriteUint32(self.cflags)
	return nil
}

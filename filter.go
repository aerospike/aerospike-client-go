// Copyright 2013-2014 Aerospike, Inc.
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
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

/**
 * Query filter definition.
 */
type Filter struct {
	name  string
	begin Value
	end   Value
}

/**
 * Create long equality filter for query.
 *
 * @param name      bin name
 * @param value     filter value
 * @return        filter instance
 */
func NewEqualFilter(name string, value interface{}) *Filter {
	val := NewValue(value)
	return NewFilter(name, val, val)
}

/**
 * Create range filter for query.
 * Range arguments must be longs or integers which can be cast to longs.
 * String ranges are not supported.
 *
 * @param name      bin name
 * @param begin     filter begin value
 * @param end     filter end value
 * @return        filter instance
 */
func NewRangeFilter(name string, begin int64, end int64) *Filter {
	return NewFilter(name, NewValue(begin), NewValue(end))
}

func NewFilter(name string, begin Value, end Value) *Filter {
	return &Filter{
		name:  name,
		begin: begin,
		end:   end,
	}
}

func (this *Filter) estimateSize() (int, error) {
	// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
	return len(this.name) + this.begin.EstimateSize() + this.end.EstimateSize() + 10, nil
}

func (this *Filter) write(buf []byte, offset int) (int, error) {
	var err error

	// Write name.
	len := copy(buf[offset+1:], []byte(this.name))
	buf[offset] = byte(len)
	offset += len + 1

	// Write particle type.
	buf[offset] = byte(this.begin.GetType())
	offset++

	// Write filter begin.
	len, err = this.begin.Write(buf, offset+4)
	if err != nil {
		return -1, err
	}
	Buffer.Int32ToBytes(int32(len), buf, offset)
	offset += len + 4

	// Write filter end.
	len, err = this.end.Write(buf, offset+4)
	if err != nil {
		return -1, err
	}
	Buffer.Int32ToBytes(int32(len), buf, offset)
	offset += len + 4

	return offset, nil
}

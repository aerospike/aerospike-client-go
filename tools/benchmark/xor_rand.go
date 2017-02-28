// Copyright 2013-2016 Aerospike, Inc.
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

package main

import (
	"encoding/binary"
	"time"
)

type XorRand struct {
	src [2]uint64
}

func NewXorRand() *XorRand {
	return &XorRand{[2]uint64{uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())}}
}

func (r *XorRand) Int64() int64 {
	return int64(r.Uint64())
}

func (r *XorRand) Uint64() uint64 {
	s1 := r.src[0]
	s0 := r.src[1]
	r.src[0] = s0
	s1 ^= s1 << 23
	r.src[1] = (s1 ^ s0 ^ (s1 >> 17) ^ (s0 >> 26))
	return r.src[1] + s0
}

func (r *XorRand) Read(p []byte) (n int, err error) {
	l := len(p) / 8
	for i := 0; i < l; i += 8 {
		binary.PutUvarint(p[i:], r.Uint64())
	}
	return len(p), nil
}

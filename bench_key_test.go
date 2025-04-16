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
// limitations under the License.

package aerospike_test

import (
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	ParticleType "github.com/aerospike/aerospike-client-go/internal/particle_type"
	"github.com/aerospike/aerospike-client-go/pkg/ripemd160"
)

var str = strings.Repeat("abcd", 128)
var strVal = as.NewValue(str)
var buffer = []byte(str)
var key *as.Key

var res []byte

// func hash_key_baseline(str string) {
// 	hash := ripemd160.New()
// 	for i := 0; i < b.N; i++ {
// 		hash.Reset()
// 		hash.Write(buffer)
// 		res = hash.Sum(nil)
// 	}
// }

func Benchmark_Key_Hash_BaseLine(b *testing.B) {
	hash := ripemd160.New()
	for i := 0; i < b.N; i++ {
		hash.Reset()
		hash.Write(buffer)
		res = hash.Sum(nil)
	}
}

func Benchmark_Key_NewKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, _ = as.NewKey(str, str, str)
		res = key.Digest()
	}
}

func Benchmark_K_ComputeDigest_Raw(b *testing.B) {
	h := ripemd160.New()
	setName := []byte(str)
	keyType := []byte{byte(ParticleType.STRING)}
	keyVal := []byte(str)
	for i := 0; i < b.N; i++ {
		h.Reset()

		// write will not fail; no error checking necessary
		h.Write(setName)
		h.Write(keyType)
		h.Write(keyVal)

		res = h.Sum(nil)
	}
}

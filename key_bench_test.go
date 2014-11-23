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
	"strings"
	"testing"

	"github.com/aerospike/aerospike-client-go/pkg/ripemd160"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"

	// . "github.com/aerospike/aerospike-client-go"
)

var str = strings.Repeat("abcd", 128)
var strVal = NewValue(str)
var buffer = []byte(str)
var key *Key

var res []byte

func Benchmark_K_Hash(b *testing.B) {
	hash := ripemd160.New()
	for i := 0; i < b.N; i++ {
		hash.Reset()
		hash.Write(buffer)
		res = hash.Sum(nil)
	}
}

func Benchmark_K_Key(b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, _ = NewKey(str, str, str)
		// res = key.Digest()
	}
}

// func Benchmark_Key_New(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		key, _ = NewKeyNew(str, str, str)
// 		res = key.Digest()
// 	}
// }

// func Benchmark_K_ComputeDigest_Orig(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		res, _ = computeDigest(str, strVal)
// 	}
// }

// func Benchmark_K_ComputeDigest_Verify(b *testing.B) {
// 	var res1, res2 []byte
// 	var key *Key
// 	for i := 0; i < b.N; i++ {
// 		key, _ = NewKey(str, str, i)
// 		res1, _ = computeDigest(key)
// 		res2, _ = computeDigestNew(key)

// 		if !bytes.Equal(res1, res2) {
// 			panic("Oh oh!")
// 		}

// 		key, _ = NewKey(str, str, fmt.Sprintf("%s%d", str, i))
// 		res1, _ = computeDigest(key)
// 		res2, _ = computeDigestNew(key)

// 		if !bytes.Equal(res1, res2) {
// 			panic(fmt.Sprintf("Oh oh!\n%v\n%v", res1, res2))
// 		}
// 	}
// }

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

// func Benchmark_ComputeDigest_New(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		res, _ = ComputeDigestNew(str, strVal)
// 	}
// }

// func Test_Compare_ComputeDigests(t *testing.T) {
// 	for i := 0; i < 100000; i++ {
// 		str := randString(rand.Intn(100))
// 		// v := str
// 		// v := rand.Int()
// 		// v := rand.Int63()
// 		// v := []byte{}
// 		// v := []byte{17, 191, 241}
// 		// v := []int{17, 191, 241}
// 		// v := []interface{}{17, "191", nil}
// 		v := []Value{NewValue(17), NewValue("191"), NewValue(nil)}
// 		val := NewValue(v)

// 		a, _ := ComputeDigest(str, val)
// 		b, _ := ComputeDigestNew(str, val)
// 		if !bytes.Equal(a, b) {
// 			panic("Digests are not the same")
// 		}
// 	}
// }

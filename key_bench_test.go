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

package aerospike

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/aerospike/aerospike-client-go/pkg/ripemd160"
)

var res []byte

func doTheHash(buf []byte, b *testing.B) {
	hash := ripemd160.New()
	for i := 0; i < b.N; i++ {
		hash.Reset()
		hash.Write(buf)
		res = hash.Sum(nil)
	}
}

func Benchmark_K_Hash_S_______1(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 1))
	doTheHash(buffer, b)
}

func Benchmark_K_Hash_S______10(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 10))
	doTheHash(buffer, b)
}

func Benchmark_K_Hash_S_____100(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 100))
	doTheHash(buffer, b)
}

func Benchmark_K_Hash_S____1000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 1000))
	doTheHash(buffer, b)
}

func Benchmark_K_Hash_S__10_000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 10000))
	doTheHash(buffer, b)
}

func Benchmark_K_Hash_S_100_000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 100000))
	doTheHash(buffer, b)
}

func makeKeys(val interface{}, b *testing.B) {
	for i := 0; i < b.N; i++ {
		key, _ := NewKey("ns", "set", val)
		res = key.Digest()
	}
}

func Benchmark_NewKey_String______1(b *testing.B) {
	buffer := strings.Repeat("s", 1)
	makeKeys(buffer, b)
}

func Benchmark_NewKey_String_____10(b *testing.B) {
	buffer := strings.Repeat("s", 10)
	makeKeys(buffer, b)
}

func Benchmark_NewKey_String____100(b *testing.B) {
	buffer := strings.Repeat("s", 100)
	makeKeys(buffer, b)
}

func Benchmark_NewKey_String___1000(b *testing.B) {
	buffer := strings.Repeat("s", 1000)
	makeKeys(buffer, b)
}

func Benchmark_NewKey_String__10000(b *testing.B) {
	buffer := strings.Repeat("s", 10000)
	makeKeys(buffer, b)
}

func Benchmark_NewKey_String_100000(b *testing.B) {
	buffer := strings.Repeat("s", 100000)
	makeKeys(buffer, b)
}
func Benchmark_NewKey_Byte______1(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 1))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_Byte_____10(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 10))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_Byte____100(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 100))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_Byte___1000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 1000))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_Byte__10000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 10000))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_Byte_100000(b *testing.B) {
	buffer := []byte(strings.Repeat("s", 100000))
	makeKeys(buffer, b)
}

func Benchmark_NewKey_________Int(b *testing.B) {
	makeKeys(rand.Int63(), b)
}

func Benchmark_NewKey_List_No_Reflect(b *testing.B) {
	list := []interface{}{
		strings.Repeat("s", 1),
		strings.Repeat("s", 10),
		strings.Repeat("s", 100),
		strings.Repeat("s", 1000),
		strings.Repeat("s", 10000),
		strings.Repeat("s", 100000),
	}
	makeKeys(list, b)
}

func Benchmark_NewKey_List_With_Reflect(b *testing.B) {
	list := []string{
		strings.Repeat("s", 1),
		strings.Repeat("s", 10),
		strings.Repeat("s", 100),
		strings.Repeat("s", 1000),
		strings.Repeat("s", 10000),
		strings.Repeat("s", 100000),
	}
	makeKeys(list, b)
}

func Benchmark_NewKey_Map_No_Reflect(b *testing.B) {
	theMap := map[interface{}]interface{}{
		1: strings.Repeat("s", 1),
		2: strings.Repeat("s", 10),
		3: strings.Repeat("s", 100),
		4: strings.Repeat("s", 1000),
		5: strings.Repeat("s", 10000),
		6: strings.Repeat("s", 100000),
		7: rand.Int63(),
	}
	makeKeys(theMap, b)
}

func Benchmark_NewKey_Map_With_Reflect(b *testing.B) {
	theMap := map[int]interface{}{
		1: strings.Repeat("s", 1),
		2: strings.Repeat("s", 10),
		3: strings.Repeat("s", 100),
		4: strings.Repeat("s", 1000),
		5: strings.Repeat("s", 10000),
		6: strings.Repeat("s", 100000),
		7: rand.Int63(),
	}
	makeKeys(theMap, b)
}

var _k, _v interface{}

func Benchmark_Map_Native_Iterate(b *testing.B) {
	theMap := map[interface{}]interface{}{
		1: strings.Repeat("s", 1),
		7: rand.Int63(),
	}

	for i := 0; i < b.N; i++ {
		for k, v := range theMap {
			_k, _v = k, v
		}
	}
}

func Benchmark_Map_Reflect_Iterate(b *testing.B) {
	theMap := map[interface{}]interface{}{
		1: strings.Repeat("s", 1),
		7: rand.Int63(),
	}

	var interfaceMap interface{} = theMap

	for i := 0; i < b.N; i++ {
		s := reflect.ValueOf(interfaceMap)
		for _, k := range s.MapKeys() {
			_k, _v = k.Interface(), s.MapIndex(k).Interface()
		}
	}
}

// func Benchmark_Key_New(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		key, _ = NewKeyNew(str, str, str)
// 		res = key.Digest()
// 	}
// }

// func Benchmark_K_ComputeDigest_Orig(b *testing.B) {
// 	key, _ := NewKey(str, str, str)
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		res, _ = computeDigest(key)
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

// func Benchmark_K_ComputeDigest_Raw(b *testing.B) {
// 	h := ripemd160.New()
// 	setName := []byte(str)
// 	keyType := []byte{byte(ParticleType.STRING)}
// 	keyVal := []byte(str)
// 	for i := 0; i < b.N; i++ {
// 		h.Reset()

// 		// write will not fail; no error checking necessary
// 		h.Write(setName)
// 		h.Write(keyType)
// 		h.Write(keyVal)

// 		res = h.Sum(nil)
// 	}
// }

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

//go:build !as_performance

// Copyright 2014-2022 Aerospike, Inc.
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
	"runtime"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// reflectPerson is the reflection-based mirror of binSerDerPerson;
// it has the same bin layout and is used to drive Benchmark_GetObject_Reflect.
type reflectPerson struct {
	TTL uint32 `asm:"ttl"`
	Gen uint32 `asm:"gen"`
	Name string `as:"name"`
	Age  int    `as:"age"`
	Bio  string `as:"bio"`
}

func setupBinSerDerKey(b *testing.B) (*as.Client, *as.Key) {
	c, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	key, _ := as.NewKey(*namespace, "binserder_bench", "person")
	if err := c.PutBins(nil, key,
		as.NewBin("name", "Ada"),
		as.NewBin("age", 37),
		as.NewBin("bio", "analyst"),
	); err != nil {
		b.Fatalf("put: %v", err)
	}
	return c, key
}

func Benchmark_GetObject_Reflect(b *testing.B) {
	c, key := setupBinSerDerKey(b)
	obj := &reflectPerson{}
	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.GetObject(nil, key, obj); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func Benchmark_GetObject_BinSerDer(b *testing.B) {
	c, key := setupBinSerDerKey(b)
	obj := &binSerDerPerson{}
	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.GetObjectBinSerDer(nil, key, obj); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

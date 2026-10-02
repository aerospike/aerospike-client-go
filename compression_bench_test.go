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

package aerospike_test

import (
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Round trips under a UseCompression policy with a request small enough to stay
// under the 128 byte compression threshold: the command keeps a second
// reference to the connection buffer for the whole call on this path. The
// uncompressed round trips at the bottom are the control for the same payload.

func Benchmark_Compression_Put_SmallRequest(b *testing.B) {
	if !isEnterpriseEdition() {
		b.Skip("requires Enterprise Edition compression support")
	}

	c, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key, err := as.NewKey(*namespace, "bench_compression", "small")
	if err != nil {
		b.Fatal(err)
	}

	wp := as.NewWritePolicy(0, 0)
	wp.UseCompression = true
	bin := as.NewBin("b", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.PutBins(wp, key, bin); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Compression_Get_SmallRequest(b *testing.B) {
	if !isEnterpriseEdition() {
		b.Skip("requires Enterprise Edition compression support")
	}

	c, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key, err := as.NewKey(*namespace, "bench_compression", "small")
	if err != nil {
		b.Fatal(err)
	}

	wp := as.NewWritePolicy(0, 0)
	wp.UseCompression = true
	if err := c.PutBins(wp, key, as.NewBin("b", 1)); err != nil {
		b.Fatal(err)
	}

	rp := as.NewPolicy()
	rp.UseCompression = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(rp, key); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_NoCompression_Put_SmallRequest(b *testing.B) {
	c, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key, err := as.NewKey(*namespace, "bench_compression", "small")
	if err != nil {
		b.Fatal(err)
	}

	wp := as.NewWritePolicy(0, 0)
	bin := as.NewBin("b", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.PutBins(wp, key, bin); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_NoCompression_Get_SmallRequest(b *testing.B) {
	c, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key, err := as.NewKey(*namespace, "bench_compression", "small")
	if err != nil {
		b.Fatal(err)
	}

	if err := c.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin("b", 1)); err != nil {
		b.Fatal(err)
	}

	rp := as.NewPolicy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(rp, key); err != nil {
			b.Fatal(err)
		}
	}
}

//go:build go1.27

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

package sdk_test

import (
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("SDK fast path", func() {
	gg.It("must round-trip a record", func() {
		s, ds := newSession()
		key := ds.Key("user-1")

		gm.Expect(s.Put(key, as.BinMap{"name": "Ada", "age": 36})).ToNot(gm.HaveOccurred())

		rec, err := s.Get(key, sdk.AllBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["name"]).To(gm.Equal("Ada"))
	})

	gg.It("must honor a projection and a header-only read", func() {
		s, ds := newSession()
		key := ds.Key("user-2")
		gm.Expect(s.Put(key, as.BinMap{"name": "Bob", "age": 41})).ToNot(gm.HaveOccurred())

		proj, err := s.Get(key, []string{"name"})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(proj.Bins).ToNot(gm.HaveKey("age"))

		// A header read must not carry the user key, which the server rejects.
		header, err := s.Get(key, sdk.NoBins)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(header.Bins).To(gm.BeEmpty())
		gm.Expect(header.Generation).To(gm.BeNumerically(">", 0))
	})

	gg.It("must report a missing record as an error", func() {
		s, ds := newSession()
		missing := ds.Key("nope")
		_, err := s.Get(missing, sdk.AllBins)
		gm.Expect(err).To(gm.HaveOccurred())
		var e *sdk.Error
		gm.Expect(asSDKError(err, &e)).To(gm.BeTrue())
		gm.Expect(e.Kind()).To(gm.Equal(sdk.KindRecordNotFound))
	})
})

var _ = gg.Describe("SDK keys", func() {
	// DataSet.Key returns no error, so the assertion worth making is not that
	// minting succeeds but that it agrees with the core client for every type
	// KeyValue admits -- which is also what makes the dropped error safe.
	sameAsCore := func(ds *sdk.DataSet, got *as.Key, identifier any) {
		gm.ExpectWithOffset(1, got).ToNot(gm.BeNil())
		want, err := as.NewKey(ds.Namespace(), ds.SetName(), identifier)
		gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())
		gm.ExpectWithOffset(1, got.Digest()).To(gm.Equal(want.Digest()))
	}

	gg.It("must mint every KeyValue member to the core client's digest", func() {
		_, ds := newSession()
		sameAsCore(ds, ds.Key("s"), "s")
		sameAsCore(ds, ds.Key(42), 42)
		sameAsCore(ds, ds.Key(int64(42)), int64(42))
		sameAsCore(ds, ds.Key(uint32(42)), uint32(42))
		sameAsCore(ds, ds.Key([]byte{1, 2}), []byte{1, 2})
	})

	// DataSet.Key panics if KeyValue ever admits a type the key writer rejects.
	// Every one of the ten members is exercised here so that claim is tested
	// rather than asserted.
	gg.It("must mint all ten KeyValue members without panicking", func() {
		_, ds := newSession()
		gm.Expect(ds.Key("s")).ToNot(gm.BeNil())
		gm.Expect(ds.Key([]byte{1, 2})).ToNot(gm.BeNil())

		// Every integer member at 42 must agree with int64(42): the normalization
		// is what lets one record be addressed by whichever width the caller has.
		want := ds.Key(int64(42)).Digest()
		for name, got := range map[string][]byte{
			"int":    ds.Key(42).Digest(),
			"int8":   ds.Key(int8(42)).Digest(),
			"int16":  ds.Key(int16(42)).Digest(),
			"int32":  ds.Key(int32(42)).Digest(),
			"int64":  ds.Key(int64(42)).Digest(),
			"uint8":  ds.Key(uint8(42)).Digest(),
			"uint16": ds.Key(uint16(42)).Digest(),
			"uint32": ds.Key(uint32(42)).Digest(),
		} {
			gm.Expect(got).To(gm.Equal(want), name)
		}
	})

	// KeyValue stops at uint32 among the unsigned types precisely so that no
	// admitted identifier can exceed MaxInt64 and alias onto a negative key.
	// uint32's maximum must therefore survive as itself.
	gg.It("must round-trip the largest admitted unsigned identifier", func() {
		_, ds := newSession()
		sameAsCore(ds, ds.Key(^uint32(0)), int64(math.MaxUint32))
		gm.Expect(ds.Key(^uint32(0)).Digest()).ToNot(gm.Equal(ds.Key(int64(-1)).Digest()))
	})

	// ID takes `any`, so it must report a bad identifier rather than abort the
	// caller. The core client's NewValue panics for a type it cannot represent
	// and errors for one it can represent but cannot key on; both arrive here as
	// an error.
	gg.It("must report a bad dynamic identifier as an error, not a panic", func() {
		_, ds := newSession()
		for _, bad := range []any{
			struct{}{},       // NewValue panics: unrepresentable
			uint64(42),       // NewValue panics: unsupported width
			1.5,              // key writer errors: FloatValue
			true,             // key writer errors: BoolValue
			map[string]int{}, // not a user key
		} {
			key, err := ds.ID(bad)
			gm.Expect(err).To(gm.HaveOccurred(), "%T", bad)
			gm.Expect(key).To(gm.BeNil(), "%T", bad)
		}
	})

	gg.It("must still mint a good dynamic identifier", func() {
		_, ds := newSession()
		key, err := ds.ID(int64(7))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		sameAsCore(ds, key, int64(7))
	})

	gg.It("must mint a named type as its underlying type", func() {
		_, ds := newSession()
		type UserID int64
		sameAsCore(ds, ds.Key(UserID(7)), int64(7))
	})

	gg.It("must mint many keys in input order", func() {
		_, ds := newSession()
		ids := []int64{3, 1, 2}
		keys := ds.Keys(ids)
		gm.Expect(keys).To(gm.HaveLen(len(ids)))
		for i, id := range ids {
			sameAsCore(ds, keys[i], id)
		}
	})
})

var _ = gg.Describe("SDK builder chain", func() {
	gg.It("must apply several operations to one bin and report the results positionally", func() {
		s, ds := newSession()
		key := ds.Key("counter")

		stream, err := s.Upsert(key).
			Bin("counter").SetTo(100).
			Bin("counter").Add(11).
			Bin("counter").Get().
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()

		row, err := stream.FirstOrRaise()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Only the Get produces a value: a put and an add send no result back,
		// so the positional view holds exactly one entry, carrying the
		// accumulated total.
		gm.Expect(row.OperationResults()).To(gm.HaveLen(1))
		first, ok := row.OperationResult(0)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(first).To(gm.BeEquivalentTo(111))
		_, ok = row.OperationResult(5)
		gm.Expect(ok).To(gm.BeFalse(), "an out-of-range position must report absence")
	})

	gg.It("must accept one key or many through the same verb", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{0, 1, 2})

		stream, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(3))

		_, err = s.Upsert(keys[0]).SetTo("v", 2).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.It("must chain segments into one batch", func() {
		s, ds := newSession()
		k1 := ds.Key("a")
		k2 := ds.Key("b")
		gm.Expect(s.Put(k1, as.BinMap{"counter": 1})).ToNot(gm.HaveOccurred())

		stream, err := s.Update(k1).Add("counter", 5).
			Insert(k2).SetTo("status", "new").
			Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(2))
	})

	gg.It("must defer an argument error to the terminal", func() {
		s, ds := newSession()
		key := ds.Key("guard")
		_, err := s.Upsert(key).SetTo("v", 1).EnsureGenerationIs(0).Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must refuse to execute a chain twice", func() {
		s, ds := newSession()
		key := ds.Key("once")
		b := s.Upsert(key).SetTo("v", 1)
		_, err := b.Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, err = b.Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

var _ = gg.Describe("SDK streams", func() {
	gg.It("must iterate with a range-over-function loop", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{0, 1, 2, 3})
		_, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(keys).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()

		count := 0
		for row := range stream.Iter() {
			if row.IsOK() {
				count++
			}
		}
		gm.Expect(stream.Err()).ToNot(gm.HaveOccurred())
		gm.Expect(count).To(gm.Equal(4))
	})

	gg.It("must deliver rows lazily from Stream", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{0, 1, 2, 3, 4})
		_, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(keys).Stream()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer stream.Close()

		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(5))
		// Rows arrive in completion order, so Index is the way back to input
		// order.
		for _, r := range rows {
			gm.Expect(r.Index).To(gm.BeNumerically(">=", 0))
		}
	})

	gg.It("must scan a whole dataset", func() {
		s, ds := newSession()
		keys := ds.Keys([]int64{0, 1, 2, 3, 4})
		_, err := s.Upsert(keys).SetTo("v", 1).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		stream, err := s.Query(ds).Execute()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rows, err := stream.Collect()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rows).To(gm.HaveLen(5))
	})
})

var _ = gg.Describe("SDK behavior", func() {
	gg.It("must resolve the scope hierarchy", func() {
		def := sdk.DefaultBehavior()

		q := def.Settings(sdk.OpRead, sdk.ShapeQuery, sdk.ModeAP)
		gm.Expect(q.MaxRetries).ToNot(gm.BeNil())
		gm.Expect(*q.MaxRetries).To(gm.Equal(5))

		p := def.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP)
		gm.Expect(*p.MaxRetries).To(gm.Equal(2))

		w := def.Settings(sdk.OpWriteNonRetryable, sdk.ShapePoint, sdk.ModeSC)
		gm.Expect(*w.DurableDelete).To(gm.BeTrue())
	})

	gg.It("must let a child's All patch beat the parent's specific scope", func() {
		child := sdk.DefaultBehavior().DeriveWithChanges("child_"+randomSet(),
			map[sdk.Scope]sdk.Settings{
				sdk.ScopeAll: {MaxRetries: sdk.IntPtr(7)},
			})
		q := child.Settings(sdk.OpRead, sdk.ShapeQuery, sdk.ModeAP)
		gm.Expect(*q.MaxRetries).To(gm.Equal(7))
	})
})

var _ = gg.Describe("SDK capability probes", func() {
	gg.It("must report what the cluster supports", func() {
		gg.GinkgoWriter.Printf("MRT=%v CDTPath=%v StringOps=%v AEL=%v ExtendedErrors=%v BlobIndex=%v\n",
			testCluster.SupportsMRT(),
			testCluster.SupportsCDTPathExpressions(),
			testCluster.SupportsStringOperations(),
			testCluster.SupportsServerCompiledAEL(),
			testCluster.SupportsExtendedErrorDetail(),
			testCluster.SupportsBlobIndex())
	})
})

// asSDKError is a small errors.As for the SDK error type.
func asSDKError(err error, target **sdk.Error) bool {
	for err != nil {
		if e, ok := err.(*sdk.Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

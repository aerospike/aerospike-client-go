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
	"reflect"
	"testing"
)

// Wire-shape assertions for the string-op CONTEXT_EVAL envelope introduced by
// SERVER-1483: `[0xFF, ctx_list, [SUBOP, args...]]`. These decode the
// pre-packed payload rather than talking to a server, so a regression to the
// old flat shape fails here instead of surfacing as PARAMETER_ERROR at runtime.

func stringOpPayload(t *testing.T, op *Operation) []any {
	t.Helper()

	rbv, ok := op.binValue.(*RawBlobValue)
	if !ok {
		t.Fatalf("expected *RawBlobValue payload, got %T", op.binValue)
	}

	obj, err := newUnpacker(rbv.Data, 0, len(rbv.Data)).unpackObjects()
	if err != nil {
		t.Fatalf("unpacking payload: %v", err)
	}

	list, ok := obj.([]any)
	if !ok {
		t.Fatalf("expected payload to decode to a list, got %T", obj)
	}
	return list
}

// normalize collapses the unpacker's numeric types so expected values can be
// written as plain ints.
func normalize(v any) any {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case []any:
		out := make([]any, len(n))
		for i := range n {
			out[i] = normalize(n[i])
		}
		return out
	default:
		return v
	}
}

func assertShape(t *testing.T, op *Operation, want []any) {
	t.Helper()

	got := normalize(stringOpPayload(t, op))
	if !reflect.DeepEqual(got, normalize(want)) {
		t.Errorf("wire shape mismatch\n got: %#v\nwant: %#v", got, normalize(want))
	}
}

func TestStringOpWireShapeWithCtx(t *testing.T) {
	t.Run("read op", func(t *testing.T) {
		assertShape(t, StrLenOp("b", CtxListIndex(2)), []any{
			255,
			[]any{ctxTypeListIndex, 2},
			[]any{_STR_OP_STRLEN},
		})
	})

	t.Run("read op with args", func(t *testing.T) {
		assertShape(t, StrStartsWithOp("b", "Wor", CtxMapKey(StringValue("b"))), []any{
			255,
			[]any{ctxTypeMapKey, "b"},
			[]any{_STR_OP_STARTS_WITH, "Wor"},
		})
	})

	t.Run("modify op", func(t *testing.T) {
		assertShape(t, StrUpperOp(DefaultStringPolicy, "b", CtxListIndex(1)), []any{
			255,
			[]any{ctxTypeListIndex, 1},
			[]any{_STR_OP_UPPER, int(StringWriteDefault)},
		})
	})

	// The trailing flags element is the case the nesting exists to make safe:
	// in the old flat form it was indistinguishable from an optional argument.
	t.Run("modify op with non-default trailing flags", func(t *testing.T) {
		policy := NewStringPolicy(StringWriteNoFail)
		assertShape(t, StrAppendOp(policy, "b", "!", CtxListIndex(1)), []any{
			255,
			[]any{ctxTypeListIndex, 1},
			[]any{_STR_OP_APPEND, "!", int(StringWriteNoFail)},
		})
	})

	t.Run("nested ctx path", func(t *testing.T) {
		assertShape(t, StrUpperOp(DefaultStringPolicy, "b",
			CtxMapKey(StringValue("items")), CtxListIndex(1)), []any{
			255,
			[]any{ctxTypeMapKey, "items", ctxTypeListIndex, 1},
			[]any{_STR_OP_UPPER, int(StringWriteDefault)},
		})
	})
}

func TestStringOpWireShapeWithoutCtx(t *testing.T) {
	t.Run("read op", func(t *testing.T) {
		assertShape(t, StrLenOp("b"), []any{_STR_OP_STRLEN})
	})

	t.Run("modify op with trailing flags", func(t *testing.T) {
		policy := NewStringPolicy(StringWriteNoFail)
		assertShape(t, StrAppendOp(policy, "b", "!"), []any{
			_STR_OP_APPEND, "!", int(StringWriteNoFail),
		})
	})
}

// The size-estimation pass and the write pass must agree; newStringOp sizes the
// buffer from the first and fills it with the second.
func TestStringOpSizeEstimateMatchesWrite(t *testing.T) {
	cases := []struct {
		name  string
		subop int
		ctx   []*CDTContext
		args  []any
	}{
		{"no ctx, no args", _STR_OP_STRLEN, nil, nil},
		{"no ctx, args", _STR_OP_APPEND, nil, []any{StringValue("!"), IntegerValue(4)}},
		{"ctx, no args", _STR_OP_STRLEN, []*CDTContext{CtxListIndex(2)}, nil},
		{"ctx, args", _STR_OP_APPEND, []*CDTContext{CtxListIndex(2)}, []any{StringValue("!"), IntegerValue(4)}},
		{"nested ctx, args", _STR_OP_PAD_END,
			[]*CDTContext{CtxMapKey(StringValue("items")), CtxListIndex(1)},
			[]any{IntegerValue(10), StringValue("."), IntegerValue(0)}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sz, err := packStringOpBytes(nil, c.subop, c.ctx, c.args)
			if err != nil {
				t.Fatalf("size pass: %v", err)
			}

			buf := newBuffer(sz)
			written, err := packStringOpBytes(buf, c.subop, c.ctx, c.args)
			if err != nil {
				t.Fatalf("write pass: %v", err)
			}

			if written != sz {
				t.Errorf("size pass returned %d, write pass wrote %d", sz, written)
			}
			if len(buf.Bytes()) != sz {
				t.Errorf("buffer holds %d bytes, size pass returned %d", len(buf.Bytes()), sz)
			}
		})
	}
}

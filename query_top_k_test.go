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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

// The client re-ranks the records each node returns, so it must order them
// exactly as the server does (query.c top_k_ranks_ahead).

var (
	ascInteger  = orderBySpec{binName: "score", orderType: BinDataTypeInteger, direction: OrderAsc}
	descInteger = orderBySpec{binName: "score", orderType: BinDataTypeInteger, direction: OrderDesc}
	ascDouble   = orderBySpec{binName: "score", orderType: BinDataTypeDouble, direction: OrderAsc}
	ascString   = orderBySpec{binName: "name", orderType: BinDataTypeString, direction: OrderAsc}
	foldString  = orderBySpec{binName: "name", orderType: BinDataTypeString, direction: OrderAsc, flags: OrderByFlagCaseInsensitive}
	ascBytes    = orderBySpec{binName: "blob", orderType: BinDataTypeBytes, direction: OrderAsc}
	nilValue    = orderByValue{isNil: true}
)

func TestCompareValues(t *testing.T) {
	testCases := []struct {
		name        string
		spec        orderBySpec
		left, right orderByValue
		want        int
	}{
		{"a smaller integer first", ascInteger, orderByValue{integer: -3}, orderByValue{integer: 2}, -1},
		{"the integer extremes", ascInteger, orderByValue{integer: math.MinInt64}, orderByValue{integer: math.MaxInt64}, -1},
		{"equal integers", ascInteger, orderByValue{integer: 7}, orderByValue{integer: 7}, 0},

		{"a smaller double first", ascDouble, orderByValue{float: 1.5}, orderByValue{float: 2.5}, -1},
		{"NaN above infinity", ascDouble, orderByValue{float: math.NaN()}, orderByValue{float: math.Inf(1)}, 1},
		{"a number below NaN", ascDouble, orderByValue{float: math.Inf(-1)}, orderByValue{float: math.NaN()}, -1},
		{"NaN equal to NaN", ascDouble, orderByValue{float: math.NaN()}, orderByValue{float: math.NaN()}, 0},
		{"-0.0 equal to 0.0", ascDouble, orderByValue{float: math.Copysign(0, -1)}, orderByValue{float: 0}, 0},

		{"strings by bytes", ascString, orderByValue{str: "apple"}, orderByValue{str: "banana"}, -1},
		{"an upper-case letter before a lower-case one by default", ascString, orderByValue{str: "b"}, orderByValue{str: "B"}, 1},
		{"a shorter string first on a common prefix", ascString, orderByValue{str: "ab"}, orderByValue{str: "abc"}, -1},
		{"the empty string first", ascString, orderByValue{str: ""}, orderByValue{str: "a"}, -1},
		{"ASCII letters folded when case is ignored", foldString, orderByValue{str: "b"}, orderByValue{str: "B"}, 0},
		{"folded letters compared by their lower case", foldString, orderByValue{str: "apple"}, orderByValue{str: "Banana"}, -1},
		{"a shorter folded string first on a common prefix", foldString, orderByValue{str: "AB"}, orderByValue{str: "abc"}, -1},
		{"the Kelvin sign not folded to k", foldString, orderByValue{str: "K"}, orderByValue{str: "k"}, 1},
		{"accented letters not folded", foldString, orderByValue{str: "é"}, orderByValue{str: "É"}, 1},

		{"bytes by value", ascBytes, orderByValue{blob: []byte{0x00, 0xff}}, orderByValue{blob: []byte{0xff}}, -1},
		{"shorter bytes first on a common prefix", ascBytes, orderByValue{blob: []byte{1, 2}}, orderByValue{blob: []byte{1, 2, 0}}, -1},
		{"equal bytes", ascBytes, orderByValue{blob: []byte{1, 2}}, orderByValue{blob: []byte{1, 2}}, 0},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := compareValues(&testCase.spec, &testCase.left, &testCase.right); got != testCase.want {
				t.Errorf("compareValues = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestRankOrder(t *testing.T) {
	testCases := []struct {
		name         string
		spec         orderBySpec
		value, other orderByValue
		want         int
	}{
		{"ascending puts the smaller value ahead", ascInteger, orderByValue{integer: 1}, orderByValue{integer: 2}, -1},
		{"descending puts the larger value ahead", descInteger, orderByValue{integer: 1}, orderByValue{integer: 2}, 1},
		{"a nil value ranks last when ascending", ascInteger, nilValue, orderByValue{integer: math.MaxInt64}, 1},
		{"a nil value ranks last when descending", descInteger, orderByValue{integer: math.MinInt64}, nilValue, -1},
		{"two nil values tie", descInteger, nilValue, nilValue, 0},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := rankOrder(&testCase.spec, &testCase.value, &testCase.other); got != testCase.want {
				t.Errorf("rankOrder = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestOrderByValueOf(t *testing.T) {
	record := &Record{Bins: BinMap{
		"int":    7,
		"int64":  int64(8),
		"float":  1.5,
		"string": "s",
		"bytes":  []byte{1},
		"bool":   true,
		"geo":    GeoJSONValue(`{"type":"Point","coordinates":[0,0]}`),
		"hll":    HLLValue{1},
		"list":   []any{1},
	}}

	testCases := []struct {
		bin       string
		orderType BinDataType
		want      orderByValue
	}{
		{"int", BinDataTypeInteger, orderByValue{integer: 7}},
		{"int64", BinDataTypeInteger, orderByValue{integer: 8}},
		{"float", BinDataTypeDouble, orderByValue{float: 1.5}},
		{"string", BinDataTypeString, orderByValue{str: "s"}},
		{"bytes", BinDataTypeBytes, orderByValue{blob: []byte{1}}},
		{"missing", BinDataTypeInteger, nilValue},
		{"int", BinDataTypeDouble, nilValue},
		{"float", BinDataTypeInteger, nilValue},
		{"bool", BinDataTypeInteger, nilValue},
		{"geo", BinDataTypeString, nilValue},
		{"hll", BinDataTypeBytes, nilValue},
		{"list", BinDataTypeInteger, nilValue},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s as type %d", testCase.bin, testCase.orderType), func(t *testing.T) {
			got := orderByValueOf(record, orderBySpec{binName: testCase.bin, orderType: testCase.orderType})
			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("orderByValueOf = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestTopKMergerRanking(t *testing.T) {
	// Each case offers records to a merger built from the statement, then
	// checks the order results() returns them in, by digest first byte.
	testCases := []struct {
		name    string
		orderBy func(statement *Statement)
		records []struct {
			digestByte byte
			bins       BinMap
		}
		want []byte
	}{
		{
			name: "the first key decides",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5}}, {2, BinMap{"score": 9}}},
			want: []byte{2, 1},
		},
		{
			name: "the second key decides a tie on the first",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5, "name": "b"}}, {2, BinMap{"score": 5, "name": "a"}}},
			want: []byte{2, 1},
		},
		{
			name: "the second key decides when both first values are missing",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"name": "b"}}, {2, BinMap{"name": "a"}}},
			want: []byte{2, 1},
		},
		{
			name: "a first value ranks ahead of a missing one, whatever the second",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"name": "a"}}, {2, BinMap{"score": 1, "name": "z"}}},
			want: []byte{2, 1},
		},
		{
			name: "the smaller digest goes first on a tie on every key",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{2, BinMap{"score": 5, "name": "a"}}, {1, BinMap{"score": 5, "name": "a"}}},
			want: []byte{1, 2},
		},
		{
			name: "the smaller digest goes first when ascending too",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{2, BinMap{"score": 5}}, {1, BinMap{"score": 5}}},
			want: []byte{1, 2},
		},
		{
			name: "a bin that isn't an order-by key doesn't break ties",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{2, BinMap{"score": 5, "name": "a"}}, {1, BinMap{"score": 5, "name": "z"}}},
			want: []byte{1, 2},
		},
		{
			name: "the second key puts the larger value first when descending",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5, "name": "a"}}, {2, BinMap{"score": 5, "name": "b"}}},
			want: []byte{2, 1},
		},
		{
			name: "a missing second value ranks last on a tie when ascending",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5}}, {2, BinMap{"score": 5, "name": "z"}}},
			want: []byte{2, 1},
		},
		{
			name: "a missing second value ranks last on a tie when descending",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("name", BinDataTypeString, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5}}, {2, BinMap{"score": 5, "name": "a"}}},
			want: []byte{2, 1},
		},
		{
			name: "the second key ignores case when asked",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderByWithFlags("name", BinDataTypeString, OrderAsc, OrderByFlagCaseInsensitive)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5, "name": "Banana"}}, {2, BinMap{"score": 5, "name": "apple"}}},
			want: []byte{2, 1},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := NewStatement("test", "topk")
			testCase.orderBy(statement)
			if err := statement.SetTopK(10); err != nil {
				t.Fatal(err)
			}
			merger := newTopKMerger(statement)

			for _, record := range testCase.records {
				digest := make([]byte, 20)
				digest[0] = record.digestByte
				key, err := NewKeyWithDigest("test", "topk", nil, digest)
				if err != nil {
					t.Fatal(err)
				}
				merger.add(&Result{Record: &Record{Key: key, Bins: record.bins}})
			}

			var got []byte
			for _, result := range merger.results() {
				got = append(got, result.Record.Key.Digest()[0])
			}
			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("results in digest order %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestTopKMerger(t *testing.T) {
	// Each case offers {score, digest first byte} records to a merger that keeps
	// the k highest scores, then checks what results() returns, best first.
	testCases := []struct {
		name    string
		k       int
		records [][2]int
		want    [][2]int
	}{
		{
			name:    "keeps the k best, best first",
			k:       3,
			records: [][2]int{{5, 1}, {1, 2}, {9, 3}, {7, 4}, {3, 5}, {8, 6}},
			want:    [][2]int{{9, 3}, {8, 6}, {7, 4}},
		},
		{
			name:    "returns every record when there are fewer than k",
			k:       10,
			records: [][2]int{{2, 1}, {6, 2}, {4, 3}},
			want:    [][2]int{{6, 2}, {4, 3}, {2, 1}},
		},
		{
			name: "keeps the better copy of a record offered twice",
			k:    3,
			records: [][2]int{
				{9, 1}, {8, 2}, {7, 3}, // full, best offered first so the heap reorders: 9, 8, 7
				{12, 1}, // a better copy of a record deep in the heap: replaces it
				{10, 1}, // a worse copy that would still make the top k: ignored
				{1, 2},  // a worse copy that can't make the top k: ignored
				{11, 4}, // a new record: evicts the worst
			},
			want: [][2]int{{12, 1}, {11, 4}, {8, 2}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := NewStatement("test", "topk")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			if err := statement.SetTopK(testCase.k); err != nil {
				t.Fatal(err)
			}
			merger := newTopKMerger(statement)

			for _, record := range testCase.records {
				digest := make([]byte, 20)
				digest[0] = byte(record[1])
				key, err := NewKeyWithDigest("test", "topk", nil, digest)
				if err != nil {
					t.Fatal(err)
				}
				merger.add(&Result{Record: &Record{Key: key, Bins: BinMap{"score": record[0]}}})
			}

			var got [][2]int
			for _, result := range merger.results() {
				got = append(got, [2]int{result.Record.Bins["score"].(int), int(result.Record.Key.Digest()[0])})
			}
			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("results %v, want %v", got, testCase.want)
			}
		})
	}

	t.Run("ranks the same when offered from many goroutines", func(t *testing.T) {
		const k, count, goroutines = 50, 1000, 8

		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		if err := statement.SetTopK(k); err != nil {
			t.Fatal(err)
		}

		// Scores repeat, so the digest decides some ties.
		results := make([]*Result, count)
		for index := range results {
			digest := make([]byte, 20)
			binary.BigEndian.PutUint16(digest, uint16(index))
			key, err := NewKeyWithDigest("test", "topk", nil, digest)
			if err != nil {
				t.Fatal(err)
			}
			results[index] = &Result{Record: &Record{Key: key, Bins: BinMap{"score": (index * 37) % 300}}}
		}

		sequential := newTopKMerger(statement)
		for _, result := range results {
			sequential.add(result)
		}

		concurrent := newTopKMerger(statement)
		var wg sync.WaitGroup
		for start := 0; start < goroutines; start++ {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for index := start; index < count; index += goroutines {
					concurrent.add(results[index])
				}
			}(start)
		}
		wg.Wait()

		want, got := sequential.results(), concurrent.results()
		if len(got) != k || !reflect.DeepEqual(got, want) {
			t.Errorf("concurrent offers kept %d records, not the same %d as sequential offers", len(got), len(want))
		}
	})
}

// queryFields builds the query command for statement and returns its fields
// by type.
func queryFields(t *testing.T, statement *Statement, sendTopK bool) map[FieldType][]byte {
	t.Helper()
	cmd := &baseCommand{bufferEx: bufferEx{dataBuffer: make([]byte, 1024)}}
	if err := cmd.setQuery(NewQueryPolicy(), nil, statement, 1, nil, false, nil, sendTopK); err != nil {
		t.Fatal(err)
	}

	buffer := cmd.dataBuffer[:cmd.dataOffset]
	fieldCount := int(binary.BigEndian.Uint16(buffer[26:]))
	fields := make(map[FieldType][]byte, fieldCount)
	offset := int(_MSG_TOTAL_HEADER_SIZE)
	for field := 0; field < fieldCount; field++ {
		size := int(binary.BigEndian.Uint32(buffer[offset:]))
		fields[FieldType(buffer[offset+4])] = buffer[offset+5 : offset+4+size]
		offset += 4 + size
	}
	return fields
}

func TestTopKWireFields(t *testing.T) {
	// Each order-by key: type, direction, flags, bin name length, bin name.
	// The limit: a 4-byte big-endian integer.
	t.Run("one order-by key and k", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.SetTopK(10)

		fields := queryFields(t, statement, true)
		if got, want := fields[ORDER_BY], []byte{1, 1, 0, 5, 's', 'c', 'o', 'r', 'e'}; !reflect.DeepEqual(got, want) {
			t.Errorf("ORDER_BY = %v, want %v", got, want)
		}
		if got, want := fields[TOP_K], []byte{0, 0, 0, 10}; !reflect.DeepEqual(got, want) {
			t.Errorf("TOP_K = %v, want %v", got, want)
		}
	})

	t.Run("two order-by keys, the second ignoring case", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.ThenOrderByWithFlags("name", BinDataTypeString, OrderAsc, OrderByFlagCaseInsensitive)
		statement.SetTopK(1000)

		fields := queryFields(t, statement, true)
		want := []byte{1, 1, 0, 5, 's', 'c', 'o', 'r', 'e', 3, 0, 1, 4, 'n', 'a', 'm', 'e'}
		if got := fields[ORDER_BY]; !reflect.DeepEqual(got, want) {
			t.Errorf("ORDER_BY = %v, want %v", got, want)
		}
		if got, want := fields[TOP_K], []byte{0, 0, 0x03, 0xe8}; !reflect.DeepEqual(got, want) {
			t.Errorf("TOP_K = %v, want %v", got, want)
		}
	})

	t.Run("an order-by without k sends no limit", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderAsc)

		fields := queryFields(t, statement, true)
		if got, want := fields[ORDER_BY], []byte{1, 0, 0, 5, 's', 'c', 'o', 'r', 'e'}; !reflect.DeepEqual(got, want) {
			t.Errorf("ORDER_BY = %v, want %v", got, want)
		}
		if limit, found := fields[TOP_K]; found {
			t.Errorf("TOP_K = %v, want no field", limit)
		}
	})

	t.Run("nothing when Top-K is not sent", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.SetTopK(10)

		fields := queryFields(t, statement, false)
		for _, fieldType := range []FieldType{ORDER_BY, TOP_K} {
			if value, found := fields[fieldType]; found {
				t.Errorf("field %d = %v, want no field", fieldType, value)
			}
		}
	})
}

func TestTopKRespondAllOps(t *testing.T) {
	// The server sends a query record only when a projected read has a value,
	// unless the request asks for every operation's result. A Top-K query asks,
	// so a record whose order-by value is empty still arrives and ranks last.
	// Other queries are sent as before.
	testCases := []struct {
		name      string
		statement func() *Statement
		want      bool
	}{
		{"a Top-K query projecting bin names", func() *Statement {
			statement := NewStatement("test", "topk", "score")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(10)
			return statement
		}, true},
		{"a Top-K query projecting operations", func() *Statement {
			statement := NewStatement("test", "topk")
			statement.Operations = []*Operation{GetBinOp("score")}
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(10)
			return statement
		}, true},
		{"a Top-K query returning whole records", func() *Statement {
			statement := NewStatement("test", "topk")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(10)
			return statement
		}, false},
		{"a Top-K query with an operation that reads every bin", func() *Statement {
			statement := NewStatement("test", "topk")
			statement.Operations = []*Operation{GetOp()}
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(10)
			return statement
		}, false},
		{"an order-by without k", func() *Statement {
			statement := NewStatement("test", "topk", "score")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			return statement
		}, false},
		{"a plain query projecting bin names", func() *Statement {
			return NewStatement("test", "topk", "score")
		}, false},
		{"a plain query projecting operations", func() *Statement {
			statement := NewStatement("test", "topk")
			statement.Operations = []*Operation{GetBinOp("score")}
			return statement
		}, false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := testCase.statement()
			cmd := &baseCommand{bufferEx: bufferEx{dataBuffer: make([]byte, 1024)}}
			if err := cmd.setQuery(NewQueryPolicy(), nil, statement, 1, statement.Operations, false, nil, statement.hasOrderBy()); err != nil {
				t.Fatal(err)
			}
			if got := cmd.dataBuffer[10]&byte(_INFO2_RESPOND_ALL_OPS) != 0; got != testCase.want {
				t.Errorf("asks for every operation's result = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestSendTopKResults(t *testing.T) {
	results := []*Result{{BVal: new(int64)}, {BVal: new(int64)}, {BVal: new(int64)}}

	t.Run("sends every result in order", func(t *testing.T) {
		recordset := newRecordset(len(results), 1)
		sendTopKResults(recordset, results)
		recordset.signalEnd()
		for position, want := range results {
			if got := <-recordset.records; got != want {
				t.Errorf("result %d is not the one sent at that position", position)
			}
		}
	})

	t.Run("stops once the recordset is closed", func(t *testing.T) {
		recordset := newRecordset(1, 1)
		go func() {
			defer recordset.signalEnd()
			sendTopKResults(recordset, results)
		}()

		if got := <-recordset.records; got != results[0] {
			t.Fatal("the first result is not the best one")
		}

		closed := make(chan struct{})
		go func() {
			recordset.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Fatal("Close is still waiting for the sender")
		}

		// The one-slot buffer may hold the second result; the third is never sent.
		for got := range recordset.records {
			if got == results[2] {
				t.Fatal("a result was sent after the recordset was closed")
			}
		}
	})
}

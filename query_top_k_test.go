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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// The client re-ranks the records each node returns, so it must order them
// exactly as the server does (query.c top_k_ranks_ahead).

var (
	ascInteger  = orderBySpec{binName: "score", orderType: BinDataTypeInteger, direction: OrderAsc}
	descInteger = orderBySpec{binName: "score", orderType: BinDataTypeInteger, direction: OrderDesc}
	ascDouble   = orderBySpec{binName: "score", orderType: BinDataTypeDouble, direction: OrderAsc}
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
		{"bytes", BinDataTypeBytes, orderByValue{blob: []byte{1}}},
		{"missing", BinDataTypeInteger, nilValue},
		{"int", BinDataTypeDouble, nilValue},
		{"float", BinDataTypeInteger, nilValue},
		{"bool", BinDataTypeInteger, nilValue},
		{"string", BinDataTypeBytes, nilValue},
		{"geo", BinDataTypeBytes, nilValue},
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
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5, "tag": []byte("b")}}, {2, BinMap{"score": 5, "tag": []byte("a")}}},
			want: []byte{2, 1},
		},
		{
			name: "the second key decides when both first values are missing",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"tag": []byte("b")}}, {2, BinMap{"tag": []byte("a")}}},
			want: []byte{2, 1},
		},
		{
			name: "a first value ranks ahead of a missing one, whatever the second",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"tag": []byte("a")}}, {2, BinMap{"score": 1, "tag": []byte("z")}}},
			want: []byte{2, 1},
		},
		{
			name: "the smaller digest goes first on a tie on every key",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{2, BinMap{"score": 5, "tag": []byte("a")}}, {1, BinMap{"score": 5, "tag": []byte("a")}}},
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
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5, "tag": []byte("a")}}, {2, BinMap{"score": 5, "tag": []byte("b")}}},
			want: []byte{2, 1},
		},
		{
			name: "a missing second value ranks last on a tie when ascending",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5}}, {2, BinMap{"score": 5, "tag": []byte("z")}}},
			want: []byte{2, 1},
		},
		{
			name: "a missing second value ranks last on a tie when descending",
			orderBy: func(statement *Statement) {
				statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
				statement.ThenOrderBy("tag", BinDataTypeBytes, OrderDesc)
			},
			records: []struct {
				digestByte byte
				bins       BinMap
			}{{1, BinMap{"score": 5}}, {2, BinMap{"score": 5, "tag": []byte("a")}}},
			want: []byte{2, 1},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := NewStatement("test", "topk")
			testCase.orderBy(statement)
			statement.SetTopK(10)
			merger := newTopKMerger(statement)

			for _, record := range testCase.records {
				digest := make([]byte, 20)
				digest[0] = record.digestByte
				key, err := NewKeyWithDigest("test", "topk", nil, digest)
				if err != nil {
					t.Fatal(err)
				}
				merger.add(&Result{Record: &Record{Key: key, Bins: record.bins}}, 0)
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
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := NewStatement("test", "topk")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(testCase.k)
			merger := newTopKMerger(statement)

			for _, record := range testCase.records {
				digest := make([]byte, 20)
				digest[0] = byte(record[1])
				key, err := NewKeyWithDigest("test", "topk", nil, digest)
				if err != nil {
					t.Fatal(err)
				}
				merger.add(&Result{Record: &Record{Key: key, Bins: BinMap{"score": record[0]}}}, 0)
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

	t.Run("keeps exactly the k best of many offers in any arrival order", func(t *testing.T) {
		const count = 1000
		type scoredRecord struct {
			score  int
			digest []byte
		}

		// Scores repeat, so the digest decides ties; digests are unique.
		records := make([]scoredRecord, count)
		for index := range records {
			digest := make([]byte, 20)
			binary.BigEndian.PutUint32(digest, uint32(index*2654435761))
			records[index] = scoredRecord{score: (index * 37) % 300, digest: digest}
		}

		// The expected order: highest score first, then smallest digest.
		expected := slices.Clone(records)
		slices.SortFunc(expected, func(left, right scoredRecord) int {
			if left.score != right.score {
				return right.score - left.score
			}
			return bytes.Compare(left.digest, right.digest)
		})

		shuffler := rand.New(rand.NewSource(7))
		for _, k := range []int{1, 10, 250, 2000} {
			statement := NewStatement("test", "topk")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(k)

			for round := range 3 {
				merger := newTopKMerger(statement)
				for _, index := range shuffler.Perm(count) {
					key, err := NewKeyWithDigest("test", "topk", nil, records[index].digest)
					if err != nil {
						t.Fatal(err)
					}
					merger.add(&Result{Record: &Record{Key: key, Bins: BinMap{"score": records[index].score}}}, 0)
				}

				got := merger.results()
				want := expected[:min(k, count)]
				if len(got) != len(want) {
					t.Fatalf("k=%d round %d: kept %d records, want %d", k, round, len(got), len(want))
				}
				for position, result := range got {
					if result.Record.Bins["score"] != want[position].score || !bytes.Equal(result.Record.Key.Digest(), want[position].digest) {
						t.Fatalf("k=%d round %d: position %d is score %v, want %d", k, round, position, result.Record.Bins["score"], want[position].score)
					}
				}
			}
		}
	})

	t.Run("ranks the same when offered from many goroutines", func(t *testing.T) {
		const k, count, goroutines = 50, 1000, 8

		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.SetTopK(k)

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
			sequential.add(result, 0)
		}

		concurrent := newTopKMerger(statement)
		var wg sync.WaitGroup
		for start := 0; start < goroutines; start++ {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for index := start; index < count; index += goroutines {
					concurrent.add(results[index], 0)
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

func TestTopKMergerLastUpdateTime(t *testing.T) {
	// Each record is {score, digest first byte, last-update time}; the merger
	// keeps the k highest scores and one copy of each record.
	testCases := []struct {
		name    string
		k       int
		records [][3]int64
		want    [][2]int64
	}{
		{
			name:    "keeps a later copy even when it ranks worse",
			k:       3,
			records: [][3]int64{{9, 1, 100}, {8, 2, 100}, {7, 3, 100}, {5, 1, 200}},
			want:    [][2]int64{{8, 2}, {7, 3}, {5, 1}},
		},
		{
			name:    "ignores an earlier copy that still ranks better",
			k:       3,
			records: [][3]int64{{4, 1, 300}, {10, 1, 200}},
			want:    [][2]int64{{4, 1}},
		},
		{
			name:    "keeps the better-ranked copy when the times are equal",
			k:       3,
			records: [][3]int64{{6, 1, 100}, {8, 1, 100}},
			want:    [][2]int64{{8, 1}},
		},
		{
			name:    "keeps the better-ranked copy whichever arrives first",
			k:       3,
			records: [][3]int64{{8, 1, 100}, {6, 1, 100}},
			want:    [][2]int64{{8, 1}},
		},
		{
			name:    "keeps the better-ranked copy when the server sends no time",
			k:       3,
			records: [][3]int64{{6, 1, 0}, {8, 1, 0}},
			want:    [][2]int64{{8, 1}},
		},
		{
			name: "replaces a record deep in the heap and drops a later copy that can't make the top k",
			k:    2,
			records: [][3]int64{
				{9, 1, 100}, {8, 2, 100},
				{12, 1, 200}, // a later, better copy of the best record: replaces it
				{1, 2, 200},  // a later copy that ranks last: replaces the earlier one
				{7, 3, 100},  // a new record: evicts the worst
			},
			want: [][2]int64{{12, 1}, {7, 3}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statement := NewStatement("test", "topk")
			statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
			statement.SetTopK(testCase.k)
			merger := newTopKMerger(statement)

			for _, record := range testCase.records {
				digest := make([]byte, 20)
				digest[0] = byte(record[1])
				key, err := NewKeyWithDigest("test", "topk", nil, digest)
				if err != nil {
					t.Fatal(err)
				}
				merger.add(&Result{Record: &Record{Key: key, Bins: BinMap{"score": int(record[0])}}}, record[2])
			}

			var got [][2]int64
			for _, result := range merger.results() {
				got = append(got, [2]int64{int64(result.Record.Bins["score"].(int)), int64(result.Record.Key.Digest()[0])})
			}
			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("results %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestParseKeyLastUpdateTime(t *testing.T) {
	// recordFields lays out a record's fields as the server sends them:
	// a 4-byte big-endian size (type byte included), the type, then the data.
	recordFields := func(fields map[FieldType][]byte) []byte {
		var wire []byte
		for fieldType, data := range fields {
			wire = binary.BigEndian.AppendUint32(wire, uint32(len(data)+1))
			wire = append(wire, byte(fieldType))
			wire = append(wire, data...)
		}
		return wire
	}
	parse := func(cmd *baseMultiCommand, fields map[FieldType][]byte) (*Key, Error) {
		wire := recordFields(fields)
		cmd.bc = bufferedConn{conn: &Connection{dataBuffer: wire}, tail: len(wire)}
		return cmd.parseKey(len(fields), nil)
	}
	digest := bytes.Repeat([]byte{0xAB}, 20)

	t.Run("reads the field as a big-endian count", func(t *testing.T) {
		cmd := &baseMultiCommand{topKMerger: &topKMerger{}}
		lut := binary.BigEndian.AppendUint64(nil, 0x0102030405060708)
		if _, err := parse(cmd, map[FieldType][]byte{DIGEST_RIPE: digest, LAST_UPDATE_TIME: lut}); err != nil {
			t.Fatal(err)
		}
		if cmd.lastUpdateTime != 0x0102030405060708 {
			t.Errorf("last-update time = %#x, want 0x0102030405060708", cmd.lastUpdateTime)
		}
	})

	t.Run("leaves it 0 when the server sends none", func(t *testing.T) {
		cmd := &baseMultiCommand{topKMerger: &topKMerger{}}
		if _, err := parse(cmd, map[FieldType][]byte{DIGEST_RIPE: digest}); err != nil {
			t.Fatal(err)
		}
		if cmd.lastUpdateTime != 0 {
			t.Errorf("last-update time = %d, want 0", cmd.lastUpdateTime)
		}
	})

	t.Run("refuses a field that is not 8 bytes", func(t *testing.T) {
		cmd := &baseMultiCommand{topKMerger: &topKMerger{}}
		_, err := parse(cmd, map[FieldType][]byte{LAST_UPDATE_TIME: {1, 2, 3, 4}})
		if err == nil || !err.Matches(types.PARSE_ERROR) {
			t.Errorf("err = %v, want PARSE_ERROR", err)
		}
	})

	t.Run("ignores the field when the query is not Top-K", func(t *testing.T) {
		for _, lut := range [][]byte{{1, 2, 3, 4}, binary.BigEndian.AppendUint64(nil, 0x0102030405060708)} {
			cmd := &baseMultiCommand{}
			key, err := parse(cmd, map[FieldType][]byte{DIGEST_RIPE: digest, LAST_UPDATE_TIME: lut})
			if err != nil {
				t.Fatalf("%d-byte field: %v", len(lut), err)
			}
			if cmd.lastUpdateTime != 0 {
				t.Errorf("%d-byte field: last-update time = %#x, want 0", len(lut), cmd.lastUpdateTime)
			}
			if !bytes.Equal(key.Digest(), digest) {
				t.Errorf("%d-byte field: digest = %x, want %x", len(lut), key.Digest(), digest)
			}
		}
	})

	t.Run("clears the previous record's value", func(t *testing.T) {
		cmd := &baseMultiCommand{topKMerger: &topKMerger{}}
		lut := binary.BigEndian.AppendUint64(nil, 42)
		if _, err := parse(cmd, map[FieldType][]byte{DIGEST_RIPE: digest, LAST_UPDATE_TIME: lut}); err != nil {
			t.Fatal(err)
		}
		if _, err := parse(cmd, map[FieldType][]byte{DIGEST_RIPE: digest}); err != nil {
			t.Fatal(err)
		}
		if cmd.lastUpdateTime != 0 {
			t.Errorf("last-update time = %d, want 0 for a record without the field", cmd.lastUpdateTime)
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

	t.Run("two order-by keys", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.ThenOrderBy("tag", BinDataTypeBytes, OrderAsc)
		statement.SetTopK(1000)

		fields := queryFields(t, statement, true)
		want := []byte{1, 1, 0, 5, 's', 'c', 'o', 'r', 'e', 4, 0, 0, 3, 't', 'a', 'g'}
		if got := fields[ORDER_BY]; !reflect.DeepEqual(got, want) {
			t.Errorf("ORDER_BY = %v, want %v", got, want)
		}
		if got, want := fields[TOP_K], []byte{0, 0, 0x03, 0xe8}; !reflect.DeepEqual(got, want) {
			t.Errorf("TOP_K = %v, want %v", got, want)
		}
	})

	// SetOrderBy sets the first key and clears the second; ThenOrderBy sets
	// the second key and is refused when no first key is set.
	price := []byte{1, 1, 0, 5, 'p', 'r', 'i', 'c', 'e'}
	ts := []byte{1, 0, 0, 2, 't', 's'}
	orderByField := func(t *testing.T, statement *Statement) []byte {
		t.Helper()
		statement.SetTopK(10)
		return queryFields(t, statement, true)[ORDER_BY]
	}

	t.Run("a second SetOrderBy sends only its own key", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.ThenOrderBy("ts", BinDataTypeInteger, OrderAsc)
		statement.SetOrderBy("price", BinDataTypeInteger, OrderDesc)

		if got := orderByField(t, statement); !reflect.DeepEqual(got, price) {
			t.Errorf("ORDER_BY = %v, want %v", got, price)
		}
	})

	t.Run("a ThenOrderBy after a second SetOrderBy sends both keys in order", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
		statement.ThenOrderBy("ts", BinDataTypeInteger, OrderAsc)
		statement.SetOrderBy("price", BinDataTypeInteger, OrderDesc)
		statement.ThenOrderBy("ts", BinDataTypeInteger, OrderAsc)

		if got, want := orderByField(t, statement), append(append([]byte{}, price...), ts...); !reflect.DeepEqual(got, want) {
			t.Errorf("ORDER_BY = %v, want %v", got, want)
		}
	})

	t.Run("a refused ThenOrderBy leaves no key behind", func(t *testing.T) {
		statement := NewStatement("test", "topk")
		statement.ThenOrderBy("ts", BinDataTypeInteger, OrderAsc)
		statement.SetOrderBy("price", BinDataTypeInteger, OrderDesc)

		if got := orderByField(t, statement); !reflect.DeepEqual(got, price) {
			t.Errorf("ORDER_BY = %v, want %v", got, price)
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

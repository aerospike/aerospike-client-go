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

package sdk

import (
	"bytes"
	"cmp"
	"fmt"
	"sort"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// SortDir is a sort direction.
type SortDir int

// The sort directions.
const (
	// Ascending sorts smallest first.
	Ascending SortDir = iota
	// Descending sorts largest first.
	Descending
)

// SortProperties names a bin to sort by, a direction, and whether string
// comparison ignores case.
type SortProperties struct {
	Name            string
	Dir             SortDir
	CaseInsensitive bool
}

// Asc sorts ascending by a bin, case-sensitively.
func Asc(name string) SortProperties {
	return SortProperties{Name: name, Dir: Ascending}
}

// Desc sorts descending by a bin, case-sensitively.
func Desc(name string) SortProperties {
	return SortProperties{Name: name, Dir: Descending}
}

// AscIgnoreCase sorts ascending by a bin, ignoring case.
func AscIgnoreCase(name string) SortProperties {
	return SortProperties{Name: name, Dir: Ascending, CaseInsensitive: true}
}

// DescIgnoreCase sorts descending by a bin, ignoring case.
func DescIgnoreCase(name string) SortProperties {
	return SortProperties{Name: name, Dir: Descending, CaseInsensitive: true}
}

// NavigatableRecordStream is an in-memory result set that can be re-sorted and
// re-paged without going back to the server.
//
// Obtain one by draining a [RecordStream] with
// [RecordStream.IntoNavigatable].
type NavigatableRecordStream struct {
	records []*RecordResult

	pageSize int
	numPages int
	// currentPage is 1-based once pagination starts; zero before it does.
	currentPage int
	index       int

	sortInfo []SortProperties
}

// NewNavigatableRecordStream wraps already-materialized rows.
func NewNavigatableRecordStream(records []*RecordResult) *NavigatableRecordStream {
	return &NavigatableRecordStream{records: records}
}

// IntoNavigatable drains the stream into a navigatable result set and closes
// the source.
func (s *RecordStream) IntoNavigatable() (*NavigatableRecordStream, error) {
	return s.IntoNavigatableLimit(0)
}

// IntoNavigatableLimit drains at most limit rows (zero means no limit) into a
// navigatable result set and closes the source.
func (s *RecordStream) IntoNavigatableLimit(limit int) (*NavigatableRecordStream, error) {
	defer s.Close()
	var rows []*RecordResult
	for limit == 0 || len(rows) < limit {
		row, err := s.Next()
		if err != nil {
			return NewNavigatableRecordStream(rows), err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return NewNavigatableRecordStream(rows), nil
}

// PageSize sets how many records make a page. Zero, the default, means one page
// holding everything. Setting it recalculates the page count and rewinds.
func (n *NavigatableRecordStream) PageSize(size int) *NavigatableRecordStream {
	if size < 0 {
		size = 0
	}
	n.pageSize = size
	if size == 0 {
		n.numPages = 1
	} else {
		n.numPages = (len(n.records) + size - 1) / size
	}
	n.Reset()
	return n
}

// SortBy replaces the sort criteria, re-sorts in place, and rewinds.
//
// Sorting is stable, so rows that compare equal on every property keep their
// original relative order.
func (n *NavigatableRecordStream) SortBy(props ...SortProperties) *NavigatableRecordStream {
	n.sortInfo = props
	if len(props) > 0 {
		sort.SliceStable(n.records, func(i, j int) bool {
			return compareRecords(n.records[i], n.records[j], props) < 0
		})
	}
	n.Reset()
	return n
}

// HasMorePages advances to the next page, reporting whether one exists.
//
// It is deliberately mutating, so the idiomatic loop is:
//
//	for nav.HasMorePages() {
//	    for nav.HasNext() { row := nav.Next() }
//	}
//
// The first call enters page one. Once the loop ends, CurrentPage is one past
// MaxPages.
func (n *NavigatableRecordStream) HasMorePages() bool {
	if n.numPages == 0 {
		n.PageSize(n.pageSize)
	}
	n.currentPage++
	return n.currentPage <= n.numPages
}

// HasNext reports whether records remain on the current page, or anywhere
// before pagination starts.
func (n *NavigatableRecordStream) HasNext() bool {
	if n.index >= len(n.records) {
		return false
	}
	if n.pageSize == 0 || n.currentPage == 0 {
		return true
	}
	return n.index/n.pageSize == n.currentPage-1
}

// Next reports the next record. It is not page-bounded on its own: gate it with
// [NavigatableRecordStream.HasNext].
func (n *NavigatableRecordStream) Next() *RecordResult {
	if n.index >= len(n.records) {
		return nil
	}
	row := n.records[n.index]
	n.index++
	return row
}

// First reports the next record on the current page.
func (n *NavigatableRecordStream) First() *RecordResult {
	if !n.HasNext() {
		return nil
	}
	return n.Next()
}

// FirstOrRaise reports the next record on the current page, erroring when the
// page is exhausted or the row failed.
func (n *NavigatableRecordStream) FirstOrRaise() (*RecordResult, error) {
	row := n.First()
	if row == nil {
		return nil, NewError(KindAerospike, "the current page holds no more records")
	}
	return row.OrRaise()
}

// CurrentPage reports the 1-based page number, zero before pagination starts.
func (n *NavigatableRecordStream) CurrentPage() int { return n.currentPage }

// MaxPages reports the total page count.
func (n *NavigatableRecordStream) MaxPages() int {
	if n.numPages == 0 {
		n.PageSize(n.pageSize)
	}
	return n.numPages
}

// SetPageTo jumps to a 1-based page.
func (n *NavigatableRecordStream) SetPageTo(page int) error {
	if page < 1 || page > n.MaxPages() {
		return NewError(KindInvalidArgument,
			"page %d is outside the range 1..%d", page, n.MaxPages())
	}
	n.currentPage = page
	if n.pageSize > 0 {
		n.index = (page - 1) * n.pageSize
	} else {
		n.index = 0
	}
	return nil
}

// Reset rewinds iteration, keeping the sort order and page size.
func (n *NavigatableRecordStream) Reset() *NavigatableRecordStream {
	n.index = 0
	n.currentPage = 0
	return n
}

// Remaining reports the not-yet-consumed rows of the current page, without
// advancing.
func (n *NavigatableRecordStream) Remaining() []*RecordResult {
	if n.index >= len(n.records) {
		return nil
	}
	end := len(n.records)
	if n.pageSize > 0 && n.currentPage > 0 {
		pageEnd := n.currentPage * n.pageSize
		if pageEnd < end {
			end = pageEnd
		}
	}
	if end <= n.index {
		return nil
	}
	return n.records[n.index:end]
}

// Records reports the whole result set in the current sort order.
func (n *NavigatableRecordStream) Records() []*RecordResult { return n.records }

// Size reports how many records the result set holds.
func (n *NavigatableRecordStream) Size() int { return len(n.records) }

// IsEmpty reports whether the result set is empty.
func (n *NavigatableRecordStream) IsEmpty() bool { return len(n.records) == 0 }

// compareRecords orders two rows by the given properties, primary first.
//
// A row whose sort bin is missing -- including a row that carries no record at
// all, as a per-key failure does -- sorts before present values ascending and
// after them descending, rather than failing.
func compareRecords(a, b *RecordResult, props []SortProperties) int {
	for _, p := range props {
		av, aOK := binValue(a, p.Name)
		bv, bOK := binValue(b, p.Name)

		var c int
		switch {
		case !aOK && !bOK:
			c = 0
		case !aOK:
			c = -1
		case !bOK:
			c = 1
		default:
			c = CompareValues(av, bv, p.CaseInsensitive)
		}
		if c == 0 {
			continue
		}
		if p.Dir == Descending {
			return -c
		}
		return c
	}
	return 0
}

// binValue reads a row's bin, reporting absence for a missing record or bin.
func binValue(r *RecordResult, name string) (any, bool) {
	if r == nil || r.Record == nil {
		return nil, false
	}
	v, ok := r.Record.Bins[name]
	return v, ok
}

// typeRank orders values across types the way the server does.
//
// The hierarchy is NIL < BOOLEAN < INTEGER < STRING < LIST < MAP < BYTES <
// DOUBLE < GEOJSON, and there is deliberately no numeric promotion between
// integers and floats: every integer sorts before every float.
func typeRank(v any) int {
	switch t := v.(type) {
	case nil:
		return 1
	case bool:
		return 2
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 3
	case string:
		return 4
	case []any:
		return 5
	case map[any]any, map[string]any, []as.MapPair:
		return 6
	case []byte:
		return 7
	case float32, float64:
		return 8
	case as.HLLValue:
		// HyperLogLog values rank with byte slices and compare by content.
		return 7
	case as.GeoJSONValue:
		return 9
	default:
		_ = t
		return 10
	}
}

// CompareValues orders two bin values using the server's type ordering.
//
// Set caseInsensitive to fold string case, which applies at every nesting
// level, including map keys. Values the server never orders fall back to a
// stable comparison of their rendered form rather than failing.
func CompareValues(a, b any, caseInsensitive bool) int {
	ra, rb := typeRank(a), typeRank(b)
	if ra != rb {
		return cmp.Compare(ra, rb)
	}

	switch ra {
	case 1: // nil
		return 0
	case 2: // bool
		av, bv := a.(bool), b.(bool)
		switch {
		case av == bv:
			return 0
		case !av:
			return -1
		default:
			return 1
		}
	case 3: // integer
		av, _ := toInt64Loose(a)
		bv, _ := toInt64Loose(b)
		return cmp.Compare(av, bv)
	case 4: // string
		av, bv := a.(string), b.(string)
		if caseInsensitive {
			av, bv = strings.ToLower(av), strings.ToLower(bv)
		}
		return cmp.Compare(av, bv)
	case 5: // list
		return compareLists(a.([]any), b.([]any), caseInsensitive)
	case 6: // map
		return compareMaps(a, b, caseInsensitive)
	case 7: // bytes and HLL, compared by content
		return bytes.Compare(rawBytes(a), rawBytes(b))
	case 8: // float
		av, _ := toFloat64Loose(a)
		bv, _ := toFloat64Loose(b)
		return cmp.Compare(av, bv)
	case 9: // GeoJSON
		return cmp.Compare(geoString(a), geoString(b))
	default:
		// Not orderable by the server: fall back to a rendered comparison so
		// the sort stays total instead of failing.
		return cmp.Compare(renderValue(a), renderValue(b))
	}
}

// compareLists compares element-wise; if every zipped element is equal, the
// shorter list sorts first.
func compareLists(a, b []any, ci bool) int {
	n := min(len(a), len(b))
	for i := range n {
		if c := CompareValues(a[i], b[i], ci); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

// compareMaps compares by length, then all keys in order, then all values.
func compareMaps(a, b any, ci bool) int {
	ak, av := sortedMapEntries(a, ci)
	bk, bv := sortedMapEntries(b, ci)
	if c := cmp.Compare(len(ak), len(bk)); c != 0 {
		return c
	}
	for i := range ak {
		if c := CompareValues(ak[i], bk[i], ci); c != 0 {
			return c
		}
	}
	for i := range av {
		if c := CompareValues(av[i], bv[i], ci); c != 0 {
			return c
		}
	}
	return 0
}

// sortedMapEntries flattens a map into keys and values, ordered by key under
// the same comparison.
func sortedMapEntries(v any, ci bool) (keys []any, values []any) {
	switch m := v.(type) {
	case map[any]any:
		for k := range m {
			keys = append(keys, k)
		}
		sort.SliceStable(keys, func(i, j int) bool { return CompareValues(keys[i], keys[j], ci) < 0 })
		for _, k := range keys {
			values = append(values, m[k])
		}
	case map[string]any:
		for k := range m {
			keys = append(keys, k)
		}
		sort.SliceStable(keys, func(i, j int) bool { return CompareValues(keys[i], keys[j], ci) < 0 })
		for _, k := range keys {
			values = append(values, m[k.(string)])
		}
	case []as.MapPair:
		for _, p := range m {
			keys = append(keys, p.Key)
		}
		sort.SliceStable(keys, func(i, j int) bool { return CompareValues(keys[i], keys[j], ci) < 0 })
		byKey := map[any]any{}
		for _, p := range m {
			byKey[p.Key] = p.Value
		}
		for _, k := range keys {
			values = append(values, byKey[k])
		}
	}
	return keys, values
}

// rawBytes extracts the byte content of a blob or HyperLogLog value.
func rawBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case as.HLLValue:
		return []byte(t)
	}
	return nil
}

// geoString extracts a GeoJSON document string.
func geoString(v any) string {
	if g, ok := v.(as.GeoJSONValue); ok {
		return string(g)
	}
	return renderValue(v)
}

// toInt64Loose widens any integral value.
func toInt64Loose(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	}
	return 0, false
}

// toFloat64Loose widens any floating-point value.
func toFloat64Loose(v any) (float64, bool) {
	switch t := v.(type) {
	case float32:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

// renderValue is the last-resort stable rendering for unorderable values.
func renderValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return sprint(v)
}

// TypedNavigatableRecordStream is the typed face of
// [NavigatableRecordStream].
type TypedNavigatableRecordStream[T any] struct {
	inner *NavigatableRecordStream
}

// IntoNavigatable drains a typed stream into a typed navigatable result set.
func (t *TypedRecordStream[T]) IntoNavigatable() (*TypedNavigatableRecordStream[T], error) {
	nav, err := t.inner.IntoNavigatable()
	return &TypedNavigatableRecordStream[T]{inner: nav}, err
}

// Untyped reports the underlying navigatable result set.
func (t *TypedNavigatableRecordStream[T]) Untyped() *NavigatableRecordStream { return t.inner }

// PageSize sets the page size.
func (t *TypedNavigatableRecordStream[T]) PageSize(size int) *TypedNavigatableRecordStream[T] {
	t.inner.PageSize(size)
	return t
}

// SortBy replaces the sort criteria and re-sorts.
func (t *TypedNavigatableRecordStream[T]) SortBy(props ...SortProperties) *TypedNavigatableRecordStream[T] {
	t.inner.SortBy(props...)
	return t
}

// HasMorePages advances to the next page, reporting whether one exists.
func (t *TypedNavigatableRecordStream[T]) HasMorePages() bool { return t.inner.HasMorePages() }

// HasNext reports whether records remain on the current page.
func (t *TypedNavigatableRecordStream[T]) HasNext() bool { return t.inner.HasNext() }

// CurrentPage reports the 1-based page number.
func (t *TypedNavigatableRecordStream[T]) CurrentPage() int { return t.inner.CurrentPage() }

// MaxPages reports the total page count.
func (t *TypedNavigatableRecordStream[T]) MaxPages() int { return t.inner.MaxPages() }

// SetPageTo jumps to a 1-based page.
func (t *TypedNavigatableRecordStream[T]) SetPageTo(page int) error { return t.inner.SetPageTo(page) }

// Reset rewinds iteration.
func (t *TypedNavigatableRecordStream[T]) Reset() *TypedNavigatableRecordStream[T] {
	t.inner.Reset()
	return t
}

// Size reports how many records the result set holds.
func (t *TypedNavigatableRecordStream[T]) Size() int { return t.inner.Size() }

// IsEmpty reports whether the result set is empty.
func (t *TypedNavigatableRecordStream[T]) IsEmpty() bool { return t.inner.IsEmpty() }

// NextObject reports the next record on the current page, mapped to T.
func (t *TypedNavigatableRecordStream[T]) NextObject() (*T, error) {
	row := t.inner.First()
	if row == nil {
		return nil, nil
	}
	return objectFromRecord[T](row)
}

// Objects reports the current page's remaining rows as entities.
func (t *TypedNavigatableRecordStream[T]) Objects() ([]*T, error) {
	rows := t.inner.Remaining()
	out := make([]*T, 0, len(rows))
	for _, r := range rows {
		obj, err := objectFromRecord[T](r)
		if err != nil {
			return out, err
		}
		out = append(out, obj)
	}
	// The rows are consumed by reading them.
	for range rows {
		t.inner.Next()
	}
	return out, nil
}

// sprint renders a value for the last-resort comparison of types the server
// does not order.
func sprint(v any) string {
	return fmt.Sprintf("%v", v)
}

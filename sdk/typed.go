package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// TypedDataSet[T] and the free functions below are the only generic
// surface for typed mapping (D-28): no QueryTyped[T]/UpsertTyped[T] on
// Session. Typed code gets a *DataSet via .DataSet() and calls the plain,
// non-generic Session methods (Query/Scan/BatchGet/...), then uses
// Collect[T]/Decode to map results back.
type TypedDataSet[T any] struct{}

func NewTypedDataSet[T any](ns, set string) (*TypedDataSet[T], error) {
	return nil, nil
}

func (d *TypedDataSet[T]) DataSet() *DataSet {
	return nil
}

// The PRD also lists (d *TypedDataSet[T]) Key[K KeyValue](id K) *as.Key /
// Keys — deliberately not implemented here: same problem as
// DataSet.Key[K] (dataset.go) had before it was made a free function —
// TypedDataSet[T]'s receiver already carries [T], so a method adding its
// own [K] is invalid Go regardless. Typed code gets a *DataSet via
// .DataSet() above and calls the free-standing sdk.Key[K](d, id) instead.

// Collect reads every remaining result from s and decodes each into T. It
// does not close s (D-17: always Close, or a documented terminal that
// does) — the caller still owns Close, the same as consuming via Iter.
func Collect[T any](s *ReadStream) ([]T, error) {
	return nil, nil
}

func (r *Record) Decode(dest any) error {
	return nil
}

func Decode[T any](r *Record) (T, error) {
	var zero T
	return zero, nil
}

// Marshal, Decode and Record.Decode all use the `as:"..."` struct tag
// convention shown in the PRD's own 10.10 Look example: `as:",key"` marks
// the field mapped to/from the record key (excluded from the returned
// BinMap on Marshal, populated from the key on Decode); `as:"binname"`
// gives every other field's bin name.
func Marshal[T any](v T) (as.BinMap, error) {
	return nil, nil
}

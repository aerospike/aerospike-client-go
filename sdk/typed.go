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

func Marshal[T any](v T) (as.BinMap, error) {
	return nil, nil
}

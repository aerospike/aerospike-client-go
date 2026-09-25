package sdk

import (
	"context"
	"iter"

	as "github.com/aerospike/aerospike-client-go/v8"
)

type ReadResult struct{}

func (r *ReadResult) Record() (*Record, error) {
	return nil, nil
}

type WriteResult struct {
	Key        *as.Key
	Generation Generation
	Affected   bool
	ResultCode int
}

type UDFResult struct {
	Key    *as.Key
	Return any
}

type ReadStream struct{}

func (s *ReadStream) Next(ctx context.Context) (*ReadResult, error) {
	return nil, nil
}

func (s *ReadStream) Iter(ctx context.Context) iter.Seq2[*ReadResult, error] {
	return func(yield func(*ReadResult, error) bool) {}
}

func (s *ReadStream) Err() error {
	return nil
}

func (s *ReadStream) Close() error {
	return nil
}

func (s *ReadStream) One(ctx context.Context) (*Record, error) {
	return nil, nil
}

func (s *ReadStream) Collect(ctx context.Context) ([]*Record, error) {
	return nil, nil
}

func (s *ReadStream) Failures() ([]*ReadResult, error) {
	return nil, nil
}

func (s *ReadStream) HasMoreChunks() (bool, error) {
	return false, nil
}

func (s *ReadStream) IntoNavigatable() (*NavigatableStream, error) {
	return nil, nil
}

type WriteStream struct{}

func (s *WriteStream) Next(ctx context.Context) (*WriteResult, error) {
	return nil, nil
}

// Iter mirrors ReadStream.Iter — 10.14 only spells this out for ReadStream,
// but WriteStream needs the same range-over-func consumption path for the
// same reason: Next's exhaustion signal isn't otherwise defined (no
// documented terminal error/sentinel), so a caller can't safely loop on
// Next alone.
func (s *WriteStream) Iter(ctx context.Context) iter.Seq2[*WriteResult, error] {
	return func(yield func(*WriteResult, error) bool) {}
}

func (s *WriteStream) Err() error {
	return nil
}

func (s *WriteStream) Close() error {
	return nil
}

// NavigatableStream is returned by ReadStream.IntoNavigatable for
// sort/page-based access. NOTE: as specified in the PRD, none of
// SortBy/PageSize/HasMorePages/HasNext/Next take ctx, and Next returns no
// error — a known open gap (pagination almost certainly round-trips to the
// server), not an oversight in this stub.
type NavigatableStream struct{}

func (n *NavigatableStream) SortBy(sorters ...SortSpec) *NavigatableStream {
	return n
}

func (n *NavigatableStream) PageSize(size int) *NavigatableStream {
	return n
}

func (n *NavigatableStream) HasMorePages() bool {
	return false
}

func (n *NavigatableStream) HasNext() bool {
	return false
}

func (n *NavigatableStream) Next() *ReadResult {
	return nil
}

type SortSpec struct{}

func Desc(bin string) SortSpec {
	return SortSpec{}
}

func Asc(bin string) SortSpec {
	return SortSpec{}
}

// Task is the one wait-model handle for async admin/background operations
// (D-20) — both index Create and query background execution return this
// same type rather than two differently-named handles.
type Task struct{}

func (t *Task) Wait(ctx context.Context) error {
	return nil
}

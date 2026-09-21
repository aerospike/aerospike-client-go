package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// QueryBuilder is returned by Session.Query and Session.Scan (10.7) — the
// two entry points share this one builder type; Query filters, Scan doesn't.
type QueryBuilder struct{}

func (b *QueryBuilder) Bins(names ...string) *QueryBuilder {
	return b
}

func (b *QueryBuilder) WithNoBins() *QueryBuilder {
	return b
}

func (b *QueryBuilder) WhereAEL(src string) *QueryBuilder {
	return b
}

func (b *QueryBuilder) Where(exp *as.Expression) *QueryBuilder {
	return b
}

func (b *QueryBuilder) DefaultWhere(pred func(*Record) bool) *QueryBuilder {
	return b
}

func (b *QueryBuilder) Filter(f *as.Filter) *QueryBuilder {
	return b
}

func (b *QueryBuilder) Partition(pf *as.PartitionFilter) *QueryBuilder {
	return b
}

func (b *QueryBuilder) OnPartition(id int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) OnPartitionRange(begin, count int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) Limit(n int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) MaxRecords(n int64) *QueryBuilder {
	return b
}

func (b *QueryBuilder) RecordsPerSecond(n int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) ChunkSize(n int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) IncludeMissingKeys() *QueryBuilder {
	return b
}

func (b *QueryBuilder) ReadTouchTTLPercent(pct int) *QueryBuilder {
	return b
}

func (b *QueryBuilder) OnBin(name string) *QueryBinBuilder {
	return nil
}

func (b *QueryBuilder) WithWriteOperations(ops ...*as.Operation) *QueryBuilder {
	return b
}

// -- Terminals --

func (b *QueryBuilder) Execute() (*ReadStream, error) {
	return nil, nil
}

func (b *QueryBuilder) ExecuteOnError(onErr *OnError) (*ReadStream, error) {
	return nil, nil
}

func (b *QueryBuilder) Stream() (*ReadStream, error) {
	return nil, nil
}

func (b *QueryBuilder) StreamOnError(onErr *OnError) (*ReadStream, error) {
	return nil, nil
}

// -- Background terminals --

func (b *QueryBuilder) ExecuteBackgroundTask() (*Task, error) {
	return nil, nil
}

func (b *QueryBuilder) ExecuteBackgroundDelete() (*Task, error) {
	return nil, nil
}

func (b *QueryBuilder) ExecuteBackgroundTouch() (*Task, error) {
	return nil, nil
}

func (b *QueryBuilder) ExecuteUDFBackgroundTask(pkg, fn string, args ...any) (*Task, error) {
	return nil, nil
}

// QueryBinBuilder is entered via QueryBuilder.OnBin. The PRD gives no
// further detail on its methods beyond the entry point, so this is
// intentionally minimal pending a real example that needs more.
type QueryBinBuilder struct{}

func (b *QueryBinBuilder) SelectAs(name string) *QueryBuilder {
	return nil
}

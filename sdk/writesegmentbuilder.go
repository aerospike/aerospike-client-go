package sdk

import (
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// WriteSegmentBuilder is the DSL entry point returned by Session.Upsert,
// Insert, Update, Replace and ReplaceIfExists — reserved for CDT/operate/
// filter writes that a flat struct can't express (D-25).
type WriteSegmentBuilder struct{}

// -- Segment modifiers (10.5) --

func (b *WriteSegmentBuilder) WhereAEL(src string) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Where(exp *as.Expression) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) ExpireAfter(d time.Duration) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) ExpireAt(t time.Time) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) NeverExpire() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) KeepTTL() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) ExpiryFromServerDefault() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) IfGeneration(gen Generation) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) DurableDelete() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) OmitDurableDelete() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) InTransaction(tx *TransactionalSession) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) IncludeMissingKeys() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) FailOnFilteredOut() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Set(name string, v any) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) SetBinsTo(names []string, values []any) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Put(bins ...*as.Bin) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Add(name string, delta any) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Append(name string, v any) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Prepend(name string, v any) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) Get(name string) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) RemoveBin(name string) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) DeleteRecord() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) TouchRecord() *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) AddOperation(op *as.Operation) *WriteSegmentBuilder {
	return b
}

func (b *WriteSegmentBuilder) OnBin(name string) *WriteBinBuilder {
	return nil
}

// -- Terminals (10.5) --

func (b *WriteSegmentBuilder) Execute() (*WriteStream, error) {
	return nil, nil
}

func (b *WriteSegmentBuilder) ExecuteOnError(onErr *OnError) (*WriteStream, error) {
	return nil, nil
}

func (b *WriteSegmentBuilder) ExecuteOne() (WriteResult, error) {
	return WriteResult{}, nil
}

func (b *WriteSegmentBuilder) Stream() (*WriteStream, error) {
	return nil, nil
}

func (b *WriteSegmentBuilder) StreamOnError(onErr *OnError) (*WriteStream, error) {
	return nil, nil
}

// WriteBinBuilder is entered via WriteSegmentBuilder.OnBin (10.15): scalar,
// CDT navigation, path-expression and HLL/bitwise/string ops all live here.
type WriteBinBuilder struct{}

// -- Scalar --

func (b *WriteBinBuilder) SetTo(value any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) Add(value any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) Get() *WriteSegmentBuilder {
	return nil
}

// -- CDT navigation: map --

func (b *WriteBinBuilder) OnMapKey(key any) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapIndex(index int) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapRank(rank int) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapValue(value any) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapKeyRange(begin, end any) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapValueRange(begin, end any) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnMapKeyRelativeIndexRange(key any, offset, count int) *CDTNavBuilder {
	return nil
}

// -- CDT navigation: list --

func (b *WriteBinBuilder) OnListIndex(index int) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnListRank(rank int) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnListValue(value any) *CDTNavBuilder {
	return nil
}

func (b *WriteBinBuilder) OnListIndexRange(begin, count int) *CDTNavBuilder {
	return nil
}

// -- Map/list whole-collection ops --

func (b *WriteBinBuilder) MapUpsertItems(items map[any]any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) MapSize() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) MapSetPolicy(policy any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) MapClear() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListAppendItems(items []any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListTrim(begin, count int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListSort() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListSize() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListSet(index int, v any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListRemoveRange(begin, count int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListRemove(index int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListPop(index int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListInsertItems(index int, items []any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListInsert(index int, v any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListIncrement(index int, delta any) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListGet(index int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListGetRange(begin, count int) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ListClear() *WriteSegmentBuilder {
	return nil
}

// -- Path expressions (server 8.1.1+) --

func (b *WriteBinBuilder) OnEachChild() *WriteBinBuilder {
	return b
}

func (b *WriteBinBuilder) OnEachChildWhere(pred *as.Expression) *WriteBinBuilder {
	return b
}

func (b *WriteBinBuilder) NoFail() *WriteBinBuilder {
	return b
}

func (b *WriteBinBuilder) CollectValues() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) CollectTree() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) CollectKeys() *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) ModifyBy(exp *as.Expression) *WriteSegmentBuilder {
	return nil
}

func (b *WriteBinBuilder) RemoveMatches() *WriteSegmentBuilder {
	return nil
}

// CDTNavBuilder is entered via a map/list navigation call (OnMapKey,
// OnListIndexRange, etc.) — the terminal vocabulary is shared across map and
// list navigation (10.15).
type CDTNavBuilder struct{}

func (b *CDTNavBuilder) SetTo(value any) *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) Insert(value any) *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) Update(value any) *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) Add(value any) *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) GetValues() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) GetKeys() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) Count() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) Remove() *WriteSegmentBuilder {
	return nil
}

// RemoveAnd removes the matched entries and returns a builder so a further
// terminal (Count, GetValues, ...) reports what was removed.
func (b *CDTNavBuilder) RemoveAnd() *CDTNavBuilder {
	return b
}

func (b *CDTNavBuilder) GetAllOtherValues() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) GetAllOtherKeys() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) GetAsOrderedMap() *WriteSegmentBuilder {
	return nil
}

func (b *CDTNavBuilder) GetExists() *WriteSegmentBuilder {
	return nil
}

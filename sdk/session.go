package sdk

import (
	"context"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

type Session struct{}

// AllBins is a named nil []string for "no bin projection, read every bin"
// — used in the PRD's own worked examples (session.Get(ctx, key,
// sdk.AllBins), session.BatchGet(ctx, keys, sdk.AllBins)) as the
// self-documenting alternative to a bare nil. A plain nil still means the
// same thing (10.4's own Look uses nil directly with a "// all bins"
// comment) — the two forms are equivalent, not two different behaviors.
var AllBins []string

// GAP: Record is completely opaque — no bin names, no generation, no
// expiration, no way to introspect what Decode will actually read.
// IfGeneration(gen Generation) (10.5) conditions a write on a generation
// value, and the PRD's own Look example for it calls rec.Gen() — implying
// Record should expose one, as a method (not a field) — but nothing in
// the catalog actually defines Record's fields/methods anywhere, and
// nothing at all suggests it exposes expiration. A caller has no
// PRD-defined way to do a read-then-conditionally-write pattern, or to
// read a record's current TTL back, until the PRD says what Record
// actually looks like. (This was previously "fixed" by adding
// Generation/Expiration fields directly — reverted: that shape was
// invented, not specified, and don't-invent-beyond-the-PRD applies to
// sdk/ types as much as to methods.)
type Record struct{}

// -- 10.3 Session accessors --

func (s *Session) Behavior() *Behavior {
	return nil
}

func (s *Session) Client() *as.Client {
	return nil
}

func (s *Session) CurrentTransaction() *as.Txn {
	return nil
}

func (s *Session) SessionFor(ctx context.Context, b *Behavior) (*Session, error) {
	return nil, nil
}

// -- 10.4 Point operations --

func (s *Session) Get(ctx context.Context, key *as.Key, bins []string) (*Record, error) {
	return nil, nil
}

func (s *Session) GetHeader(ctx context.Context, key *as.Key) (*Record, error) {
	return nil, nil
}

func (s *Session) Put(ctx context.Context, key *as.Key, bins as.BinMap) error {
	return nil
}

func (s *Session) Delete(ctx context.Context, key *as.Key) error {
	return nil
}

func (s *Session) Exists(ctx context.Context, key *as.Key) (bool, error) {
	return false, nil
}

func (s *Session) Touch(ctx context.Context, key *as.Key) error {
	return nil
}

func (s *Session) Truncate(ctx context.Context, ds *DataSet, before time.Time) error {
	return nil
}

// -- 10.5 Write verbs (DSL — CDT/operate/filters) --

func (s *Session) Upsert(ctx context.Context, key *as.Key) *WriteSegmentBuilder {
	return nil
}

func (s *Session) Insert(ctx context.Context, key *as.Key) *WriteSegmentBuilder {
	return nil
}

func (s *Session) Update(ctx context.Context, key *as.Key) *WriteSegmentBuilder {
	return nil
}

func (s *Session) Replace(ctx context.Context, key *as.Key) *WriteSegmentBuilder {
	return nil
}

func (s *Session) ReplaceIfExists(ctx context.Context, key *as.Key) *WriteSegmentBuilder {
	return nil
}

// -- 10.6 Mixed/multi-key writes --

func (s *Session) BatchWrite(ctx context.Context, ops []WriteOp) (*WriteStream, error) {
	return nil, nil
}

// -- 10.7 Reads: BatchGet, Query, Scan --

func (s *Session) BatchGet(ctx context.Context, keys []*as.Key, bins []string) (*ReadStream, error) {
	return nil, nil
}

func (s *Session) Query(ctx context.Context, ds *DataSet) *QueryBuilder {
	return nil
}

func (s *Session) Scan(ctx context.Context, ds *DataSet) *QueryBuilder {
	return nil
}

// -- 10.12 Transactions --

func (s *Session) BeginTransaction(ctx context.Context) (*TransactionalSession, error) {
	return nil, nil
}

func (s *Session) BeginTransactionTimeout(ctx context.Context, d time.Duration) (*TransactionalSession, error) {
	return nil, nil
}

func (s *Session) RunInTransaction(ctx context.Context, fn func(*Session) error, opts ...TxnOption) error {
	return nil
}

// -- 10.13 Indexes and info --

func (s *Session) Index(ctx context.Context, ds *DataSet) *IndexBuilder {
	return nil
}

func (s *Session) ListIndexes(ctx context.Context) ([]IndexInfo, error) {
	return nil, nil
}

func (s *Session) Info(ctx context.Context, command string) (map[string]string, error) {
	return nil, nil
}

func (s *Session) InfoOnAllNodes(ctx context.Context, command string) (map[string]map[string]string, error) {
	return nil, nil
}

func (s *Session) InfoCommands() *InfoCommands {
	return nil
}

func (s *Session) IsNamespaceSC(ctx context.Context, ns string) (bool, error) {
	return false, nil
}

func (s *Session) NamespaceScStatus(ctx context.Context, ns string) (string, error) {
	return "", nil
}

// -- 10.11 UDF --

func (s *Session) RegisterUDF(ctx context.Context, u UDF) (*UDFModule, error) {
	return nil, nil
}

func (s *Session) RemoveUDF(ctx context.Context, name string) error {
	return nil
}

func (s *Session) ListUDF(ctx context.Context) ([]UDFInfo, error) {
	return nil, nil
}

func (s *Session) ExecuteUDF(ctx context.Context, key *as.Key) *UDFFunctionBuilder {
	return nil
}

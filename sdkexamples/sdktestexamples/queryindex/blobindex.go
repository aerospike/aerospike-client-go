package queryindex

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

const (
	blobIndexName = "qbindex"
	blobBinName   = "bb"
	blobSize      = 5
)

// blobBytesFor mirrors the source Java test's Buffer.longToBytes(50000+i,
// bytes, 0): an 8-byte big-endian encoding of a long value. Ordinary Go
// standard-library code building test data, not sdk/ surface — the
// source Java equivalent (com.aerospike.client.sdk.command.Buffer) is
// the same kind of internal utility, not public SDK capability either.
func blobBytesFor(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

// DemonstrateBlobIndex mirrors the source Java test file
// QueryBlobTest.java (active, 2 tests: queryBlob, queryBlobInList):
// create a blob secondary index, seed 5 records with both a scalar blob
// bin and a list-of-one-blob bin, then run both filter queries — one
// matching the scalar blob directly, one matching a blob inside a list.
//
// GAP: the source test creates a *second* index — qblist, IndexType.BLOB
// with IndexCollectionType.LIST, on binNameList — confirming
// Collection(t CollectionType) is real for the LIST collection type too.
// Not built: sdk/index.go's CollectionType has zero defined values
// (sdk/FUNCTIONAL_GAPS.md finding #27), so there is nothing to pass it.
// The list-blob-contains query itself (queryBlobInList, below) doesn't
// actually require that index to exist — AEL filtering can match a
// list's contents without a collection index accelerating it — so it's
// still demonstrated; only the second Create call is skipped.
//
// GAP: can't verify the query actually matched or what came back —
// Record (sdk/session.go) has no bin-accessor methods at all (finding
// #16). Both queries here can only confirm the call completes.
//
// GAP: same as DemonstrateStringIndex — the source test's
// INDEX_ALREADY_EXISTS retry-and-ignore pattern has no PRD-grounded
// sentinel error to check against (sdk/errors.go has none), so it isn't
// reproduced here either.
func (s *Service) DemonstrateBlobIndex(ctx context.Context) error {
	task, err := s.session.Index(ctx, s.ds).
		OnBin(blobBinName).
		Named(blobIndexName).
		Blob().
		Create(ctx)
	if err != nil {
		return fmt.Errorf("create blob index %s: %w", blobIndexName, err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for blob index %s: %w", blobIndexName, err)
	}

	const binNameList = "bblist"
	for i := int64(1); i <= blobSize; i++ {
		key := sdk.Key(s.ds, i)
		bytes := blobBytesFor(50000 + i)
		list := []any{bytes}
		if _, err := s.session.Upsert(ctx, key).
			SetBinsTo([]string{blobBinName, binNameList}, []any{bytes, list}).
			ExecuteOne(); err != nil {
			return fmt.Errorf("seed record %d: %w", i, err)
		}
	}

	target := blobBytesFor(50003)
	hexStr := hex.EncodeToString(target)

	// queryBlob: match the scalar blob bin directly.
	scalarAEL := fmt.Sprintf("$.%s == x'%s'", blobBinName, hexStr)
	if _, err := s.session.Query(ctx, s.ds).
		Bins(blobBinName).
		WhereAEL(scalarAEL).
		Execute(); err != nil {
		return fmt.Errorf("query blob by scalar match: %w", err)
	}

	// queryBlobInList: match a blob inside the list bin.
	listAEL := fmt.Sprintf("$.%s.[=X'%s'].get(return: EXISTS) == true", binNameList, hexStr)
	if _, err := s.session.Query(ctx, s.ds).
		Bins(blobBinName, binNameList).
		WhereAEL(listAEL).
		Execute(); err != nil {
		return fmt.Errorf("query blob by list-contains match: %w", err)
	}

	if err := s.session.Index(ctx, s.ds).
		Named(blobIndexName).
		Drop(ctx); err != nil {
		return fmt.Errorf("drop blob index %s: %w", blobIndexName, err)
	}

	fmt.Printf("blob index %s: created, queried both ways, dropped (can't verify matches — see GAP comment)\n", blobIndexName)
	return nil
}

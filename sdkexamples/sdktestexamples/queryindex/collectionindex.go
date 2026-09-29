package queryindex

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

const (
	collectionKeyPrefix   = "qkey"
	collectionMapKeyPfx   = "mkey"
	collectionMapValuePfx = "qvalue"
	collectionBinName     = "map_bin"
	collectionSize        = 20
)

// DemonstrateCollectionIndex mirrors the source Java test file
// QueryCollectionTest.java (active, 1 test: queryCollection): seed 20
// records with a map bin (each always holding "mkey1", holding "mkey2"
// when the record index is even, and "mkey3" when it's a multiple of
// 3), then run a filter query checking for the presence of "mkey2".
//
// GAP: the source test first creates a MAPKEYS-collection secondary
// index (IndexType.STRING, IndexCollectionType.MAPKEYS) on the map bin.
// Not built: sdk/index.go's CollectionType has zero defined values
// (sdk/FUNCTIONAL_GAPS.md finding #27), so there is nothing to pass
// Collection(t) — this is the first file in this package where the
// *only* real index shape the source test needs is one CollectionType
// can't express at all (QueryBlobTest.java's LIST-collection index at
// least had a DEFAULT-collection sibling to build instead). The
// map-key-existence filter query itself doesn't actually require that
// index to exist — AEL filtering can match without a collection index
// accelerating it — so it's still demonstrated; only the Create/Drop
// calls are skipped entirely, not adapted.
//
// GAP: can't verify the query actually matched or what came back —
// Record (sdk/session.go) has no bin-accessor methods at all (finding
// #16). This can only confirm the call completes.
func (s *Service) DemonstrateCollectionIndex(ctx context.Context) error {
	for i := 1; i <= collectionSize; i++ {
		key := sdk.Key(s.ds, fmt.Sprintf("%s%d", collectionKeyPrefix, i))

		m := map[string]any{
			collectionMapKeyPfx + "1": fmt.Sprintf("%s%d", collectionMapValuePfx, i),
		}
		if i%2 == 0 {
			m[collectionMapKeyPfx+"2"] = fmt.Sprintf("%s%d", collectionMapValuePfx, i)
		}
		if i%3 == 0 {
			m[collectionMapKeyPfx+"3"] = fmt.Sprintf("%s%d", collectionMapValuePfx, i)
		}

		if _, err := s.session.Upsert(ctx, key).
			Set(collectionBinName, m).
			ExecuteOne(); err != nil {
			return fmt.Errorf("seed record %d: %w", i, err)
		}
	}

	queryMapKey := collectionMapKeyPfx + "2"
	ael := fmt.Sprintf("$.%s.%s.get(return: EXISTS) == true", collectionBinName, queryMapKey)

	if _, err := s.session.Query(ctx, s.ds).
		WhereAEL(ael).
		Execute(); err != nil {
		return fmt.Errorf("query collection for map key %s: %w", queryMapKey, err)
	}

	fmt.Printf("collection query: filtered on map key %s presence (no index — see GAP comment; can't verify matches)\n", queryMapKey)
	return nil
}

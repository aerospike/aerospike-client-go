package commonexamples

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstratePointShortcuts exercises the three single-key shortcuts (10.4)
// that neither ecommerce nor queryexamples calls directly: Exists, Touch,
// and Delete as plain Session methods, distinct from the CDT/operate
// builders both other packages use for everything.
//
// GAP: this only exercises the happy path — id always exists when Exists,
// Touch, and Delete are each called, matching the source Java example's
// own single-record section (id 113, seeded earlier in that file, never
// missing). The source example wraps each of these three calls in
// Optional<Boolean> with explicit ifPresentOrElse(..., "unexpected")
// handling, implying Touch/Delete carry a not-found signal too, not just
// Exists. But D-7 (Not-found) only pins down Get/BatchGet's missing-key
// behavior as ErrNotFound — it says nothing about what Touch or Delete do
// on a key that doesn't exist: silently succeed as an idempotent no-op,
// return ErrNotFound, or something else. Exists's own not-found shape is
// unambiguous ((false, nil), demonstrated below), but Touch/Delete's is a
// real, undocumented gap — not something to demonstrate by guessing at a
// behavior the PRD never specifies.
func (s *Service) DemonstratePointShortcuts(ctx context.Context, id int64) error {
	key := sdk.Key(s.ds, id)

	if err := s.session.Put(ctx, key, as.BinMap{nameBin: "Charlie", ageBin: 11}); err != nil {
		return fmt.Errorf("seed record %d: %w", id, err)
	}

	exists, err := s.session.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("check exists %d: %w", id, err)
	}
	fmt.Printf("record %d exists: %v\n", id, exists)

	if err := s.session.Touch(ctx, key); err != nil {
		return fmt.Errorf("touch record %d: %w", id, err)
	}
	fmt.Printf("record %d touched\n", id)

	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete record %d: %w", id, err)
	}

	exists, err = s.session.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("check exists after delete %d: %w", id, err)
	}
	fmt.Printf("record %d exists after delete: %v\n", id, exists)
	return nil
}

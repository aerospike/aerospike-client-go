package operate

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateDeleteRecord mirrors the source Java test operateDeleteRecord():
// read a bin, then delete the whole record, atomically, in one call; verify
// deletion; rewrite the record; then read a bin, delete the record, and
// write a fresh bin, atomically, in one call — an atomic delete-and-recreate.
//
// GAP: Java's test reads the fetched bin values back from the operation's
// own result (rec.getLong(binName1)) to confirm exactly what was deleted.
// Can't be done here: ExecuteOne() returns (WriteResult, error), and
// WriteResult has no value field at all — the same limitation already
// documented in ecommerce/products.go's RecordProductRatings GAP,
// cookbookexamples/onetomany's DeleteListing, and
// cookbookexamples/leaderboard's GetScoresAroundPlayer. What this
// demonstrates instead is only what's independently verifiable:
// Session.Exists confirms the record is actually gone (a genuine,
// working check, not degraded), and confirms it exists again afterward
// — but not the bin values themselves.
func (s *Service) DemonstrateDeleteRecord(ctx context.Context, id int64) error {
	key := sdk.Key(s.ds, id)

	if _, err := s.session.Upsert(ctx, key).
		Set(binName1, int64(1)).
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %d: %w", id, err)
	}

	// DX GAP: this call shape reads confusingly — Upsert() suggests
	// create-or-update, but the chain ends in DeleteRecord(), deleting
	// the whole record. Not a naming slip: every WriteSegmentBuilder
	// chain must start with one of Upsert/Insert/Update/Replace/
	// ReplaceIfExists (the whole call's record-exists policy), even when
	// the operation embedded in it is a delete — the entry verb is
	// effectively vestigial here, a policy choice the API forces even
	// when it doesn't describe what's actually happening. Real evidence
	// this is genuinely confusing, not just a reaction to it: the source
	// Java test doesn't call this inline either — it wraps the identical
	// shape in a specially-named helper,
	// upsertForScDurableRecordDelete(key), specifically to explain why an
	// "upsert" call is really a delete. If the SDK's own test authors
	// felt the raw call needed an explanatory wrapper, that's a real
	// signal the shape itself is non-obvious, not something unique to Go.
	//
	// Read the bin, then delete the whole record, atomically, in one call.
	if _, err := s.session.Upsert(ctx, key).
		DurableDelete().
		Get(binName1).
		DeleteRecord().
		ExecuteOne(); err != nil {
		return fmt.Errorf("read+delete record %d: %w", id, err)
	}

	exists, err := s.session.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("check exists after delete %d: %w", id, err)
	}
	if exists {
		return fmt.Errorf("expected record %d to be deleted, but it still exists", id)
	}
	fmt.Printf("record %d confirmed deleted\n", id)

	// Rewrite, then delete-and-recreate atomically in one call.
	if _, err := s.session.Insert(ctx, key).
		Set(binName1, int64(1)).
		Set(binName2, int64(2)).
		ExecuteOne(); err != nil {
		return fmt.Errorf("rewrite %s/%s on record %d: %w", binName1, binName2, id, err)
	}

	// DX GAP: worse than the read+delete call above — here DeleteRecord()
	// is followed by Set(binName2, ...) in the very same chain, which
	// reads like it resurrects the record it just deleted. It doesn't:
	// this is the server treating the whole chain as one atomic
	// operate — delete, then write fresh bins, in a single round trip —
	// but nothing about the call shape signals that. A reader has to
	// already know the semantics to trust that Set after DeleteRecord
	// isn't a bug or a no-op. Same root cause as the entry-verb
	// mismatch above (Upsert() starting a chain that deletes): the
	// builder has no vocabulary for "atomically replace this record's
	// entire contents," so that intent has to be spelled out as
	// delete-then-write and inferred by the reader from op order alone.
	if _, err := s.session.Upsert(ctx, key).
		DurableDelete().
		Get(binName1).
		DeleteRecord().
		Set(binName2, int64(2)).
		Get(binName2).
		ExecuteOne(); err != nil {
		return fmt.Errorf("delete-and-recreate record %d: %w", id, err)
	}

	exists, err = s.session.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("check exists after delete-and-recreate %d: %w", id, err)
	}
	if !exists {
		return fmt.Errorf("expected record %d to exist after delete-and-recreate, but it doesn't", id)
	}
	fmt.Printf("record %d confirmed recreated (can't verify %s is gone and %s=2 — see GAP comment)\n", id, binName1, binName2)
	return nil
}

const binName2 = "optintbin2"

// DemonstrateTouchRecord mirrors the source Java test operateTouchRecord():
// write a record with a short TTL, then read a bin and touch the whole
// record (resetting its TTL) atomically, in one call.
//
// GAP: same limitation as DemonstrateDeleteRecord — can't read the
// fetched bin value back from the WriteResult. Also can't verify the TTL
// was actually refreshed: Record (sdk/session.go) is completely opaque,
// no expiration field or method anywhere in the PRD (already documented,
// sdk/FUNCTIONAL_GAPS.md finding #16 — this was previously worked around
// by adding an Expiration field directly to Record, reverted after being
// flagged as an unauthorized invention). So this can only confirm the
// combined read+touch call was accepted, not what it actually read or
// changed.
func (s *Service) DemonstrateTouchRecord(ctx context.Context, id int64) error {
	key := sdk.Key(s.ds, id)

	if _, err := s.session.Upsert(ctx, key).
		Set(binName1, int64(42)).
		ExpireAfter(60 * time.Second).
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %d with a 60s TTL: %w", id, err)
	}

	// DX GAP: Get(binName1) here has no observable effect at all — this
	// function's own doc comment above already says WriteResult can't
	// carry the fetched value back. So this line compiles, matches the
	// source Java shape, and does genuinely nothing a caller of this Go
	// function can ever see. Without already knowing that backstory, it
	// reads like dead code someone forgot to remove, not a deliberate
	// choice to mirror the source. Related, smaller ambiguity in the
	// same chain: TouchRecord() and ExpireAfter(120s) both touch the
	// record's TTL-adjacent state in one call, but the PRD (10.5) never
	// says what "touch" means once an explicit new TTL is also given in
	// the same operate — does TouchRecord do anything once ExpireAfter
	// is present, or is it redundant here? Left as-is (matching the
	// source call shape) rather than guessed at.
	if _, err := s.session.Upsert(ctx, key).
		Get(binName1).
		TouchRecord().
		ExpireAfter(120 * time.Second).
		ExecuteOne(); err != nil {
		return fmt.Errorf("read+touch record %d: %w", id, err)
	}
	fmt.Printf("record %d touched (can't verify the read value or refreshed TTL — see GAP comment)\n", id)
	return nil
}

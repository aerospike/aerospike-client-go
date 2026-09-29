package onetomany

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// AddListing saves a new listing and appends its id to the owning agent's
// listings list, inside one transaction — RunInTransaction(D-11) is the
// real capability behind this: checked against the actual
// TransactionalSession.java, Java's doInTransaction/doInTransactionReturning
// is the *only* transaction-control surface that exists (no public
// commit()/rollback() at all) — so this uses the one Go mechanism that's
// actually grounded in real Java capability, not the separate manual
// BeginTransaction/Commit/Rollback path (which doesn't correspond to
// anything the real SDK provides).
//
// GAP (see sdk/FUNCTIONAL_GAPS.md #15, #25): the cookbook's own
// listAppend(id, opts -> opts.addUnique().allowFailures()) call doesn't
// exist in any current Java source (checked both the core SDK and the
// mapper library directly) — that specific method is as stale as the
// TypedDataSet rename this package's doc comment already flags. The
// underlying capability (Java's cdt.ListWriteFlags/ListPolicy,
// ADD_UNIQUE/NO_FAIL) is real and current, matched by Go's classic
// client (as.NewListPolicy, as.ListWriteFlagsAddUnique) — but there's no
// PRD-grounded sdk/ method to invoke it through anymore:
// WriteBinBuilder.ListAppendItems (the method this call used to go
// through) was removed entirely — it was never named anywhere in
// sdk/PRD.md (finding #25), so there's no atomic CDT list-append at all
// in sdk/ now, unconditional or otherwise.
//
// Worked around here with the only PRD-grounded alternative: read the
// agent's current listing-id list, append client-side, then Set(name, v)
// the whole bin back — not atomic in the CDT sense (a concurrent AddListing
// on the same agent could race), but RunInTransaction's MRT wrapping
// around the read+write below is what actually keeps this consistent, not
// a CDT-level guarantee either way.
func (s *Service) AddListing(ctx context.Context, agentID int64, listing Listing) error {
	listing.AgentID = agentID
	listingKey := sdk.Key(s.listingDS.DataSet(), listing.ID)
	agentKey := sdk.Key(s.agentDS.DataSet(), agentID)

	return s.session.RunInTransaction(ctx, func(tx *sdk.Session) error {
		bins, err := sdk.Marshal(listing)
		if err != nil {
			return fmt.Errorf("marshal listing %s: %w", listing.ID, err)
		}
		if err := tx.Put(ctx, listingKey, bins); err != nil {
			return fmt.Errorf("put listing %s: %w", listing.ID, err)
		}

		record, err := tx.Get(ctx, agentKey, []string{agentListingsBin})
		if err != nil {
			return fmt.Errorf("get agent %d: %w", agentID, err)
		}
		agent, err := sdk.Decode[Agent](record)
		if err != nil {
			return fmt.Errorf("decode agent %d: %w", agentID, err)
		}
		listingIDs := append(agent.ListingIDs, listing.ID)

		if _, err := tx.Update(ctx, agentKey).
			Set(agentListingsBin, listingIDs).
			ExecuteOne(); err != nil {
			return fmt.Errorf("append listing %s to agent %d: %w", listing.ID, agentID, err)
		}
		return nil
	})
}

// DeleteListing deletes a listing and removes its id from its agent's
// listings list, inside one transaction. Reports whether the listing was
// found and deleted.
//
// GAP: the source Java example returns the actual removed-count
// (onListValue(id).removeAnd().count(), checked > 0) to confirm the id was
// really present in the list. Go's CDTNavBuilder.Count() (sdk/
// writesegmentbuilder.go) compiles into the same chain
// (OnListValue(id).RemoveAnd().Count()), but its terminal is ExecuteOne()
// (WriteResult, error) — WriteResult has no field for the count value
// itself, only Affected (bool). This is the same WriteResult-has-no-value
// limitation already documented in ecommerce/products.go's
// RecordProductRatings GAP.
//
// That means deleted below isn't just a weaker signal than Java's — it's
// an uncertain one. The PRD never says what Affected means for a CDT op
// that matches nothing (same open question already flagged for
// generation-mismatch writes elsewhere in this repo): does a
// RemoveAnd().Count() that removes zero items report Affected == false
// (nothing changed), or Affected == true (the write round-trip still
// "succeeded," it just touched nothing)? Until that's specified, treat
// this return value as best-effort, not a confirmed "the id was actually
// in the list" check the way Java's count > 0 is.
func (s *Service) DeleteListing(ctx context.Context, listingID string) (bool, error) {
	listingKey := sdk.Key(s.listingDS.DataSet(), listingID)

	var deleted bool
	err := s.session.RunInTransaction(ctx, func(tx *sdk.Session) error {
		record, err := tx.Get(ctx, listingKey, []string{listingAgentIDBin})
		if errors.Is(err, sdk.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get listing %s: %w", listingID, err)
		}
		listing, err := sdk.Decode[Listing](record)
		if err != nil {
			return fmt.Errorf("decode listing %s: %w", listingID, err)
		}

		if err := tx.Delete(ctx, listingKey); err != nil {
			return fmt.Errorf("delete listing %s: %w", listingID, err)
		}

		agentKey := sdk.Key(s.agentDS.DataSet(), listing.AgentID)
		result, err := tx.Update(ctx, agentKey).
			OnBin(agentListingsBin).OnListValue(listingID).RemoveAnd().Count().
			ExecuteOne()
		if err != nil {
			return fmt.Errorf("remove listing %s from agent %d: %w", listingID, listing.AgentID, err)
		}
		deleted = result.Affected
		return nil
	})
	return deleted, err
}

// GetListings reads an agent's listing-id list, then batch-reads every
// listing it references, inside one (read-only) transaction — matching
// the source example's use of doInTransactionReturning for a consistent
// read. Go has no separate "-Returning" transaction helper the way Java
// does: RunInTransaction's callback is func(*Session) error only. That's
// not a gap — Java needs a distinct Transactional<T> interface because a
// Java lambda can't reassign a captured local variable, but a Go closure
// can freely write to one declared in the enclosing function (listings,
// below), so capturing the result this way is the ordinary, idiomatic Go
// pattern, not a workaround for a missing capability.
func (s *Service) GetListings(ctx context.Context, agentID int64) ([]Listing, error) {
	agentKey := sdk.Key(s.agentDS.DataSet(), agentID)

	var listings []Listing
	err := s.session.RunInTransaction(ctx, func(tx *sdk.Session) error {
		record, err := tx.Get(ctx, agentKey, []string{agentListingsBin})
		if errors.Is(err, sdk.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get agent %d: %w", agentID, err)
		}
		agent, err := sdk.Decode[Agent](record)
		if err != nil {
			return fmt.Errorf("decode agent %d: %w", agentID, err)
		}
		if len(agent.ListingIDs) == 0 {
			return nil
		}

		keys := sdk.Keys(s.listingDS.DataSet(), agent.ListingIDs)
		stream, err := tx.BatchGet(ctx, keys, sdk.AllBins)
		if err != nil {
			return fmt.Errorf("batch get listings for agent %d: %w", agentID, err)
		}
		defer stream.Close()

		for row, rowErr := range stream.Iter(ctx) {
			if rowErr != nil {
				fmt.Printf("  Error: %v\n", rowErr)
				continue
			}
			rec, err := row.Record()
			if err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}
			listing, err := sdk.Decode[Listing](rec)
			if err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}
			listings = append(listings, listing)
		}
		return stream.Err()
	})
	return listings, err
}

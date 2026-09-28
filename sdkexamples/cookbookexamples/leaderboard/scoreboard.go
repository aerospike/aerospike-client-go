package leaderboard

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// UpdatePlayerScore sets a player's score on both the player record and
// the scoreboard's bucketed map, inside one transaction — pass oldScore
// < 0 for a brand-new leaderboard entry (nothing to remove yet).
func (s *Service) UpdatePlayerScore(ctx context.Context, playerID, oldScore, newScore int64) error {
	newBucketKey := sdk.Key(s.scoreboardDS, bucketForScore(newScore))
	newMapKey := mapKey(playerID, newScore)

	return s.session.RunInTransaction(ctx, func(tx *sdk.Session) error {
		if oldScore < 0 {
			if _, err := tx.Upsert(ctx, newBucketKey).
				OnBin(scoreboardBin).MapSetPolicy(as.MapOrder.KEY_ORDERED).
				OnBin(scoreboardBin).OnMapKey(newMapKey).Insert(playerID).
				ExecuteOne(); err != nil {
				return fmt.Errorf("insert new scoreboard entry for player %d: %w", playerID, err)
			}
		} else {
			oldMapKey := mapKey(playerID, oldScore)
			if bucketForScore(newScore) == bucketForScore(oldScore) {
				if _, err := tx.Upsert(ctx, newBucketKey).
					OnBin(scoreboardBin).OnMapKey(oldMapKey).Remove().
					OnBin(scoreboardBin).MapSetPolicy(as.MapOrder.KEY_ORDERED).
					OnBin(scoreboardBin).OnMapKey(newMapKey).Insert(playerID).
					ExecuteOne(); err != nil {
					return fmt.Errorf("move scoreboard entry for player %d within bucket: %w", playerID, err)
				}
			} else {
				oldBucketKey := sdk.Key(s.scoreboardDS, bucketForScore(oldScore))
				if _, err := tx.Upsert(ctx, oldBucketKey).
					OnBin(scoreboardBin).OnMapKey(oldMapKey).Remove().
					ExecuteOne(); err != nil {
					return fmt.Errorf("remove old scoreboard entry for player %d: %w", playerID, err)
				}
				if _, err := tx.Upsert(ctx, newBucketKey).
					OnBin(scoreboardBin).MapSetPolicy(as.MapOrder.KEY_ORDERED).
					OnBin(scoreboardBin).OnMapKey(newMapKey).Insert(playerID).
					ExecuteOne(); err != nil {
					return fmt.Errorf("insert new scoreboard entry for player %d in new bucket: %w", playerID, err)
				}
			}
			playerKey := sdk.Key(s.playerDS.DataSet(), playerID)
			if _, err := tx.Upsert(ctx, playerKey).
				Set(playerScoreBin, newScore).
				ExecuteOne(); err != nil {
				return fmt.Errorf("update player %d score bin: %w", playerID, err)
			}
		}
		return nil
	})
}

// GetScoresAroundPlayer reads the map keys immediately above and below a
// player's own scoreboard entry, within that player's bucket only.
//
// GAP: this exercises OnMapKeyRelativeIndexRange (sdk/writesegmentbuilder.go)
// for the first time anywhere in these examples — real, PRD-listed API
// (10.15: "range/list/relative forms... OnMapKeyRange, OnListIndexRange,
// …") that matches the source Java example's single AEL relative-range
// selector ($.score.{-N:N~'key'}.getKeys()) exactly in intent. But there
// is no read-side entry point for this: OnMapKeyRelativeIndexRange lives
// on WriteBinBuilder, so the only way to reach it is through a write
// segment, and its terminal (GetKeys()) returns to WriteSegmentBuilder,
// whose only terminal is ExecuteOne() (WriteResult, error) —
// WriteResult has no value field to carry the returned keys back. Same
// WriteResult-has-no-value limitation already documented in
// ecommerce/products.go's RecordProductRatings GAP and
// cookbookexamples/onetomany's DeleteListing. So this can confirm the
// operation was accepted, not actually return the neighboring players —
// there's nothing here pretending otherwise.
//
// GAP: the source Java example also spills over into neighboring buckets
// when the requested range overflows the current one
// (onMapIndexRange(index, count) — an index-based range, not the
// key-relative range above). Checked sdk/writesegmentbuilder.go directly:
// no OnMapIndexRange exists under any name. The PRD's own 10.15 text only
// gestures at "range/list/relative forms... …" via an ellipsis, never
// naming this one specifically — not enough to build against, so the
// overflow case isn't attempted here.
func (s *Service) GetScoresAroundPlayer(ctx context.Context, playerID, score int64, numEitherSide int) error {
	bucketKey := sdk.Key(s.scoreboardDS, bucketForScore(score))
	key := mapKey(playerID, score)

	_, err := s.session.Update(ctx, bucketKey).
		OnBin(scoreboardBin).OnMapKeyRelativeIndexRange(key, -numEitherSide, 2*numEitherSide+1).GetKeys().
		ExecuteOne()
	if err != nil {
		return fmt.Errorf("get scores around player %d: %w", playerID, err)
	}
	fmt.Printf("requested scores around player %d (result not readable — see GAP comment)\n", playerID)
	return nil
}

package leaderboard

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// UpdatePlayerScore sets a player's score on both the player record and
// the scoreboard's bucketed map, inside one transaction — pass oldScore
// < 0 for a brand-new leaderboard entry (nothing to remove yet).
//
// GAP: every bucket write here used to call
// OnBin(scoreboardBin).MapSetPolicy(as.MapOrder.KEY_ORDERED) first, to
// request the map be stored key-ordered — required for the
// now-removed GetScoresAroundPlayer's relative-range read to make sense
// at all (see the doc comment below). WriteBinBuilder.MapSetPolicy has
// since been removed entirely (it
// was never PRD-grounded, sdk/FUNCTIONAL_GAPS.md finding #25) — there is
// now no PRD-grounded way to request map ordering at all (findings #15,
// #22 also apply: no ListPolicy/MapPolicy plumbing, no
// AerospikeMap-style typed wrapper either). The inserts below no longer
// request KEY_ORDERED at all; whether the server still stores this map
// key-ordered by some other default is not something sdk/ can express or
// verify either way.
func (s *Service) UpdatePlayerScore(ctx context.Context, playerID, oldScore, newScore int64) error {
	newBucketKey := sdk.Key(s.scoreboardDS, bucketForScore(newScore))
	newMapKey := mapKey(playerID, newScore)

	return s.session.RunInTransaction(ctx, func(tx *sdk.Session) error {
		if oldScore < 0 {
			if _, err := tx.Upsert(ctx, newBucketKey).
				OnBin(scoreboardBin).OnMapKey(newMapKey).Insert(playerID).
				ExecuteOne(); err != nil {
				return fmt.Errorf("insert new scoreboard entry for player %d: %w", playerID, err)
			}
		} else {
			oldMapKey := mapKey(playerID, oldScore)
			if bucketForScore(newScore) == bucketForScore(oldScore) {
				if _, err := tx.Upsert(ctx, newBucketKey).
					OnBin(scoreboardBin).OnMapKey(oldMapKey).Remove().
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

// GetScoresAroundPlayer previously read the map keys immediately above and
// below a player's own scoreboard entry, within that player's bucket
// only. There is no function here any more: there is nothing real left
// to call.
//
// GAP: this used OnMapKeyRelativeIndexRange (sdk/writesegmentbuilder.go)
// — a real capability the source Java example needs
// ($.score.{-N:N~'key'}.getKeys()) — but that method has since been
// removed: it was only ever grounded by the same "range/list/relative
// forms... OnMapKeyRange, OnListIndexRange, …" ellipsis already ruled
// insufficient to ground OnMapIndexRange (finding #17), a fact this
// package's own earlier comment inconsistently didn't apply to itself.
// Corrected during the full-sdk/ audit prompted by a user question
// (sdk/FUNCTIONAL_GAPS.md finding #27).
//
// No substitute exists: OnMapKeyRange(begin, end) (still present) takes
// exact key bounds, not "N nearest either side" — and this map's keys
// are score-derived strings (mapKey, above) with no way to compute
// "the key N entries away" without already knowing the map's contents,
// which sdk/ can't read back either way (Record has no bin-accessor
// methods, finding #16). Even when OnMapKeyRelativeIndexRange existed,
// this was already unreadable — WriteResult has no value field to carry
// GetKeys() back (same limitation as ecommerce/products.go and
// onetomany's DeleteListing) — so nothing demonstrable is lost by
// removing the call entirely versus keeping an unreadable one. The
// source Java example's overflow-into-neighboring-buckets behavior
// (onMapIndexRange(index, count), a different, index-based range) was
// never buildable either — no OnMapIndexRange exists under any name.

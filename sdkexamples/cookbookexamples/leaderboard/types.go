// Package leaderboard demonstrates a bucketed-scoreboard leaderboard: the
// full player list stays a plain typed dataset, but scores live in a
// separate "scoreboard" dataset, one record per score bucket
// (bucket = score / scoresPerBucket), each holding a single key-ordered
// map keyed by a zero-padded "score-playerId" composite string — so map
// key order is score order, and a relative-range read around a known key
// answers "who's near this player" without a secondary index or a scan.
//
// Source: the use-case-cookbook's gaming/Leaderboard.java. Same situation
// as cookbookexamples/onetomany: that file (and the rest of the cookbook)
// imports com.aerospike.client.sdk.TypedDataSet and
// com.aerospike.mapper.tools.AeroMapper, neither of which exists in the
// current aerospike-client-java-sdk source (only TypeSafeDataSet.java
// does) — the cookbook predates a rename and wouldn't compile against the
// current SDK. This package re-derives the same real mechanic using this
// repo's already-established Go idiom, not a translation.
package leaderboard

import "fmt"

const (
	scoreboardBin = "score"

	scoresPerBucket = 25
	maxScore        = 6300
)

func bucketForScore(score int64) int64 {
	return score / scoresPerBucket
}

// mapKey is the zero-padded "score-playerId" composite string that makes
// key-ordered map order equal to score order.
func mapKey(playerID, score int64) string {
	return fmt.Sprintf("%05d-%09d", score, playerID)
}

const (
	playerUserNameBin  = "userName"
	playerFirstNameBin = "firstName"
	playerLastNameBin  = "lastName"
	playerScoreBin     = "score"
)

// Player is a leaderboard participant, keyed by ID in the "players"
// dataset.
type Player struct {
	ID        int64  `as:",key"`
	UserName  string `as:"userName"`
	FirstName string `as:"firstName"`
	LastName  string `as:"lastName"`
	Score     int64  `as:"score"`
}

func (p Player) String() string {
	return fmt.Sprintf("Player[%d, %s, score=%d]", p.ID, p.UserName, p.Score)
}

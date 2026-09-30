package leaderboard

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Service groups the session, the typed player dataset, and the plain
// (untyped) scoreboard dataset — each scoreboard record is just one map
// bin, never mapped to a struct.
type Service struct {
	session      *sdk.Session
	playerDS     *sdk.TypedDataSet[Player]
	scoreboardDS *sdk.DataSet
}

func NewService(session *sdk.Session, playerDS *sdk.TypedDataSet[Player], scoreboardDS *sdk.DataSet) *Service {
	return &Service{session: session, playerDS: playerDS, scoreboardDS: scoreboardDS}
}

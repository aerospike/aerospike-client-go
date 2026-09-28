// Command leaderboard runs the bucketed-scoreboard example end to end:
// seed players and populate the scoreboard, update one player's score
// (moving their scoreboard entry, possibly into a different bucket, all
// inside one transaction), then request the scores around that player.
package main

import (
	"context"
	"log"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/cookbookexamples/leaderboard"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cluster, err := sdk.NewClusterDefinition("localhost", 3100).Connect(ctx)
	if err != nil {
		return err
	}
	defer cluster.Close()

	if err := cluster.Ping(ctx); err != nil {
		return err
	}

	session, err := cluster.CreateSession(ctx, sdk.DefaultBehavior())
	if err != nil {
		return err
	}

	playerDS, err := sdk.NewTypedDataSet[leaderboard.Player]("test", "players")
	if err != nil {
		return err
	}
	scoreboardDS := sdk.MustNewDataSet("test", "scoreboard")

	svc := leaderboard.NewService(session, playerDS, scoreboardDS)

	if err := svc.Setup(ctx); err != nil {
		return err
	}

	const playerID = int64(1)
	const oldScore = int64(3120)
	const newScore = int64(3200)

	if err := svc.UpdatePlayerScore(ctx, playerID, oldScore, newScore); err != nil {
		return err
	}
	log.Printf("player %d moved from score %d to %d", playerID, oldScore, newScore)

	return svc.GetScoresAroundPlayer(ctx, playerID, newScore, 3)
}

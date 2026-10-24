package leaderboard

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

var seedPlayers = []Player{
	{ID: 1, UserName: "estefanaruecker", FirstName: "Estefana", LastName: "Ruecker", Score: 3120},
	{ID: 2, UserName: "arnoldomacgyver", FirstName: "Arnoldo", LastName: "MacGyver", Score: 2980},
	{ID: 3, UserName: "jacquelynewillms", FirstName: "Jacquelyne", LastName: "Willms", Score: 3105},
	{ID: 4, UserName: "harrisjones", FirstName: "Harris", LastName: "Jones", Score: 3140},
	{ID: 5, UserName: "ardeliarenner", FirstName: "Ardelia", LastName: "Renner", Score: 2950},
	{ID: 6, UserName: "estefanajones", FirstName: "Estefana", LastName: "Jones", Score: 3098},
}

// Setup truncates both datasets, seeds a handful of players, then
// populates the scoreboard for each via UpdatePlayerScore — the same
// transaction-wrapped path a caller would use afterward, not a bulk
// shortcut.
func (s *Service) Setup(ctx context.Context) error {
	if err := s.session.Truncate(ctx, s.playerDS.DataSet(), time.Now()); err != nil {
		return fmt.Errorf("truncate players: %w", err)
	}
	if err := s.session.Truncate(ctx, s.scoreboardDS, time.Now()); err != nil {
		return fmt.Errorf("truncate scoreboard: %w", err)
	}

	for _, player := range seedPlayers {
		bins, err := sdk.Marshal(player)
		if err != nil {
			return fmt.Errorf("marshal player %d: %w", player.ID, err)
		}
		key := sdk.Key(s.playerDS.DataSet(), player.ID)
		if err := s.session.Put(ctx, key, bins); err != nil {
			return fmt.Errorf("seed player %d: %w", player.ID, err)
		}
		if err := s.UpdatePlayerScore(ctx, player.ID, -1, player.Score); err != nil {
			return fmt.Errorf("seed scoreboard entry for player %d: %w", player.ID, err)
		}
	}
	return nil
}

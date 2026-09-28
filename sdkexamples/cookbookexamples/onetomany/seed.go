package onetomany

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

var seedAgents = []Agent{
	{ID: 1, FirstName: "Estefana", LastName: "Ruecker", Email: "estefana.ruecker@example.com", Phone: "555-0101", RegisteredAt: time.Now()},
	{ID: 2, FirstName: "Arnoldo", LastName: "MacGyver", Email: "arnoldo.macgyver@example.com", Phone: "555-0102", RegisteredAt: time.Now()},
	{ID: 3, FirstName: "Jacquelyne", LastName: "Willms", Email: "jacquelyne.willms@example.com", Phone: "555-0103", RegisteredAt: time.Now()},
}

var seedListings = []struct {
	Listing Listing
	AgentID int64
}{
	{Listing{ID: "Listing-1", Address: "123 Main St", City: "Springfield", State: "CA", Zip: "90001", URL: "https://example.com/listings/Listing-1", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 1},
	{Listing{ID: "Listing-2", Address: "456 Oak Ave", City: "Franklin", State: "TX", Zip: "75001", URL: "https://example.com/listings/Listing-2", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 1},
	{Listing{ID: "Listing-3", Address: "789 Elm St", City: "Greenville", State: "NY", Zip: "10001", URL: "https://example.com/listings/Listing-3", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 2},
	{Listing{ID: "Listing-4", Address: "321 Maple Dr", City: "Clinton", State: "FL", Zip: "33001", URL: "https://example.com/listings/Listing-4", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 2},
	{Listing{ID: "Listing-5", Address: "654 Cedar Ln", City: "Fairview", State: "WA", Zip: "98001", URL: "https://example.com/listings/Listing-5", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 3},
	{Listing{ID: "Listing-6", Address: "987 Main St", City: "Springfield", State: "CA", Zip: "90002", URL: "https://example.com/listings/Listing-6", DateListed: time.Now(), Description: "A lovely property in a great neighborhood."}, 3},
}

// Setup truncates both datasets, seeds a handful of agents and listings,
// then associates each listing with its agent via AddListing — the same
// transaction-wrapped path a caller would use afterward, not a bulk
// shortcut, so the seed data ends up in exactly the state the rest of
// this package expects (agent.ListingIDs populated, listing.AgentID set).
func (s *Service) Setup(ctx context.Context) error {
	if err := s.session.Truncate(ctx, s.agentDS.DataSet(), time.Now()); err != nil {
		return fmt.Errorf("truncate agents: %w", err)
	}
	if err := s.session.Truncate(ctx, s.listingDS.DataSet(), time.Now()); err != nil {
		return fmt.Errorf("truncate listings: %w", err)
	}

	for _, agent := range seedAgents {
		bins, err := sdk.Marshal(agent)
		if err != nil {
			return fmt.Errorf("marshal agent %d: %w", agent.ID, err)
		}
		key := sdk.Key(s.agentDS.DataSet(), agent.ID)
		if err := s.session.Put(ctx, key, bins); err != nil {
			return fmt.Errorf("seed agent %d: %w", agent.ID, err)
		}
	}

	for _, sl := range seedListings {
		if err := s.AddListing(ctx, sl.AgentID, sl.Listing); err != nil {
			return fmt.Errorf("seed listing %s: %w", sl.Listing.ID, err)
		}
	}
	return nil
}

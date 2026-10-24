// Command onetomany runs the one-to-many relationship example end to
// end: seed agents and listings, list an agent's current listings, add a
// new listing to that agent, list again, then delete a listing and list
// once more — each mutation wrapped in a transaction so the agent's
// listing list and each listing's back-reference never go out of sync.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/cookbookexamples/onetomany"
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

	agentDS, err := sdk.NewTypedDataSet[onetomany.Agent]("test", "agents")
	if err != nil {
		return err
	}
	listingDS, err := sdk.NewTypedDataSet[onetomany.Listing]("test", "listings")
	if err != nil {
		return err
	}

	svc := onetomany.NewService(session, agentDS, listingDS)

	if err := svc.Setup(ctx); err != nil {
		return err
	}

	const agentID = int64(1)

	listings, err := svc.GetListings(ctx, agentID)
	if err != nil {
		return err
	}
	logListings("Current listings", listings)

	newListing := onetomany.Listing{
		ID:          "Listing-X999",
		Address:     "999 New Listing Way",
		City:        "Springfield",
		State:       "CA",
		Zip:         "90003",
		URL:         "https://example.com/listings/Listing-X999",
		DateListed:  time.Now(),
		Description: "A brand new listing.",
	}
	if err := svc.AddListing(ctx, agentID, newListing); err != nil {
		return err
	}

	listings, err = svc.GetListings(ctx, agentID)
	if err != nil {
		return err
	}
	logListings("Listings after adding a new one", listings)

	if len(listings) > 0 {
		deleted, err := svc.DeleteListing(ctx, listings[0].ID)
		if err != nil {
			return err
		}
		log.Printf("deleted %s: %v", listings[0].ID, deleted)
	}

	listings, err = svc.GetListings(ctx, agentID)
	if err != nil {
		return err
	}
	logListings("Listings after deleting one", listings)
	return nil
}

func logListings(header string, listings []onetomany.Listing) {
	log.Printf("%s (%d):", header, len(listings))
	for _, l := range listings {
		log.Printf("  %s", l)
	}
}

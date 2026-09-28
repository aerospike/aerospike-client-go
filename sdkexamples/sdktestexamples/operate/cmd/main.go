// Command operate runs the whole-record operate examples end to end:
// read-then-delete-record, delete-and-recreate atomically, and
// read-then-touch-record with a refreshed TTL.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/sdktestexamples/operate"
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

	ds := sdk.MustNewDataSet("test", "operate")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := operate.NewService(session, ds)

	if err := svc.DemonstrateDeleteRecord(ctx, 1); err != nil {
		return err
	}
	return svc.DemonstrateTouchRecord(ctx, 2)
}

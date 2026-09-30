// Command pointreads runs the header-only and missing-keys read
// examples end to end.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/sdktestexamples/pointreads"
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

	ds := sdk.MustNewDataSet("test", "pointreads")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := pointreads.NewService(session, ds)

	if err := svc.DemonstrateGetHeader(ctx, "getHeader"); err != nil {
		return err
	}
	if err := svc.DemonstrateQueryWithNoBins(ctx, "withNoBins"); err != nil {
		return err
	}
	return svc.DemonstrateIncludeMissingKeys(ctx, "present", "missing")
}

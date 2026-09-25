// Command commonexamples runs the general-purpose Session examples end to
// end: truncate the dataset, create a secondary index, exercise the point
// exists/touch/delete shortcuts, then batch-check/touch/delete one
// coherent set of keys (one deliberately missing throughout).
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/repoexamples/commonexamples"
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

	ds := sdk.MustNewDataSet("test", "common")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := commonexamples.NewService(session, ds)

	if err := svc.DemonstrateIndexCreation(ctx); err != nil {
		return err
	}

	if err := svc.DemonstratePointShortcuts(ctx, 1); err != nil {
		return err
	}

	return svc.DemonstrateBatchExistsTouchDelete(ctx, []int64{10, 11, 12})
}

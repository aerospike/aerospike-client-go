// Command partition runs the partition-scoped query examples end to end.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/sdktestexamples/partition"
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

	ds := sdk.MustNewDataSet("test", "test_pagination")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := partition.NewService(session, ds)

	// Source test uses targetPartition=10, numRecords=150, limit=90,
	// chunkSize=18 — reduced here (20/15/5) purely so seeding (which
	// must construct and hash candidate keys until enough land in one
	// partition, ~4096 candidates per hit on average) finishes quickly;
	// the demonstrated capability is identical either way.
	if err := svc.DemonstratePartitionQuery(ctx, 10, 20, 15, 5); err != nil {
		return err
	}

	// Matches the PRD's own §10.7 Look example verbatim: .OnPartitionRange(0, 2048).
	return svc.DemonstratePartitionRangeQuery(ctx, 2048)
}

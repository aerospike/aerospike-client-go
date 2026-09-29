// Command udf runs the UDF examples end to end, mirroring the source
// Java test's four isolated scenarios (each its own key and bin):
// writeUsingUdf, writeIfGenerationNotChanged, writeIfNotExists,
// writeWithValidation — plus a background UDF task
// (BackgroundTaskTest.java's backgroundUdf). Registers the embedded Lua
// module first, lists registered modules, then removes it at the end.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/sdktestexamples/udf"
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

	ds := sdk.MustNewDataSet("test", "udf")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := udf.NewService(session, ds)

	if err := svc.RegisterModule(ctx); err != nil {
		return err
	}

	if err := svc.DemonstrateWriteUsingUdf(ctx); err != nil {
		return err
	}
	if err := svc.DemonstrateWriteIfGenerationNotChanged(ctx); err != nil {
		return err
	}
	if err := svc.DemonstrateWriteIfNotExists(ctx); err != nil {
		return err
	}
	if err := svc.DemonstrateWriteWithValidation(ctx); err != nil {
		return err
	}
	if err := svc.DemonstrateBackgroundUdf(ctx); err != nil {
		return err
	}

	count, err := svc.ListModules(ctx)
	if err != nil {
		return err
	}
	log.Printf("registered UDF modules: %d", count)

	return svc.RemoveModule(ctx)
}

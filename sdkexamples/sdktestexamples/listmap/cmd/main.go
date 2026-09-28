// Command listmap runs the list/map bin value examples end to end.
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/sdktestexamples/listmap"
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

	ds := sdk.MustNewDataSet("test", "listmap")
	if err := session.Truncate(ctx, ds, time.Now()); err != nil {
		return err
	}

	svc := listmap.NewService(session, ds)

	demos := []struct {
		name string
		run  func(context.Context, string) error
	}{
		{"aerospikeListBinValue", svc.DemonstrateListBinsValues},
		{"aerospikeListOps", svc.DemonstrateListOps},
		{"aerospikeMapBinsValues", svc.DemonstrateMapBinsValues},
		{"aerospikeMapOps", svc.DemonstrateMapOps},
		{"listStrings", svc.DemonstrateListStrings},
		{"listComplex", svc.DemonstrateListComplex},
		{"mapStrings", svc.DemonstrateMapStrings},
		{"mapComplex", svc.DemonstrateMapComplex},
		{"listMapCombined", svc.DemonstrateListMapCombined},
		{"keyOrderedMapTypes", svc.DemonstrateKeyOrderedMap},
		{"sortedMapReplace", svc.DemonstrateSortedMapReplace},
	}

	for _, d := range demos {
		if err := d.run(ctx, d.name); err != nil {
			return err
		}
	}
	return nil
}

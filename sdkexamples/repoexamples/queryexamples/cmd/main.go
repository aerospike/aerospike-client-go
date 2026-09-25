// Command queryexamples runs the query/CDT/concurrency-control examples
// end to end. Currently: demonstrate the built-in Behavior presets, list
// namespaces and their stats, seed one customer, demonstrate a
// generation-checked conditional write, exercise the top-level list/map
// CDT mutation vocabulary, page through sorted results, throttle/chunk a
// scan, project a bin under an alias, run background task/delete/touch
// scans, demonstrate TTL expiration, run a filtered query, then round-trip
// a typed object with a nested struct.
package main

import (
	"context"
	"fmt"
	"log"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/repoexamples/queryexamples"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// Mirrors the source Java QueryExamples.java's own main() connect
	// chain (usingServicesAlternate/withNativeCredentials/preferringRacks/
	// withSystemSettings) — previously simplified away to a bare
	// NewClusterDefinition(...).Connect(ctx) here without being flagged as
	// a deliberate difference. WithSystemSettings is passed a zero-value
	// SystemSettings{}: the Java call configures a circuit breaker
	// (maximumErrorsInErrorWindow) and a connection pool
	// (minimumConnectionsPerNode/maximumConnectionsPerNode) via a builder
	// callback, but sdk/behavior.go's SystemSettings is currently an empty
	// struct — no fields exist to carry either setting, so there's nothing
	// to set here (a real, separate gap from the connect-chain one this
	// fixes).
	cluster, err := sdk.NewClusterDefinition("localhost", 3100).
		UsingServicesAlternate().
		WithNativeCredentials("admin", "password123").
		PreferringRacks(1).
		WithSystemSettings(sdk.SystemSettings{}).
		Connect(ctx)
	if err != nil {
		return err
	}
	defer cluster.Close()

	if err := cluster.Ping(ctx); err != nil {
		return err
	}

	if err := queryexamples.DemonstrateBehaviorPresets(ctx, cluster); err != nil {
		return err
	}

	session, err := cluster.CreateSession(ctx, sdk.DefaultBehavior())
	if err != nil {
		return err
	}

	customerDS, err := sdk.NewTypedDataSet[queryexamples.Customer]("test", "person")
	if err != nil {
		return err
	}

	svc := queryexamples.NewService(session, customerDS)

	namespaces, err := svc.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	for _, ns := range namespaces {
		stats, err := svc.NamespaceStats(ctx, ns)
		if err != nil {
			return err
		}
		fmt.Printf("namespace %s: %v\n", ns, stats)
	}

	const customerID = int64(999)
	if err := svc.SeedCustomer(ctx, queryexamples.Customer{ID: customerID, Name: "Sample", Age: 30}); err != nil {
		return err
	}

	if err := svc.DemonstrateGenerationCheck(ctx, customerID); err != nil {
		return err
	}

	const cdtTestID = int64(500)
	if err := svc.DemonstrateListMapOperations(ctx, cdtTestID); err != nil {
		return err
	}

	if err := svc.DemonstrateSortedPaging(ctx); err != nil {
		return err
	}

	if err := svc.DemonstrateQueryThrottling(ctx); err != nil {
		return err
	}

	if err := svc.DemonstrateBinProjection(ctx); err != nil {
		return err
	}

	if err := svc.DemonstrateBackgroundTask(ctx, customerID); err != nil {
		return err
	}

	if err := svc.DemonstrateBackgroundDelete(ctx); err != nil {
		return err
	}

	if err := svc.DemonstrateBackgroundTouch(ctx, customerID); err != nil {
		return err
	}

	const ttlTestID = int64(1)
	if err := svc.DemonstrateTTL(ctx, ttlTestID); err != nil {
		return err
	}

	if err := svc.DemonstrateQueryFiltering(ctx); err != nil {
		return err
	}

	const objectMappingID = int64(998)
	return svc.DemonstrateObjectMapping(ctx, objectMappingID)
}

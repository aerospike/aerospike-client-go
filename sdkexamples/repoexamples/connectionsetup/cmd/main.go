// Command connectionsetup runs the connection-setup example end to end.
package main

import (
	"context"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/repoexamples/connectionsetup"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cluster, err := connectionsetup.Connect(ctx, connectionsetup.ConnectOptions{
		Hosts:                  []*as.Host{as.NewHost("localhost", 3100)},
		ClusterName:            "docker",
		AppID:                  "connectionsetup-example",
		FailIfNotConnected:     true,
		UsingServicesAlternate: false,
		Auth:                   connectionsetup.AuthInternal,
		User:                   "admin",
		Password:               "password123",
	})
	if err != nil {
		return err
	}
	defer cluster.Close()

	return cluster.Ping(ctx)
}

package connectionsetup

import (
	"context"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// ConnectOptions is the real, working subset of connection options found
// in ClusterTest.java's initCluster() and SdkConnector.connect(), plus
// YamlConfigConnectionExample.java's AppID/FailIfNotConnected.
type ConnectOptions struct {
	Hosts                  []*as.Host
	ClusterName            string
	AppID                  string
	FailIfNotConnected     bool
	UsingServicesAlternate bool
	Auth                   AuthMode
	User, Password         string
}

// Connect ports the real connect-chain shape found across three sources
// (see the package doc comment): build a ClusterDefinition from a seed
// host list rather than a single host:port pair, apply cluster name, app
// id, and fail-if-not-connected, optionally use alternate services, pick
// an auth mode exactly like ClusterTest.java's real switch, then connect.
func Connect(ctx context.Context, opts ConnectOptions) (*sdk.Cluster, error) {
	def := sdk.WithHosts(opts.Hosts...).
		ValidateClusterNameIs(opts.ClusterName).
		AppID(opts.AppID).
		FailIfNotConnected(opts.FailIfNotConnected)

	if opts.UsingServicesAlternate {
		def = def.UsingServicesAlternate()
	}

	switch opts.Auth {
	case AuthInternal:
		def = def.WithNativeCredentials(opts.User, opts.Password)
	case AuthExternal:
		def = def.WithExternalCredentials(opts.User, opts.Password)
	case AuthExternalInsecure:
		def = def.WithExternalInsecureCredentials(opts.User, opts.Password)
	}

	return def.Connect(ctx)
}

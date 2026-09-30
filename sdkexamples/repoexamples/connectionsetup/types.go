// Package connectionsetup demonstrates the ClusterDefinition connect-chain
// surface beyond the bare NewClusterDefinition(host, port).Connect(ctx)
// every other package's cmd/main.go uses. Real grounding, from three
// separate sources, none of them a dedicated named test — connecting is
// a one-time setup step, not something any single test/example is "about"
// — but each option below is genuinely exercised, not invented:
//
//   - examples/'s YamlConfigConnectionExample.java (unported): the real,
//     customer-facing connect chain `.appId("yaml-config-example").
//     failIfNotConnected(true)`, plus a conditional
//     `.usingServicesAlternate()` (already touched elsewhere in this
//     repo, included here for a coherent single chain).
//   - the SDK's own shared test bootstrap, ClusterTest.java's
//     initCluster(): the multi-host constructor form (`Host.parseHosts`
//     feeding `new ClusterDefinition(hosts)`, matching Go's free-function
//     `WithHosts`), `.clusterName(...)`, and a real
//     INTERNAL/EXTERNAL/EXTERNAL_INSECURE auth-mode switch choosing
//     between `withNativeCredentials`/`withExternalCredentials`/
//     `withExternalInsecureCredentials` based on runtime config.
//   - the cookbook's own connection helper, SdkConnector.java: the same
//     Host[]-constructor and `clusterName` shape, independently,
//     confirming this isn't a one-off in the test harness.
//
// GAP: `WithCertificateCredentials`, `WithIPMap`, `TendTimeout`, and
// `LoginTimeout` are real, PRD-named methods with zero usage found
// anywhere across the Java test suite, the examples dir, or the
// cookbook — not demonstrated here, no test to port from.
package connectionsetup

// AuthMode mirrors ClusterTest.java's real INTERNAL/EXTERNAL/
// EXTERNAL_INSECURE switch — the three credential-mode branches that
// initCluster() actually chooses between at runtime.
type AuthMode int

const (
	AuthNone AuthMode = iota
	AuthInternal
	AuthExternal
	AuthExternalInsecure
)

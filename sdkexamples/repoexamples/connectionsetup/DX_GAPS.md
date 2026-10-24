# DX gaps found while building this example

## Nothing here is a dedicated named test — every source is shared bootstrap code

`WithHosts`, `ValidateClusterNameIs`, `AppID`, `FailIfNotConnected`,
`WithExternalCredentials`, and `WithExternalInsecureCredentials` are all
real, all genuinely called with real argument shapes — but every single
occurrence found anywhere in the Java tree (tests, examples dir,
cookbook) is inside a shared connection-bootstrap helper
(`ClusterTest.java`'s `initCluster()`, the examples dir's `Example.java`,
the cookbook's `SdkConnector.java`), never a scenario a specific named
test or example is actually *about*. That's expected — connecting is a
one-time setup step every program does once, not something with its own
use case — but it does mean this package can't cite one clean source
the way `sdktestexamples/rawnext` or `sdktestexamples/prepend` can; it
synthesizes the real, working shape from three separate bootstrap
classes instead.

## Four real, PRD-named methods have zero usage anywhere

`WithCertificateCredentials`, `WithIPMap`, `TendTimeout`, and
`LoginTimeout` are declared in `sdk/cluster.go` and named in
`sdk/PRD.md` with "Keep" actions, but a direct grep across the entire
Java test suite, examples dir, and cookbook turns up nothing — no test,
example, or cookbook use case ever calls the Java equivalents
(`withCertificateCredentials()`, `ipMap(...)`, `tendTimeout(...)`,
`loginTimeout(...)`). Not demonstrated here; no test to port from.

## Can't verify any of this actually works

Same as every other package in this repo: `sdk/`'s `ClusterDefinition`/
`Cluster` are stub types, so `Connect(ctx)` returns `nil, nil`
unconditionally regardless of what options are passed. This can only
confirm the call shape compiles against real argument types
(`*as.Host`, `AuthMode`, etc.), not that any of these connection options
actually take effect against a live cluster.

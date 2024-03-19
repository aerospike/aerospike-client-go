module github.com/aerospike/aerospike-client-go/v5

go 1.16

require (
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/nxadm/tail v1.4.11 // indirect
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.32.0
	github.com/yuin/gopher-lua v1.1.1
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sync v0.6.0
)

retract (
	// Scan/Query/Other streaming commands could put a faulty connection back to the pool after a cluster event where in certain conditions its contents would end up in another scan and mix the results.
	[v5.6.0, v5.9.1]

	// An authentication bug was discovered where the client may fail to refresh its session token after it expires, requiring the client to be restarted. Update to the latest client.
	[v5.0.0, v5.5.0]
)

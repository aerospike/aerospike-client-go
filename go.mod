module github.com/aerospike/aerospike-client-go/v5

go 1.13

require (
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.15.0
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da
	golang.org/x/net v0.0.0-20210805182204-aaa1db679c0d // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210809222454-d867a43fc93e // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.5 // indirect
)

retract (
    // Scan/Query/Other streaming commands could put a faulty connection back to the pool after a cluster event where in certain conditions its contents would end up in another scan and mix the results.
	[v5.6.0, v5.7.0, v5.8.0, v5.9.0, v5.9.1]
	// An authentication bug was discovered where the client may fail to refresh its session token after it expires, requiring the client to be restarted. Update to the latest client.
	[v5.0.0, v5.0.1, v5.0.2, v5.1.0, v5.2.0, v5.3.0, v5.4.0, v5.5.0]
)

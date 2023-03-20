module github.com/aerospike/aerospike-client-go/v6

go 1.16

require (
	github.com/onsi/ginkgo/v2 v2.9.1
	github.com/onsi/gomega v1.27.4
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da
	golang.org/x/sync v0.1.0
)

retract (
	// Scan/Query/Other streaming commands could put a faulty connection back to the pool after a cluster event where in certain conditions its contents would end up in another scan and mix the results.
	[v6.2.1, v6.3.0]

	// Theis release contains major bugs in `BatchOperate` and Scan/Query. Update to the latest version.
	[v6.0.0, v6.2.0]
)

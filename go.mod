module github.com/aerospike/aerospike-client-go/v6

go 1.17

require (
	github.com/onsi/ginkgo/v2 v2.9.2
	github.com/onsi/gomega v1.27.4
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da
	golang.org/x/sync v0.1.0
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/stretchr/testify v1.8.1 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/tools v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	// Scan/Query/Other streaming commands could put a faulty connection back to the pool after a cluster event where in certain conditions its contents would end up in another scan and mix the results.
	[v6.2.1, v6.3.0]

	// Theis release contains major bugs in `BatchOperate` and Scan/Query. Update to the latest version.
	[v6.0.0, v6.2.0]
)

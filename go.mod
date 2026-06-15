module github.com/aerospike/aerospike-client-go/v8

go 1.25.0

require (
	github.com/onsi/ginkgo/v2 v2.27.5
	github.com/onsi/gomega v1.39.0
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad
	github.com/yuin/gopher-lua v1.1.2
	golang.org/x/sync v0.20.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20250403155104-27863c87afa6 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/tools v0.45.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

retract [v8.3.0, v8.4.1] // Problem with User Agent code returning a closed connection without an error.

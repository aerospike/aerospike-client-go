package aerospike

const (
	LATENCY_CONN  = iota
	LATENCY_WRITE = iota
	LATENCY_READ  = iota
	LATENCY_BATCH = iota
	LATENCY_QUERY = iota
	LATENCY_NONE  = iota
)

var latency_bucket_names = map[int]string{
	LATENCY_CONN:  "CONN",
	LATENCY_WRITE: "WRITE",
	LATENCY_READ:  "READ",
	LATENCY_BATCH: "BATCH",
	LATENCY_QUERY: "QUERY",
}

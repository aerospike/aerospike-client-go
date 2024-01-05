package aerospike

type MetricsPolicy struct {
	listener       MetricsListener
	reportDir      string
	interval       int
	latencyColumns int
	latencyShift   int
}

func NewMetricsPolicy() *MetricsPolicy {
	return &MetricsPolicy{
		reportDir:      ".",
		interval:       30,
		latencyColumns: 5,
		latencyShift:   3,
	}
}

package aerospike

type MetricsListener interface {
	onEnable(cluster *Cluster, policy *MetricsPolicy)
	onDisable(cluster *Cluster)
}

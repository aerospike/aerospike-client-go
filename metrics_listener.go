package aerospike

type MetricsListener interface {
	// Takes in a cluster object to collect data about it
	onEnable(cluster *Cluster, policy *MetricsPolicy)
	onSnapshot(cluster *Cluster)
	onDisable(cluster *Cluster)
}

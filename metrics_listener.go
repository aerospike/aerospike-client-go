package aerospike

type MetricsListener interface {
	// Takes in a cluster object to collect data about it
	onEnable(cluster *Cluster, policy *MetricsPolicy) error
	onSnapshot(cluster *Cluster) error
	onNodeClose(node *Node) error
	onDisable(cluster *Cluster) error
}

package aerospike

// The reason this is a slice of maps is because we are carrying multiple sets of labels
// one for each node in the cluster.
type Labels []map[string]string

func NewLabels(pairs ...map[string]string) *Labels {
	labels := make(Labels, 0)
	for _, pairMap := range pairs {
		if len(pairMap) > 0 {
			labels = append(labels, pairMap)
		}
	}

	return &labels
}

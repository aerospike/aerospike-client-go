package aerospike

type Labels struct {
	Labels *[]map[string]string
}

func NewLabels(pairs ...map[string]string) *Labels {
	labels := make([]map[string]string, 0)
	for _, pairMap := range pairs {
		if pairMap != nil || len(pairMap) > 0 {
			labels = append(labels, pairMap)
		}
	}

	mp := Labels{
		Labels: &labels,
	}
	return &mp
}

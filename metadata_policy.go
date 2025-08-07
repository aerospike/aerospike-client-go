package aerospike

import "maps"

type Labels struct {
	Labels map[string]string // Labels is a slice of maps, where each map represents a set of labels.
}

func NewLabels(pairs ...map[string]string) *Labels {
	labels := make(map[string]string, 0)
	for _, pairMap := range pairs {
		if pairMap != nil || len(pairMap) > 0 {
			maps.Copy(labels, pairMap)
		}
	}

	mp := Labels{
		Labels: labels,
	}
	return &mp
}

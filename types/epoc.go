package types

import (
	"time"
)

const (
	CITRUSLEAF_EPOCH = 1262304000
)

// Converts an Expiration time to TTL in seconds
func TTL(secsFromCitrusLeafEpoc int) int {
	return int(int64(CITRUSLEAF_EPOCH+secsFromCitrusLeafEpoc) - time.Now().Unix())
}

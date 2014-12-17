package types

import (
	"time"
)

const (
	// citrusleaf epoc: Jan 01 2010 00:00:00 GMT
	CITRUSLEAF_EPOCH = 1262304000
)

// TTL converts an Expiration time from citrusleaf epoc to TTL in seconds.
func TTL(secsFromCitrusLeafEpoc int) int {
	return int(int64(CITRUSLEAF_EPOCH+secsFromCitrusLeafEpoc) - time.Now().Unix())
}

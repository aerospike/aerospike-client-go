package sdk

// Generation is a named type so a caller can't pass a raw 0 without a
// conversion — Execute rejects a zero-value Generation on IfGeneration.
type Generation uint32

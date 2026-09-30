package sdk

// OnError is a disposition passed to ExecuteOnError, describing what to do
// with per-row failures in a multi-key write.
type OnError struct{}

func Handler(fn func(err error)) *OnError {
	return &OnError{}
}

func InStream() *OnError {
	return &OnError{}
}

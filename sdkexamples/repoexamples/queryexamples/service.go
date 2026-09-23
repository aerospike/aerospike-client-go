package queryexamples

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Service holds the session and dataset every operation needs, injected
// once at construction.
type Service struct {
	session    *sdk.Session
	customerDS *sdk.TypedDataSet[Customer]
}

// NewService wires a Service to the given session and dataset.
func NewService(session *sdk.Session, customerDS *sdk.TypedDataSet[Customer]) *Service {
	return &Service{session: session, customerDS: customerDS}
}

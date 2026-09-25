package commonexamples

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Service groups the session and the untyped dataset every demonstration
// in this package operates against.
type Service struct {
	session *sdk.Session
	ds      *sdk.DataSet
}

func NewService(session *sdk.Session, ds *sdk.DataSet) *Service {
	return &Service{session: session, ds: ds}
}

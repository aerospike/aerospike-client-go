package udf

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Service groups the session, the dataset each demonstration keys into,
// and the registered UDF module handle — nil until RegisterModule has
// run.
type Service struct {
	session *sdk.Session
	ds      *sdk.DataSet
	module  *sdk.UDFModule
}

func NewService(session *sdk.Session, ds *sdk.DataSet) *Service {
	return &Service{session: session, ds: ds}
}

package streamdisposition

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

type Service struct {
	session *sdk.Session
	ds      *sdk.DataSet
}

func NewService(session *sdk.Session, ds *sdk.DataSet) *Service {
	return &Service{session: session, ds: ds}
}

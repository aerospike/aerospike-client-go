package onetomany

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

type Service struct {
	session   *sdk.Session
	agentDS   *sdk.TypedDataSet[Agent]
	listingDS *sdk.TypedDataSet[Listing]
}

func NewService(session *sdk.Session, agentDS *sdk.TypedDataSet[Agent], listingDS *sdk.TypedDataSet[Listing]) *Service {
	return &Service{session: session, agentDS: agentDS, listingDS: listingDS}
}

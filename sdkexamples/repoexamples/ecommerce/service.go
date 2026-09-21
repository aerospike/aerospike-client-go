package ecommerce

import (
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Service holds the session and datasets every operation needs, injected
// once at construction — callers get a Service and call methods on it,
// rather than threading session/dataset params through every function.
type Service struct {
	session    *sdk.Session
	customerDS *sdk.TypedDataSet[Customer]
	productDS  *sdk.TypedDataSet[Product]
	orderDS    *sdk.TypedDataSet[Order]
}

// NewService wires a Service to the given session and datasets.
func NewService(session *sdk.Session, customerDS *sdk.TypedDataSet[Customer], productDS *sdk.TypedDataSet[Product], orderDS *sdk.TypedDataSet[Order]) *Service {
	return &Service{
		session:    session,
		customerDS: customerDS,
		productDS:  productDS,
		orderDS:    orderDS,
	}
}

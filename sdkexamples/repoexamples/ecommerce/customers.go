package ecommerce

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// customerOp marshals a Customer into an UpsertOp; see marshalOp for why.
//
// DX GAP: this function, and marshalOp/applyBins behind it, only exist
// because WriteOp has no constructor that takes Marshal[T]'s output
// directly — a customer with a typed struct and a working Marshal[T]
// still has to hand-write a bridge (loop the BinMap, call .Set() per key)
// to get it into a WriteOp at all. The vendor-side fix would be something
// like UpsertOp(key).From(v) (or a free function taking Marshal[T]'s
// output) so this bridge ships with the SDK instead of being reinvented in
// every application that uses typed writes.
func customerOp(key *as.Key, c Customer) (sdk.WriteOp, error) {
	return marshalOp(key, c, fmt.Sprintf("customer %s", c.ID))
}

// SpenderSummary is one row of the ListTopSpenders report.
type SpenderSummary struct {
	Customer        Customer
	OrderCount      int
	OrderTotalCents int64
}

// ListTopSpenders batch-fetches five customers, then queries each
// customer's orders, returning one summary row per customer rather than
// printing directly — that keeps the fetch/aggregate logic separate from
// how a caller chooses to present it. This runs sequentially; the sdk
// package has no async query variant, so true parallelism here would be a
// goroutine-per-customer plus a sync.WaitGroup, not a client-provided API.
func (s *Service) ListTopSpenders(ctx context.Context) ([]SpenderSummary, error) {
	topIDs := []string{"C-103", "C-107", "C-110", "C-112", "C-117"}
	keys := make([]*as.Key, len(topIDs))
	for i, id := range topIDs {
		keys[i] = sdk.Key(s.customerDS.DataSet(), id)
	}

	stream, err := s.session.BatchGet(ctx, keys, nil)
	if err != nil {
		return nil, fmt.Errorf("batch get top customers: %w", err)
	}
	defer stream.Close()
	topCustomers, err := sdk.Collect[Customer](stream)
	if err != nil {
		return nil, fmt.Errorf("collect top customers: %w", err)
	}

	summaries := make([]SpenderSummary, 0, len(topCustomers))
	for _, customer := range topCustomers {
		orderStream, err := s.session.Query(ctx, s.orderDS.DataSet()).
			WhereAEL(fmt.Sprintf("$.%s == '%s'", orderCustomerIDBin, customer.ID)).
			Execute()
		if err != nil {
			return nil, fmt.Errorf("query orders for customer %s: %w", customer.ID, err)
		}
		// Closed explicitly here rather than deferred — this runs once per
		// customer in the loop, and defer would hold every prior
		// customer's stream open until ListTopSpenders itself returns.
		customerOrders, err := sdk.Collect[Order](orderStream)
		orderStream.Close()
		if err != nil {
			return nil, fmt.Errorf("collect orders for customer %s: %w", customer.ID, err)
		}

		var totalSpent int64
		for _, o := range customerOrders {
			totalSpent += o.TotalCents
		}
		summaries = append(summaries, SpenderSummary{
			Customer:        customer,
			OrderCount:      len(customerOrders),
			OrderTotalCents: totalSpent,
		})
	}
	return summaries, nil
}

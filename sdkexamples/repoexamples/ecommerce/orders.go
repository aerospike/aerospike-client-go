package ecommerce

import (
	"context"
	"errors"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// orderOp marshals an Order into an UpsertOp; see marshalOp for why.
func orderOp(key *as.Key, o Order) (sdk.WriteOp, error) {
	return marshalOp(key, o, fmt.Sprintf("order %s", o.OrderID))
}

// PlaceOrder looks up a customer and product, validates stock, then commits
// the order + stock decrement + balance increment as one heterogeneous
// batch (D-10), returning the order it created. The order ID is generated
// per call rather than hardcoded — a fixed literal would make a second
// call silently overwrite the first order.
func (s *Service) PlaceOrder(ctx context.Context, customerID, sku string, qty int) (Order, error) {
	customerKey := sdk.Key(s.customerDS.DataSet(), customerID)
	productKey := sdk.Key(s.productDS.DataSet(), sku)

	customerRecord, err := s.session.Get(ctx, customerKey, nil)
	if err != nil {
		return Order{}, fmt.Errorf("get customer %s: %w", customerID, err)
	}
	customer, err := sdk.Decode[Customer](customerRecord)
	if err != nil {
		return Order{}, fmt.Errorf("decode customer %s: %w", customerID, err)
	}

	productRecord, err := s.session.Get(ctx, productKey, nil)
	if err != nil {
		return Order{}, fmt.Errorf("get product %s: %w", sku, err)
	}
	product, err := sdk.Decode[Product](productRecord)
	if err != nil {
		return Order{}, fmt.Errorf("decode product %s: %w", sku, err)
	}

	if product.StockQty < qty {
		return Order{}, fmt.Errorf("insufficient stock for %s (available: %d, requested: %d)", product.Name, product.StockQty, qty)
	}

	now := time.Now()
	total := product.PriceCents * int64(qty)
	order := Order{
		OrderID:    fmt.Sprintf("ORD-%d", now.UnixNano()),
		CustomerID: customer.ID,
		SKU:        product.SKU,
		Qty:        qty,
		TotalCents: total,
		Status:     "CONFIRMED",
		Timestamp:  now.UnixMilli(),
	}

	orderKey := sdk.Key(s.orderDS.DataSet(), order.OrderID)
	placeOp, err := orderOp(orderKey, order)
	if err != nil {
		return Order{}, fmt.Errorf("build order op for %s: %w", order.OrderID, err)
	}
	ops := []sdk.WriteOp{
		placeOp,
		sdk.UpdateOp(productKey).Add(productStockBin, -qty),
		sdk.UpdateOp(customerKey).Add(customerBalanceBin, total),
	}
	// DX GAP: BatchWrite returns a raw *WriteStream, so finding out whether
	// the batch actually applied means the customer has to write the same
	// four-step dance every time — check the exec error, Close the
	// stream, walk it checking Affected per entry, then check stream.Err()
	// after the loop (D-19). checkWrite exists purely to keep that dance
	// from being copy-pasted at every write call site; a vendor-provided
	// equivalent (e.g. stream.EnsureAllApplied(ctx) error, or a
	// WriteStream.Collect(ctx) ([]WriteResult, error) mirroring
	// ReadStream.Collect) would mean this application code never needed
	// to write checkWrite at all.
	writeStream, err := s.session.BatchWrite(ctx, ops)
	if err := checkWrite(ctx, writeStream, err, fmt.Sprintf("commit order %s", order.OrderID)); err != nil {
		return Order{}, err
	}

	return order, nil
}

// missingCustomerID is the fixed example of a key that doesn't exist,
// shared by both parts of DemonstrateErrorHandling below — a single
// source of truth rather than a parameter that only one of the two parts
// would actually honor.
const missingCustomerID = "C-MISSING"

// DemonstrateErrorHandling shows two error-handling shapes: a single-key
// lookup checked with errors.Is/errors.As, and a batch of present/missing
// keys checked per row via the stream.
//
// GAP: Session.BatchGet (10.3/10.7) is a flat method — ctx, keys, bins —
// with nowhere to hang IncludeMissingKeys() or ExecuteOnError/InStream/
// Handler; those dispositions only exist on QueryBuilder and
// WriteSegmentBuilder terminals in the PRD, not on BatchGet. The
// stream-level per-row check below (D-6) is the closest available
// equivalent.
func (s *Service) DemonstrateErrorHandling(ctx context.Context) error {
	fmt.Printf("--- Attempting order for non-existent customer: %s ---\n", missingCustomerID)

	customerKey := sdk.Key(s.customerDS.DataSet(), missingCustomerID)
	_, err := s.session.Get(ctx, customerKey, nil)
	switch {
	case errors.Is(err, sdk.ErrNotFound):
		fmt.Printf("Expected error: customer not found: %s\n", missingCustomerID)
		var sdkErr *sdk.Error
		if errors.As(err, &sdkErr) {
			fmt.Printf("  (code=%d op=%s)\n", sdkErr.Code, sdkErr.Op)
		}
	case err != nil:
		return fmt.Errorf("get customer %s: %w", missingCustomerID, err)
	}

	fmt.Println("\nBatchGet over a mix of present and missing keys:")
	keys := []*as.Key{
		sdk.Key(s.customerDS.DataSet(), "C-100"),
		sdk.Key(s.customerDS.DataSet(), missingCustomerID),
		sdk.Key(s.customerDS.DataSet(), "C-ALSO-MISSING"),
	}
	stream, err := s.session.BatchGet(ctx, keys, nil)
	if err != nil {
		return fmt.Errorf("batch get customers: %w", err)
	}
	defer stream.Close()

	// DX GAP, three things worth flagging here:
	//
	// 1. Iter takes its own ctx even though the stream was already
	//    obtained from a ctx-taking call (BatchGet above) — this is the
	//    one place in the whole API that doesn't follow "ctx captured
	//    once at construction, never re-asked at a terminal" (D-1;
	//    WriteSegmentBuilder.Execute() takes no ctx for exactly that
	//    reason). There's a plausible justification — a long scan is
	//    consumed over an open-ended period, and each page-fetch during
	//    iteration is really its own round trip, so a caller might want a
	//    fresh per-page deadline rather than one deadline covering the
	//    whole scan — but nothing documents that this is why streams are
	//    the exception, so it just reads as an inconsistency.
	//
	// 2. A single row can fail two independent ways that have to be
	//    checked separately and in the right order: rowErr (a stream/
	//    transport-level failure fetching this row) and row.Record()'s
	//    own error (a per-key server-side outcome, e.g. not found,
	//    carried inside the *ReadResult itself). Missing either check
	//    silently drops that failure. A single row.Err() folding both
	//    together would remove this as a whole class of mistake.
	//
	// 3. Worse: rowErr can be dropped with zero visible trace. Because
	//    Iter yields (*ReadResult, error) as a range-over-func Seq2,
	//    "for row := range stream.Iter(ctx)" — one loop variable, error
	//    silently discarded — compiles with no error, no vet warning, no
	//    staticcheck finding (verified: both exit clean on this exact
	//    pattern). It reads identically to the completely ordinary,
	//    correct Go idiom for ranging a map or slice — there's no `_` or
	//    any other visible marker that an error is being thrown away, the
	//    way there would be for a plain "v, _ := f()" assignment. Putting
	//    the per-row error inside the yielded value instead (a single
	//    iter.Seq[*ReadResult], error surfaced via Record()/an Err()
	//    accessor) wouldn't make ignoring it impossible — a caller could
	//    still write "rec, _ := row.Record()" — but it would at least
	//    force an explicit, grep-able "_" at the point of ignoring it,
	//    rather than an omission that looks like nothing happened at all.
	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		if _, err := row.Record(); err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			fmt.Println("  OK")
		}
	}
	// stream.Err(), checked after the loop, is a third and distinct check
	// from the call-time err above: it's the only way to tell "the loop
	// stopped because iteration finished cleanly" apart from "the loop
	// stopped because something broke mid-stream" — see StreamOrders,
	// which does this same check for the same reason (D-6).
	if err := stream.Err(); err != nil {
		return fmt.Errorf("batch get customers: %w", err)
	}
	fmt.Println()
	return nil
}

// StreamOrders ranges over the query results with Iter — a pull-based
// range-over-func loop, so consumption rate is naturally backpressured
// with no separate subscribe/request protocol to wire up.
// ExecuteOnError(InStream()) means per-row failures surface as the row's
// own error during iteration instead of aborting the whole stream.
func (s *Service) StreamOrders(ctx context.Context, customerID string) error {
	fmt.Printf("--- Streaming orders for customer %s ---\n", customerID)

	stream, err := s.session.Query(ctx, s.orderDS.DataSet()).
		WhereAEL(fmt.Sprintf("$.%s == '%s'", orderCustomerIDBin, customerID)).
		ExecuteOnError(sdk.InStream())
	if err != nil {
		return fmt.Errorf("query orders for customer %s: %w", customerID, err)
	}
	defer stream.Close()

	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		rec, err := row.Record()
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}
		order, err := sdk.Decode[Order](rec)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}
		fmt.Printf("  Received: %s\n", order)
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("stream orders for customer %s: %w", customerID, err)
	}
	fmt.Println("  Stream complete.")
	fmt.Println()
	return nil
}

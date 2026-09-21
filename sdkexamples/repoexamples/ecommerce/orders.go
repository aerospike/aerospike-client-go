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
// batch (D-10), returning the order it created. Java composes the two
// lookups with CompletableFuture and closes the write with a mid-chain
// insert().update().update().execute(); Go replaces both with plain
// sequential ctx-aware calls plus BatchWrite([]WriteOp) — no
// Future/Promise plumbing needed, and no mid-chain verb pretending to be
// the batch boundary. Unlike the Java original, the order ID is generated
// here rather than hardcoded — a fixed literal would make a second call
// silently overwrite the first order.
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

// DemonstrateErrorHandling shows the Go-idiomatic equivalents of Java's
// three approaches (CompletableFuture.exceptionally, an ErrorHandler
// lambda, and ErrorStrategy.IN_STREAM):
//
//  1. a switch on errors.Is(err, sdk.ErrNotFound) — this is what
//     exceptionally() collapses to once there's no Future to chain;
//  2. GAP: Java attaches includeMissingKeys()/a per-key error handler to
//     a multi-key *query*. Session.BatchGet (10.3/10.7) is a flat method —
//     ctx, keys, bins — with nowhere to hang IncludeMissingKeys() or
//     ExecuteOnError/InStream/Handler. Those dispositions only exist on
//     QueryBuilder and WriteSegmentBuilder terminals in the PRD, not on
//     BatchGet. The closest available equivalent is the stream-level
//     per-row check below (D-6), which also stands in for Java's third,
//     IN_STREAM variant — there's no separate "in-stream" batch mode
//     distinct from just reading Next/Iter results.
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
	fmt.Println()
	return nil
}

// StreamOrders ranges over the query results with Iter — a pull-based
// range-over-func loop gives the same backpressure Java gets from
// Flow.Subscription.request(n), without a separate subscribe/request
// protocol to wire up. ExecuteOnError(InStream()) matches Java's
// ErrorStrategy.IN_STREAM here exactly: per-row failures surface as the
// row's own error during iteration instead of aborting the whole stream.
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

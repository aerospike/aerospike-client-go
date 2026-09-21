package ecommerce

import (
	"context"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Seed data is a small, purpose-picked set — just enough to exercise every
// scenario in this example (a point lookup, a missing key, the five
// "top spender" batch-gets, a per-customer order query, and a stock/price
// range wide enough to exercise both Scan filters) — not a line-for-line
// port of the Java example's full 20/100/54-record catalog. The catalog
// size isn't part of what this example demonstrates.

// Customer{ID, Name, Email, CreditLimitCents, BalanceCents}
var seedCustomers = []Customer{
	{"C-100", "Alice Park", "alice@example.com", 500_000, 0},
	{"C-103", "David Kim", "dkim@example.com", 750_000, 124_000},
	{"C-107", "Henry Nguyen", "henry.n@example.com", 600_000, 210_000},
	{"C-110", "Karen Zhang", "karen.z@example.com", 500_000, 180_750},
	{"C-112", "Mia Garcia", "mia.g@example.com", 800_000, 340_000},
	{"C-117", "Ruby Anderson", "ruby.a@example.com", 550_000, 155_200},
}

// Product{SKU, Name, PriceCents, StockQty, SalePriceCents}
var seedProducts = []Product{
	{"SKU-LAP01", "Ultrabook Laptop", 89_999, 25, 0},    // used by PlaceOrder
	{"SKU-TV55", `55" 4K Smart TV`, 49_999, 20, 0},      // used by RecordProductRatings
	{"SKU-HP01", "Wireless Headphones", 7_999, 150, 0},  // affordable: stock>100, price<10000
	{"SKU-BLN01", "High-Speed Blender", 8_999, 90, 0},   // not affordable: stock<=100
	{"SKU-CBL01", "USB-C Cable 3-Pack", 999, 800, 0},    // overstocked+cheap: sale-price candidate
	{"SKU-TSH01", "Performance T-Shirt", 1_999, 500, 0}, // overstocked+cheap: sale-price candidate
}

// Order{OrderID, CustomerID, SKU, Qty, TotalCents, Status, Timestamp}.
// Timestamp is 0 here and filled in below from DaysAgo, since it depends
// on "now" at seed time.
var seedOrders = []struct {
	Order   Order
	DaysAgo int
}{
	{Order{"ORD-1001", "C-100", "SKU-TV55", 1, 49_999, "CONFIRMED", 0}, 10},
	{Order{"ORD-1002", "C-100", "SKU-HP01", 2, 15_998, "SHIPPED", 0}, 8},
	{Order{"ORD-1010", "C-103", "SKU-LAP01", 1, 89_999, "DELIVERED", 0}, 45},
	{Order{"ORD-1021", "C-107", "SKU-HP01", 1, 7_999, "DELIVERED", 0}, 50},
	{Order{"ORD-1028", "C-110", "SKU-BLN01", 1, 8_999, "DELIVERED", 0}, 35},
	{Order{"ORD-1034", "C-112", "SKU-TV55", 1, 49_999, "DELIVERED", 0}, 55},
	{Order{"ORD-1047", "C-117", "SKU-LAP01", 1, 89_999, "DELIVERED", 0}, 38},
}

// checkWrite closes stream and walks it, returning an error if any entry
// failed or didn't apply. D-19: a batch entry can complete without error
// yet not actually apply (a generation/filter no-op) — Affected is what
// tells the two apart, so this checks it per entry rather than trusting a
// nil error alone. Shared by every Execute()-returning call in this
// package instead of repeating the same walk at each call site.
func checkWrite(ctx context.Context, stream *sdk.WriteStream, execErr error, op string) error {
	if execErr != nil {
		return fmt.Errorf("%s: %w", op, execErr)
	}
	defer stream.Close()
	for result, err := range stream.Iter(ctx) {
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		if !result.Affected {
			return fmt.Errorf("%s: write to %v did not apply", op, result.Key)
		}
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// marshalOp is the shared body behind customerOp/productOp/orderOp: marshal
// v via Marshal[T] (10.10) and apply the result onto a fresh UpsertOp.
// label names the value being marshaled, for the error message only.
func marshalOp[T any](key *as.Key, v T, label string) (sdk.WriteOp, error) {
	bins, err := sdk.Marshal(v)
	if err != nil {
		return sdk.WriteOp{}, fmt.Errorf("marshal %s: %w", label, err)
	}
	return applyBins(sdk.UpsertOp(key), bins), nil
}

// applyBins sets each entry of a marshaled BinMap onto a WriteOp — WriteOp
// itself has no bulk "set these bins" method, only per-field Set, so this
// is the bridge between Marshal[T]'s output (10.10) and a batch write
// entry. customerOp/productOp/orderOp all go through here rather than
// hand-rolling the field<->bin mapping Java's per-type RecordMapper would
// do — Go doesn't need a formal mapper interface for a one-off write.
func applyBins(op sdk.WriteOp, bins as.BinMap) sdk.WriteOp {
	for name, v := range bins {
		op = op.Set(name, v)
	}
	return op
}

// Seed bulk-loads customers, products and orders in one BatchWrite.
//
// GAP: Java's version is one line — session.replace(ds).objects(list).using(mapper).execute()
// — a row-oriented bulk typed write (PRD 10.9, RowWriteBuilder). That
// builder isn't part of the sdk package yet, so this uses the
// already-built BatchWrite([]WriteOp) path instead, mapping each record
// through Marshal[T] by hand. That's a real ergonomics gap for the common
// "load N typed objects" case.
func (s *Service) Seed(ctx context.Context) error {
	fmt.Printf("Seeding %d customers, %d products, %d orders ...\n",
		len(seedCustomers), len(seedProducts), len(seedOrders))

	now := time.Now()
	var ops []sdk.WriteOp
	for _, c := range seedCustomers {
		op, err := customerOp(sdk.Key(s.customerDS.DataSet(), c.ID), c)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}
	for _, p := range seedProducts {
		op, err := productOp(sdk.Key(s.productDS.DataSet(), p.SKU), p)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}
	for _, seed := range seedOrders {
		order := seed.Order
		order.Timestamp = now.Add(-time.Duration(seed.DaysAgo) * 24 * time.Hour).UnixMilli()
		op, err := orderOp(sdk.Key(s.orderDS.DataSet(), order.OrderID), order)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}

	if _, err := s.session.BatchWrite(ctx, ops); err != nil {
		return fmt.Errorf("seed batch write: %w", err)
	}
	return nil
}

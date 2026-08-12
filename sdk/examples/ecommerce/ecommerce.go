//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ecommerce is an order-fulfillment domain model: concurrent reads, a
// batch spanning three sets, streaming scans, map operations and a background
// sale-price pass.
//
// Port of the Java SDK's EcommerceExample, by way of the Rust SDK's
// `ecommerce`. A customer places an order: the customer and the product are
// fetched concurrently, validated, and then the order write, the stock
// decrement and the balance update go out as one batch. The rest of the example
// covers the three ways a missing record can surface, streaming one customer's
// orders, a dashboard built from concurrent queries, CDT map operations for
// product ratings, a filtered product scan, and a background query that applies
// sale prices server-side.
//
// Java composes the async work with CompletableFuture and Flow.Publisher and
// Rust with join!/join_all; Go's answer is goroutines over a Session, which is
// safe for concurrent use.
package ecommerce

import (
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// --- Entities ---------------------------------------------------------------
//
// Customer and Product are mapped by reflection over their struct tags:
// `as:",key"` marks the user key, so it is not written as a bin, and
// `as:"name"` keeps the Java bin names while the Go fields stay idiomatic.

// Customer is a buyer with a credit limit and an outstanding balance.
type Customer struct {
	ID               string `as:",key"`
	Name             string `as:"name"`
	Email            string `as:"email"`
	CreditLimitCents int64  `as:"creditLimit"`
	BalanceCents     int64  `as:"balance"`
}

func (c *Customer) String() string {
	return fmt.Sprintf("Customer[%s, %s, balance=$%s, limit=$%s]",
		c.ID, c.Name, dollars(c.BalanceCents), dollars(c.CreditLimitCents))
}

// Product is a catalogue entry. SalePriceCents is zero until the background
// pass in applySalePrices writes one.
type Product struct {
	SKU            string `as:",key"`
	Name           string `as:"name"`
	PriceCents     int64  `as:"price"`
	StockQty       int64  `as:"stock"`
	SalePriceCents int64  `as:"salePrice"`
}

// IsOnSale reports whether the background pass has discounted this product.
func (p *Product) IsOnSale() bool { return p.SalePriceCents > 0 }

func (p *Product) String() string {
	if p.IsOnSale() {
		return fmt.Sprintf("Product[%s, %s, $%s -> SALE $%s, stock=%d]",
			p.SKU, p.Name, dollars(p.PriceCents), dollars(p.SalePriceCents), p.StockQty)
	}
	return fmt.Sprintf("Product[%s, %s, $%s, stock=%d]",
		p.SKU, p.Name, dollars(p.PriceCents), p.StockQty)
}

// Order takes control of its own mapping by implementing [sdk.RecordMapper],
// which overrides reflection entirely.
//
// That matters here for more than taste: placeOrder needs the same bin layout
// for a hand-built write segment as the typed layer uses for its own writes, and
// ToBins is the one place both read it from, so the two cannot drift apart.
type Order struct {
	OrderID    string
	CustomerID string
	SKU        string
	Qty        int64
	TotalCents int64
	Status     string
	Timestamp  int64
}

// ID reports the user key.
func (o *Order) ID() any { return o.OrderID }

// ToBins reports the bins to write. The order ID travels in the key, so it is
// deliberately absent.
func (o *Order) ToBins() (as.BinMap, error) {
	return as.BinMap{
		"customerId": o.CustomerID,
		"sku":        o.SKU,
		"qty":        o.Qty,
		"totalCents": o.TotalCents,
		"status":     o.Status,
		"timestamp":  o.Timestamp,
	}, nil
}

// SetFromRecord rebuilds an order from a record, recovering the order ID from
// the key the default Behavior sent with the write.
func (o *Order) SetFromRecord(bins as.BinMap, key *as.Key, _ uint32) error {
	o.OrderID = exrun.KeyString(key)
	o.CustomerID = binString(bins["customerId"])
	o.SKU = binString(bins["sku"])
	o.Qty = binInt(bins["qty"])
	o.TotalCents = binInt(bins["totalCents"])
	o.Status = binString(bins["status"])
	o.Timestamp = binInt(bins["timestamp"])
	return nil
}

func (o *Order) String() string {
	return fmt.Sprintf("Order[%s, customer=%s, sku=%s, qty=%d, $%s, %s]",
		o.OrderID, o.CustomerID, o.SKU, o.Qty, dollars(o.TotalCents), o.Status)
}

// shop carries what every step needs: the harness, the three typed sets, and
// whether the cluster can compile AEL text.
type shop struct {
	env       *exrun.Env
	customers *sdk.TypedDataSet[Customer]
	products  *sdk.TypedDataSet[Product]
	orders    *sdk.TypedDataSet[Order]

	// ael records whether server-compiled AEL source is available. The filters
	// below are written both ways so the example runs against older clusters
	// too; the client refuses to send AEL to a cluster below 8.1.3.
	ael bool
}

// Run executes the example.
func Run(env *exrun.Env) error {
	customerSet, err := env.DataSet("customers")
	if err != nil {
		return err
	}
	productSet, err := env.DataSet("products")
	if err != nil {
		return err
	}
	orderSet, err := env.DataSet("orders")
	if err != nil {
		return err
	}

	s := &shop{
		env:       env,
		customers: sdk.TypedDataSetFrom[Customer](customerSet),
		products:  sdk.TypedDataSetFrom[Product](productSet),
		orders:    sdk.TypedDataSetFrom[Order](orderSet),
		ael:       env.Cluster.SupportsServerCompiledAEL(),
	}
	if !s.ael {
		env.Printf("note: this cluster cannot compile AEL text (needs server 8.1.3+), " +
			"so the filters below are sent as client-built expressions.")
	}

	if err := s.seed(); err != nil {
		return err
	}
	if err := s.placeOrder("C-100", "SKU-LAP01", 1); err != nil {
		return err
	}
	if err := s.placeOrderWithErrorHandling("C-MISSING"); err != nil {
		return err
	}
	if err := s.streamOrders("C-100"); err != nil {
		return err
	}
	if err := s.topSpenderDashboard(); err != nil {
		return err
	}
	if err := s.productRatings(); err != nil {
		return err
	}
	if err := s.scanAffordableProducts(); err != nil {
		return err
	}
	if err := s.applySalePrices(); err != nil {
		return err
	}
	// The same scan again, so the background pass's effect is visible.
	return s.scanAffordableProducts()
}

// --- 1. Seeding -------------------------------------------------------------

// seed bulk-loads the demo dataset.
func (s *shop) seed() error {
	customers, products, orders := seedCustomers(), seedProducts(), seedOrders()
	s.env.Printf("Seeding %d customers, %d products, %d orders ...",
		len(customers), len(products), len(orders))

	started := time.Now()
	// Replace, not Upsert, so a re-run leaves no stale bins behind.
	for chunk := range slices.Chunk(customers, 50) {
		if err := drain(s.env.Session.ReplaceTyped(s.customers).Objects(chunk).Execute()); err != nil {
			return err
		}
	}
	for chunk := range slices.Chunk(products, 50) {
		if err := drain(s.env.Session.ReplaceTyped(s.products).Objects(chunk).Execute()); err != nil {
			return err
		}
	}
	for chunk := range slices.Chunk(orders, 50) {
		if err := drain(s.env.Session.ReplaceTyped(s.orders).Objects(chunk).Execute()); err != nil {
			return err
		}
	}
	s.env.Printf("Seed data loaded (%d records) in %s.\n",
		len(customers)+len(products)+len(orders), time.Since(started).Round(time.Millisecond))
	return nil
}

// --- 2. Placing an order ----------------------------------------------------

// placeOrder fetches the customer and the product concurrently, validates
// stock, then writes the order, decrements stock and bills the customer in one
// batch.
func (s *shop) placeOrder(customerID, sku string, qty int64) error {
	s.env.Printf("--- Placing order: customer=%s, sku=%s, qty=%d ---", customerID, sku, qty)

	// Step A and B run at the same time: a Session is safe for concurrent use,
	// so two goroutines need no coordination beyond waiting for both.
	var (
		customer *Customer
		product  *Product
		g        errgroup.Group
	)
	g.Go(func() error {
		var err error
		customer, err = s.readCustomer(customerID)
		return err
	})
	g.Go(func() error {
		var err error
		product, err = s.readProduct(sku)
		return err
	})
	if err := g.Wait(); err != nil {
		return err
	}
	if customer == nil || product == nil {
		return fmt.Errorf("cannot place order: customer %q or product %q is missing", customerID, sku)
	}

	// Step C: validate.
	if product.StockQty < qty {
		s.env.Printf("  Insufficient stock for %s (available: %d)", product.Name, product.StockQty)
		return nil
	}

	total := product.PriceCents * qty
	order := &Order{
		OrderID:    "ORD-2001",
		CustomerID: customer.ID,
		SKU:        product.SKU,
		Qty:        qty,
		TotalCents: total,
		Status:     "CONFIRMED",
		Timestamp:  time.Now().UnixMilli(),
	}

	orderKey, err := s.orders.IDForObject(order)
	if err != nil {
		return err
	}
	productKey := s.products.DataSet().Key(sku)
	customerKey := s.customers.DataSet().Key(customerID)
	bins, err := order.ToBins()
	if err != nil {
		return err
	}

	// Step D: one chain, three verbs, three different sets — the client sends
	// it as a single batch, so the order, the stock and the balance move
	// together.
	segment := s.env.Session.Insert(orderKey)
	for name, value := range bins {
		segment = segment.SetTo(name, value)
	}
	if err := drain(segment.
		Update(productKey).Bin("stock").Add(-qty).
		Update(customerKey).Bin("balance").Add(total).
		Execute()); err != nil {
		return err
	}

	s.env.Printf("Order placed: %s", order)
	updated, err := s.readProduct(sku)
	if err != nil {
		return err
	}
	s.env.Printf("Updated product: %s\n", updated)
	return nil
}

// readCustomer reports one customer, or nil when the record is absent: a point
// read of a missing record yields an empty stream rather than a failure.
func (s *shop) readCustomer(id string) (*Customer, error) {
	key := s.customers.DataSet().Key(id)
	stream, err := s.env.Session.QueryTypedKeys(s.customers, []*as.Key{key}).Execute()
	if err != nil {
		return nil, err
	}
	return stream.FirstObject()
}

// readProduct is [shop.readCustomer] for the product set.
func (s *shop) readProduct(sku string) (*Product, error) {
	key := s.products.DataSet().Key(sku)
	stream, err := s.env.Session.QueryTypedKeys(s.products, []*as.Key{key}).Execute()
	if err != nil {
		return nil, err
	}
	return stream.FirstObject()
}

// --- 3. Error handling ------------------------------------------------------

// placeOrderWithErrorHandling shows the three ways a missing record surfaces:
// as nothing at all, dispatched to a handler, or embedded in the stream.
func (s *shop) placeOrderWithErrorHandling(customerID string) error {
	s.env.Printf("--- Attempting order for non-existent customer: %s ---", customerID)

	// Option A: the point read above simply reports nothing.
	customer, err := s.readCustomer(customerID)
	if err != nil {
		return err
	}
	if customer != nil {
		s.env.Printf("Found: %s", customer)
	} else {
		s.env.Printf("Expected error: customer not found: %s", customerID)
	}

	// Option B: a handler. Actionable failures go to the callback and never
	// reach the stream — but a missing record on a *read* is not actionable, so
	// with IncludeMissingKeys those rows still arrive in-stream, just not as OK.
	s.env.Printf("\nUsing an error handler:")
	keys := s.customers.DataSet().Keys([]string{"C-100", "C-MISSING", "C-ALSO-MISSING"})
	handler := sdk.Handler(func(key *as.Key, index int64, err *sdk.Error) {
		s.env.Printf("  Error at index %d for key %s: %s", index, exrun.KeyString(key), err.Message())
	})
	if err := s.reportRows(s.env.Session.Query(keys).IncludeMissingKeys().ExecuteOnError(handler)); err != nil {
		return err
	}

	// Option C: InStream — every key comes back as a row and the caller
	// inspects each one.
	s.env.Printf("\nUsing the in-stream disposition:")
	keys = s.customers.DataSet().Keys([]string{"C-100", "C-MISSING"})
	if err := s.reportRows(s.env.Session.Query(keys).IncludeMissingKeys().ExecuteOnError(sdk.InStream())); err != nil {
		return err
	}
	s.env.Printf("")
	return nil
}

// reportRows prints one line per row, distinguishing the OK ones.
func (s *shop) reportRows(stream *sdk.RecordStream, err error) error {
	if err != nil {
		return err
	}
	defer stream.Close()
	for row := range stream.Iter() {
		if row.IsOK() {
			s.env.Printf("  OK:      %s", exrun.KeyString(row.Key))
		} else {
			s.env.Printf("  Missing: %s (%s)", exrun.KeyString(row.Key), row.ResultCode)
		}
	}
	return stream.Err()
}

// --- 4. Streaming one customer's orders -------------------------------------

// streamOrders streams one customer's orders with a server-side filter. The
// stream is pull-based, so asking for the next row *is* the backpressure.
func (s *shop) streamOrders(customerID string) error {
	s.env.Printf("--- Streaming orders for customer %s ---", customerID)
	stream, err := s.ordersOf(customerID).Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	count := 0
	for {
		order, err := stream.NextObject()
		if err != nil {
			return err
		}
		if order == nil {
			break
		}
		count++
		s.env.Printf("  Received: %s", order)
	}
	s.env.Printf("  Stream complete: %d order(s).\n", count)
	return nil
}

// --- 5. Dashboard from concurrent queries -----------------------------------

// topSpenderDashboard batch-reads five customers, then runs their order queries
// concurrently.
func (s *shop) topSpenderDashboard() error {
	s.env.Printf("--- Top-spender dashboard (batch read + concurrent queries) ---")

	keys := s.customers.DataSet().Keys([]string{"C-103", "C-107", "C-110", "C-112", "C-117"})
	stream, err := s.env.Session.QueryTypedKeys(s.customers, keys).Execute()
	if err != nil {
		return err
	}
	top, err := stream.IntoObjects()
	if err != nil {
		return err
	}

	// All five order queries are in flight at once; each goroutine writes only
	// its own slot, so the results stay in the customers' order.
	orderLists := make([][]*Order, len(top))
	var g errgroup.Group
	for i, customer := range top {
		id := customer.ID
		g.Go(func() error {
			rows, err := s.ordersOf(id).Execute()
			if err != nil {
				return err
			}
			orderLists[i], err = rows.IntoObjects()
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	for i, customer := range top {
		var total int64
		for _, o := range orderLists[i] {
			total += o.TotalCents
		}
		s.env.Printf("  %-18s  balance=$%9s  orders=%d  order_total=$%s",
			customer.Name, dollars(customer.BalanceCents), len(orderLists[i]), dollars(total))
	}
	s.env.Printf("")
	return nil
}

// --- 6. CDT map operations: product ratings ---------------------------------

// productRatings keeps per-customer ratings in one map bin: writes, a point
// read, a rank read, a value-range count, an update and a removal.
func (s *shop) productRatings() error {
	s.env.Printf("--- Map operations: product ratings for SKU-TV55 ---")
	tv := s.products.DataSet().Key("SKU-TV55")

	// Seven ratings in one atomic operate. Only the first selection needs a
	// creation order: after that the map exists.
	keyOrdered := as.MapOrder.KEY_ORDERED
	if err := first(s.env.Session.Upsert(tv).
		Bin("ratings").OnMapKey("C-100", &keyOrdered).SetTo(5).
		Bin("ratings").OnMapKey("C-101", nil).SetTo(4).
		Bin("ratings").OnMapKey("C-103", nil).SetTo(4).
		Bin("ratings").OnMapKey("C-107", nil).SetTo(3).
		Bin("ratings").OnMapKey("C-112", nil).SetTo(5).
		Bin("ratings").OnMapKey("C-110", nil).SetTo(2).
		Bin("ratings").OnMapKey("C-117", nil).SetTo(4).
		Execute()); err != nil {
		return err
	}
	s.env.Printf("  Added 7 ratings.")

	alice, err := s.readRatings(s.env.Session.Query(tv).
		Bin("ratings").OnMapKey("C-100", nil).GetValues())
	if err != nil {
		return err
	}
	s.env.Printf("  Alice's rating: %s", alice)

	// Rank -1 is the largest value, so a one-entry rank range is the
	// highest-rated entry.
	highest, err := s.readRatings(s.env.Session.Query(tv).
		Bin("ratings").OnMapRankRange(-1, 1).GetKeysAndValues())
	if err != nil {
		return err
	}
	s.env.Printf("  Highest rating entry: %s", highest)

	// How many 4-and-5-star ratings, i.e. values in [4, 6).
	fourPlus, err := s.readRatings(s.env.Session.Query(tv).
		Bin("ratings").OnMapValueRange(4, 6).Count())
	if err != nil {
		return err
	}
	s.env.Printf("  4+ star ratings: %s", fourPlus)

	if err := first(s.env.Session.Upsert(tv).
		Bin("ratings").OnMapKey("C-107", nil).SetTo(5).Execute()); err != nil {
		return err
	}
	s.env.Printf("  Updated C-107's rating to 5.")

	if err := first(s.env.Session.Upsert(tv).
		Bin("ratings").OnMapKey("C-110", nil).Remove().Execute()); err != nil {
		return err
	}
	s.env.Printf("  Removed C-110's rating.")

	fourPlus, err = s.readRatings(s.env.Session.Query(tv).
		Bin("ratings").OnMapValueRange(4, 6).Count())
	if err != nil {
		return err
	}
	s.env.Printf("  4+ star ratings after update: %s\n", fourPlus)
	return nil
}

// readRatings runs a one-operation query and renders what the `ratings`
// operation reported.
func (s *shop) readRatings(q *sdk.QueryBuilder) (string, error) {
	stream, err := q.Execute()
	if err != nil {
		return "", err
	}
	defer stream.Close()
	row, err := stream.FirstOrRaise()
	if err != nil {
		return "", err
	}
	record, err := row.RecordOrRaise()
	if err != nil {
		return "", err
	}
	return exrun.Render(record.Bins["ratings"]), nil
}

// --- 7. Filtered scan -------------------------------------------------------

// scanAffordableProducts scans the product set for affordable, well-stocked
// items.
func (s *shop) scanAffordableProducts() error {
	s.env.Printf("--- Scanning for products: stock > 100 AND price < $100 ---")
	stream, err := s.affordableProducts().Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	count := 0
	for {
		product, err := stream.NextObject()
		if err != nil {
			return err
		}
		if product == nil {
			break
		}
		count++
		sale := ""
		if product.IsOnSale() {
			sale = "  SALE $" + dollars(product.SalePriceCents)
		}
		s.env.Printf("  [%2d] %-35s $%6s  stock=%-3d%s",
			count, product.Name, dollars(product.PriceCents), product.StockQty, sale)
	}
	s.env.Printf("  Scan complete: %d matching products found.\n", count)
	return nil
}

// --- 8. Background sale prices ----------------------------------------------

// applySalePrices discounts overstocked, cheap products server-side.
//
// A background task returns no rows: the whole point is that the client sends
// one query and the server rewrites every match, so nothing streams back.
func (s *shop) applySalePrices() error {
	s.env.Printf("--- Background scan: applying sale prices (stock > 250, price <= $50) ---")

	// price >= $10 -> 80% of price, otherwise 90% of price.
	price := as.ExpIntBin("price")
	discounted := as.ExpCond(
		as.ExpGreaterEq(price, as.ExpIntVal(1000)),
		as.ExpNumDiv(as.ExpNumMul(price, as.ExpIntVal(8)), as.ExpIntVal(10)),
		as.ExpNumDiv(as.ExpNumMul(price, as.ExpIntVal(9)), as.ExpIntVal(10)),
	)

	// The task must target a dataset, not keys.
	query := s.env.Session.Query(s.products.DataSet())
	if s.ael {
		query = query.Where("$.stock > 250 and $.price <= 5000")
	} else {
		query = query.Where(as.ExpAnd(
			as.ExpGreater(as.ExpIntBin("stock"), as.ExpIntVal(250)),
			as.ExpLessEq(price, as.ExpIntVal(5000)),
		))
	}
	task, err := query.
		WithWriteOperations(as.ExpWriteOp("salePrice", discounted, as.ExpWriteFlagDefault)).
		ExecuteBackgroundTask()
	if err != nil {
		return err
	}
	if err := <-task.OnComplete(); err != nil {
		return err
	}
	s.env.Printf("  Sale prices applied.\n")
	return nil
}

// --- Predicates -------------------------------------------------------------

// ordersOf opens a query for one customer's orders.
func (s *shop) ordersOf(customerID string) *sdk.TypedQueryBuilder[Order] {
	q := s.env.Session.QueryTyped(s.orders)
	if s.ael {
		return q.Where(fmt.Sprintf("$.customerId == '%s'", customerID))
	}
	return q.Where(as.ExpEq(as.ExpStringBin("customerId"), as.ExpStringVal(customerID)))
}

// affordableProducts opens the well-stocked, cheap product query.
func (s *shop) affordableProducts() *sdk.TypedQueryBuilder[Product] {
	q := s.env.Session.QueryTyped(s.products)
	if s.ael {
		return q.Where("$.stock > 100 and $.price < 10000")
	}
	return q.Where(as.ExpAnd(
		as.ExpGreater(as.ExpIntBin("stock"), as.ExpIntVal(100)),
		as.ExpLess(as.ExpIntBin("price"), as.ExpIntVal(10000)),
	))
}

// --- Helpers ----------------------------------------------------------------

// drain consumes a write stream, so the writes are known complete and any
// per-record failure surfaces.
func drain(stream *sdk.RecordStream, err error) error {
	if err != nil {
		return err
	}
	defer stream.Close()
	rows, err := stream.Collect()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := row.OrRaise(); err != nil {
			return err
		}
	}
	return nil
}

// first consumes a single-record write stream and raises its row's failure.
func first(stream *sdk.RecordStream, err error) error {
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.FirstOrRaise()
	return err
}

// dollars renders cents as a decimal amount.
func dollars(cents int64) string { return fmt.Sprintf("%.2f", float64(cents)/100.0) }

// binInt reads an integer bin. The server has one integer type, but the core
// client hands it back as whichever Go type held it.
func binInt(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// binString reads a string bin, tolerating an absent one.
func binString(v any) string {
	s, _ := v.(string)
	return s
}

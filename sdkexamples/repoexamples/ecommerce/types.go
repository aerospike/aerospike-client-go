// Package ecommerce is an order-fulfillment example: place an order,
// handle errors, stream a customer's orders, report top spenders, and run
// CDT map ops and a background sale-price scan.
//
// Bin<->field mapping for Marshal[T]/Decode[T] (10.10) uses the `as:"..."`
// struct tag convention shown in the PRD's own Look example for that
// section — `as:",key"` marks the field populated from/excluded to the
// record key, `as:"binname"` gives every other field's bin name. Bin
// names are lowercase per that same example (as:"name", as:"age"), not
// the capitalized Go field name.
package ecommerce

import "fmt"

// Bin names, mirroring the `as:"..."` tags on Customer/Product/Order
// below — kept as constants because raw bin-name strings used in a
// filter, CDT op, or expression can't reference a struct tag directly, so
// this is the single place both sides (the tag and every reference to
// that bin by name) have to agree.
const (
	orderCustomerIDBin = "customerId"

	productPriceBin     = "priceCents"
	productStockBin     = "stockQty"
	productSalePriceBin = "salePriceCents"

	customerBalanceBin = "balanceCents"
)

// Customer is a buyer, keyed by ID in the "customers" dataset.
type Customer struct {
	ID               string `as:",key"`
	Name             string `as:"name"`
	Email            string `as:"email"`
	CreditLimitCents int64  `as:"creditLimitCents"`
	BalanceCents     int64  `as:"balanceCents"`
}

// String renders a Customer for display, e.g. in ListTopSpenders' report.
func (c Customer) String() string {
	return fmt.Sprintf("Customer[%s, %s, balance=$%.2f, limit=$%.2f]",
		c.ID, c.Name, float64(c.BalanceCents)/100.0, float64(c.CreditLimitCents)/100.0)
}

// Product is a catalog item, keyed by SKU in the "products" dataset.
type Product struct {
	SKU            string `as:",key"`
	Name           string `as:"name"`
	PriceCents     int64  `as:"priceCents"`
	StockQty       int    `as:"stockQty"`
	SalePriceCents int64  `as:"salePriceCents"`
}

// IsOnSale reports whether ApplySalePrices has set a sale price.
func (p Product) IsOnSale() bool {
	return p.SalePriceCents > 0
}

// String renders a Product for display, e.g. in ScanAffordableProducts.
func (p Product) String() string {
	if p.IsOnSale() {
		return fmt.Sprintf("Product[%s, %s, $%.2f -> SALE $%.2f, stock=%d]",
			p.SKU, p.Name, float64(p.PriceCents)/100.0, float64(p.SalePriceCents)/100.0, p.StockQty)
	}
	return fmt.Sprintf("Product[%s, %s, $%.2f, stock=%d]",
		p.SKU, p.Name, float64(p.PriceCents)/100.0, p.StockQty)
}

// Order is a placed order, keyed by OrderID in the "orders" dataset.
type Order struct {
	OrderID    string `as:",key"`
	CustomerID string `as:"customerId"`
	SKU        string `as:"sku"`
	Qty        int    `as:"qty"`
	TotalCents int64  `as:"totalCents"`
	Status     string `as:"status"`
	Timestamp  int64  `as:"timestamp"`
}

// String renders an Order for display, e.g. in StreamOrders.
func (o Order) String() string {
	return fmt.Sprintf("Order[%s, customer=%s, sku=%s, qty=%d, $%.2f, %s]",
		o.OrderID, o.CustomerID, o.SKU, o.Qty, float64(o.TotalCents)/100.0, o.Status)
}

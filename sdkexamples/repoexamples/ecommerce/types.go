// Package ecommerce is the Go port of the Java SDK's ecommerce example
// (aerospike-client-java-sdk/examples/.../ecommerce). Per D-27, this is not
// a literal method-for-method translation — it targets the Go API shape
// from sdk/PRD.md, not the Java fluent shape.
//
// GAP: the Java version implements a custom RecordMapper<T> per type
// (fromMap/toMap/id) to control bin<->field mapping. The Go PRD only
// defines Marshal[T]/Decode[T] (10.10) with no documented mapping
// convention (no struct-tag rule, no mapper interface). These structs use
// plain exported fields with the same names as the Java getters would
// imply; how Marshal/Decode would actually resolve bin names is left
// unspecified by the PRD.
package ecommerce

import "fmt"

// Bin names, assumed to equal each field's Go name. Marshal[T]'s actual
// naming convention isn't defined by the PRD, but every raw bin-name
// string used in a filter, CDT op, or expression has to agree with
// whatever convention Marshal ends up using — these constants are that
// single source of truth: change the assumed convention here, not at each
// call site that references a bin by name.
const (
	orderCustomerIDBin = "CustomerID"

	productPriceBin     = "PriceCents"
	productStockBin     = "StockQty"
	productSalePriceBin = "SalePriceCents"

	customerBalanceBin = "BalanceCents"
)

// Customer is a buyer, keyed by ID in the "customers" dataset.
type Customer struct {
	ID               string
	Name             string
	Email            string
	CreditLimitCents int64
	BalanceCents     int64
}

// String renders a Customer for display, e.g. in ListTopSpenders' report.
func (c Customer) String() string {
	return fmt.Sprintf("Customer[%s, %s, balance=$%.2f, limit=$%.2f]",
		c.ID, c.Name, float64(c.BalanceCents)/100.0, float64(c.CreditLimitCents)/100.0)
}

// Product is a catalog item, keyed by SKU in the "products" dataset.
type Product struct {
	SKU            string
	Name           string
	PriceCents     int64
	StockQty       int
	SalePriceCents int64
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
	OrderID    string
	CustomerID string
	SKU        string
	Qty        int
	TotalCents int64
	Status     string
	Timestamp  int64
}

// String renders an Order for display, e.g. in StreamOrders.
func (o Order) String() string {
	return fmt.Sprintf("Order[%s, customer=%s, sku=%s, qty=%d, $%.2f, %s]",
		o.OrderID, o.CustomerID, o.SKU, o.Qty, float64(o.TotalCents)/100.0, o.Status)
}

// Package queryexamples covers the query/CDT/concurrency-control surface
// of the source Java example: generation checks, secondary-index/namespace
// info, list and map CDT operations, sorted/paged reads, and background
// tasks. The source file is a sprawling internal test harness (commented-
// out code, deliberate error triggers, debug prints), not a curated
// example, so this is built concept-by-concept rather than translated
// line-for-line — each concept gets its own clean, focused function.
package queryexamples

import "fmt"

// Bin names, assumed to equal each field's Go name — see the ecommerce
// package's types.go for why this convention exists (Marshal[T]'s actual
// naming convention isn't defined by the PRD).
const (
	customerNameBin = "Name"
	customerAgeBin  = "Age"

	cdtScoresBin    = "Scores"
	cdtTagsBin      = "Tags"
	cdtInventoryBin = "Inventory"
)

// Address is a customer's mailing address.
type Address struct {
	Line1   string
	City    string
	State   string
	Country string
	ZipCode string
}

// String renders an Address for display.
func (a Address) String() string {
	return fmt.Sprintf("Address[%s, %s, %s, %s, %s]", a.Line1, a.City, a.State, a.Country, a.ZipCode)
}

// Customer is a person record, keyed by ID in the "person" dataset.
type Customer struct {
	ID      int64
	Name    string
	Age     int
	Address *Address
}

// String renders a Customer for display.
func (c Customer) String() string {
	return fmt.Sprintf("Customer[id=%d, name=%s, age=%d, address=%v]", c.ID, c.Name, c.Age, c.Address)
}

// Package queryexamples covers the query/CDT/concurrency-control surface
// of the source Java example: generation checks, secondary-index/namespace
// info, list and map CDT operations, sorted/paged reads, and background
// tasks. The source file is a sprawling internal test harness (commented-
// out code, deliberate error triggers, debug prints), not a curated
// example, so this is built concept-by-concept rather than translated
// line-for-line — each concept gets its own clean, focused function.
package queryexamples

import "fmt"

// Bin names, mirroring the `as:"..."` tags on Customer/Address below — see
// the ecommerce package's types.go for why these are kept as separate
// constants rather than read off the tags directly.
const (
	customerNameBin = "name"
	customerAgeBin  = "age"

	cdtScoresBin    = "scores"
	cdtTagsBin      = "tags"
	cdtInventoryBin = "inventory"
)

// Address is a customer's mailing address.
type Address struct {
	Line1   string `as:"line1"`
	City    string `as:"city"`
	State   string `as:"state"`
	Country string `as:"country"`
	ZipCode string `as:"zipCode"`
}

// String renders an Address for display.
func (a Address) String() string {
	return fmt.Sprintf("Address[%s, %s, %s, %s, %s]", a.Line1, a.City, a.State, a.Country, a.ZipCode)
}

// Customer is a person record, keyed by ID in the "person" dataset.
type Customer struct {
	ID      int64    `as:",key"`
	Name    string   `as:"name"`
	Age     int      `as:"age"`
	Address *Address `as:"address"`
}

// String renders a Customer for display.
func (c Customer) String() string {
	return fmt.Sprintf("Customer[id=%d, name=%s, age=%d, address=%v]", c.ID, c.Name, c.Age, c.Address)
}

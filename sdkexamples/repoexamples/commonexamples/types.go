// Package commonexamples covers the general-purpose Session surface the
// source Java CommonExample.java exercises: index management, point
// existence/touch/delete shortcuts, batch touch/delete, and filtered-write
// semantics. Unlike ecommerce and queryexamples, this package uses the
// plain untyped DataSet (10.2) rather than TypedDataSet[T] — nothing here
// needs struct mapping, and the untyped construction path (NewDataSet,
// MustNewDataSet) was otherwise untouched by both other packages.
package commonexamples

const (
	nameBin = "name"
	ageBin  = "age"
)

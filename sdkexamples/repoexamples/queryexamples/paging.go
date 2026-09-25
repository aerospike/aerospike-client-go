package queryexamples

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateSortedPaging scans customers, sorts them client-side (age
// descending, then name ascending), pages through the results, then
// re-sorts by name alone and pages through again — the PRD's
// NavigatableStream vocabulary (10.14) in full: IntoNavigatable, SortBy
// (variadic, so multi-field sort is real), PageSize, HasMorePages,
// HasNext, Next.
//
// GAP: three things the source Java example does have no PRD equivalent:
//  1. NavigatableStream isn't a Closer — no Close() method exists at all,
//     unlike ReadStream/WriteStream (D-17). Java treats its navigable
//     stream as a try-with-resources Closeable. Whether the underlying
//     ReadStream should still be closed once IntoNavigatable() is called,
//     or whether ownership transfers to the navigable stream, isn't
//     specified either way — so this doesn't close anything after
//     IntoNavigatable, rather than guess which is correct.
//  2. Jumping to a specific page number (Java: navStream.setPageTo(2)) —
//     no equivalent method exists; only forward iteration via
//     HasMorePages/HasNext/Next is defined.
//  3. Case-insensitive ascending sort (Java:
//     SortProperties.ascendingIgnoreCase("name")) — only plain Asc/Desc
//     exist, no case-insensitivity option.
func (s *Service) DemonstrateSortedPaging(ctx context.Context) error {
	stream, err := s.session.Scan(ctx, s.customerDS.DataSet()).
		Limit(13).
		Execute()
	if err != nil {
		return fmt.Errorf("scan customers: %w", err)
	}

	nav, err := stream.IntoNavigatable()
	if err != nil {
		return fmt.Errorf("get navigatable stream: %w", err)
	}
	nav.SortBy(sdk.Desc(customerAgeBin), sdk.Asc(customerNameBin)).PageSize(5)

	if err := printPages(nav, "page"); err != nil {
		return err
	}

	nav.SortBy(sdk.Asc(customerNameBin))
	return printPages(nav, "re-sorted page")
}

// DX GAP: this nested HasMorePages/HasNext/Next loop is the one place in
// the whole package that can't detect a stream-level failure at all.
// ReadStream and WriteStream both got Iter(ctx) iter.Seq2[T, error] plus
// Err() checked after the loop (the same D-6 checkpoint made consistent
// everywhere else in this codebase) — NavigatableStream got neither.
// Next() returns a bare *ReadResult, no error slot, and there's no Err()
// method to check afterward. If a page fetch fails mid-stream,
// HasMorePages() presumably just returns false and this loop ends looking
// identical to a clean finish — there's no way to tell the two apart, not
// because a caller might ignore the signal (like the droppable rowErr
// case elsewhere), but because the signal has nowhere to exist in the
// first place. This is also why printPages has to be a separate helper
// at all: a flat "for row, err := range nav.Iter(ctx)" would let both
// call sites in DemonstrateSortedPaging just range directly, the same way
// every other stream consumption in this package already does.
func printPages(nav *sdk.NavigatableStream, label string) error {
	page := 0
	for nav.HasMorePages() {
		page++
		fmt.Printf("---- %s %d ----\n", label, page)
		for nav.HasNext() {
			row := nav.Next()
			rec, err := row.Record()
			if err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}
			customer, err := sdk.Decode[Customer](rec)
			if err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}
			fmt.Println(" ", customer)
		}
	}
	return nil
}

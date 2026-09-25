# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "DX GAP" *.go`) at the
call site where they bite. This file is the one cross-cutting finding that
ties most of them together.

## Stream/task ceremony is glued into almost every method

Numbers, not a vibe: every file in this package touches stream or task
lifecycle. 26 separate `Close`/`Iter`/`stream.Err()`/`checkWrite`/`Collect`/
`Wait` statements across 633 lines — about one every 24 lines — in a
package whose actual job is "check stock, decrement inventory, aggregate
spend, tag a rating." None of that is inherently about streaming.

5 of the 8 `Service` methods have hand-rolled stream/task lifecycle code
sitting directly in the business logic. The rest only look cleaner because
they delegate to `checkWrite` — which hides `Close()` so well a careful
reader can't tell it's happening at all.

**This isn't the example overusing streams. It's the root cause behind
nearly every real bug this review found:**

- `ListTopSpenders` leaked two `ReadStream`s (one nested in a loop)
- `DemonstrateErrorHandling` skipped the `stream.Err()` checkpoint that
  `StreamOrders` two functions over remembered to do
- `RecordProductRatings` leaked three `WriteStream`s and skipped the
  `Affected` check `PlaceOrder` does correctly, for the same reason
- `checkWrite` closes the stream invisibly, with nothing at the call site
  signaling that it owns cleanup
- `Seed` doesn't check `Affected` on its batch write at all
- nothing stops a caller from writing `for row := range stream.Iter(ctx)`
  and silently dropping every per-row error — it compiles clean, `go vet`
  and `staticcheck` both pass it
- nothing stops a caller from ignoring `Task` entirely after
  `ExecuteBackgroundTask()` — same silent-drop problem, one level up

Six different bugs, one shape: **the SDK has no higher-level primitive for
the two things every non-trivial call needs — "read everything and close
cleanly" and "write everything and verify it all applied."** So every call
site re-derives that ceremony by hand, slightly differently each time, and
it's easy to get wrong even under active review — this session proved that
repeatedly.

### What would actually fix it

- A `WriteStream` convenience mirroring `ReadStream.Collect` — something
  like `stream.EnsureAllApplied(ctx) error` — so `checkWrite` doesn't need
  to exist as customer-authored code.
- `Collect[T]`/`Iter` that close the stream themselves, or a variant that
  does, so "get everything" isn't a 3-step manual dance every time.
- Accept that Go can't force `Task.Wait` or per-row error checks — the
  honest fix there is a `bodyclose`-style static analyzer shipped with the
  SDK, not anything expressible in the API's own signatures.

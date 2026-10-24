# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file is a short index of the two found here,
since neither had previously been recorded in a consolidated tracking
document — one is genuinely new, the other was a restatement of an
existing finding that had itself only ever lived in a single inline
comment elsewhere.

## No write-policy plumbed through on list/map CDT mutations (sdk/-layer gap, corrected) — then the underlying method itself was removed

`AddListing` needs Java's `listAppend(id, opts -> opts.addUnique().allowFailures())`
— don't add a duplicate, don't fail the whole call if this one add can't
happen. First written up here as "Go has nothing like this at all," which
was wrong: Go's own classic client (`as.NewListPolicy`,
`as.ListWriteFlagsAddUnique`, in `cdt_list.go`) already matches Java's
`ListPolicy`/`ListWriteFlags` exactly. The real, narrower gap was
initially framed as `ListAppendItems(items []any)`
(`sdk/writesegmentbuilder.go`) having no parameter to pass that policy
through.

That framing is now moot: `ListAppendItems` (and 17 sibling `List*`/`Map*`
whole-collection methods on `WriteBinBuilder`) has been removed entirely
— a user question ("where did you even get `ListAppendItems`?") prompted
checking whether the *base* method was PRD-grounded at all, and it
wasn't (`sdk/FUNCTIONAL_GAPS.md` finding #25). `AddListing` in `relate.go`
now does a read-modify-write (`Get` the agent's current list, append
client-side, `Set` the whole bin back) instead — not atomic in the CDT
sense at all anymore (not just missing a policy flag), relying entirely
on `RunInTransaction`'s MRT wrapping for consistency. Losing the CDT-level
duplicate-detection question above is now moot too: there's no CDT append
happening at all to ask it about.

See `sdk/FUNCTIONAL_GAPS.md` findings #15 and #25 for the full corrected
history, including how the original overstatement happened (a stale
cookbook method name led to comparing against something that doesn't
exist in current Java either).

## WriteResult has no value field — CDT-count confirmation is unreliable, not just weaker

`DeleteListing` needs to know whether the listing id was actually present
in the agent's list (Java: `onListValue(id).removeAnd().count() > 0`). Go
compiles the identical chain (`OnListValue(id).RemoveAnd().Count()`), but
its terminal, `ExecuteOne() (WriteResult, error)`, has no field for the
count value — only `Affected` (bool). Worse than just losing precision:
the PRD never specifies what `Affected` means for a CDT op that matches
nothing, so it isn't confirmed to even correlate with "was it in the
list" versus "did the write round-trip succeed regardless." Treated as
best-effort in `relate.go`, not as a real confirmation.

This is the same root limitation already flagged, inline only, in
`ecommerce/products.go`'s `RecordProductRatings` GAP comment — not a new
finding, but this is the first place it's cross-referenced rather than
independently rediscovered.

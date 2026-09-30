# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file records the one finding that's more of a
missing primitive than a missing method.

## Batch read results carry no key identity

`batchExists` in `batchops.go` needs to report, per key, whether it
exists — the same thing the source Java example does (`Key: %s -> %b` for
every row). It can't. `ReadResult` (`sdk/stream.go`) has exactly one
method, `Record() (*Record, error)`; `Record` itself (`sdk/session.go`)
carries `Generation` and `Expiration`, nothing else. Neither type holds
the key a row came from.

`WriteResult` isn't like this — it has a `Key *as.Key` field, so
`batchTouch`/`batchDelete` in the same file *can* print `Key: %v -> %v`
per row, no problem. The asymmetry is the point: the write path already
solved "which key does this result belong to," and the read path just
never did.

Correlating rows back to the input `keys` slice by position isn't a safe
workaround either — nothing in the PRD says `BatchGet`/`Query`/`Scan`
results preserve input-key order, and Aerospike batch reads are typically
split and reassembled per-node internally, so assuming index `i` of the
results matches index `i` of the request is exactly the kind of thing that
works in a stub and breaks against a real cluster.

So `batchExists` reports an aggregate count (`3 of 5 keys exist`) instead
of a per-key breakdown — the honest version of what's actually knowable
here, not a fabricated or guessed key.

### What would actually fix it

- Add `Key *as.Key` to `ReadResult` (or to `Record`), mirroring
  `WriteResult` exactly. This is a small, symmetric completion of an
  already-decided shape (D-24 already split `ReadResult`/`WriteResult`/
  `UDFResult` apart specifically so each could carry what it needs) —
  `WriteResult` just needs it and `ReadResult` doesn't have it yet, not a
  new design.

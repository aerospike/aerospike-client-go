# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file is a short index of the one found here.

Note: an earlier pass through this package reported "no gaps found" —
that was wrong, caught while restoring exact functional parity with the
source Java test's four isolated scenarios. The gap below was always
there; it just hadn't surfaced yet because the first version of
`DemonstrateWriteIfNotExists` never attempted the verification step that
exposes it.

## Record has no bin-accessor methods — can't verify a UDF write's effect

Hits twice in this package:

- `DemonstrateWriteUsingUdf`: the source Java test queries the key after
  the `writeBin` call and reads the bin back (`rec.getString(binName)`)
  to confirm the write actually happened, not just that the call was
  accepted.
- `DemonstrateWriteIfNotExists`: the source Java test calls `writeUnique`
  twice, then reads the bin back to confirm the *second* call was a
  no-op — the value is still `"first"`, not overwritten by `"second"`.

Neither can be done here: `Record` (`sdk/session.go`) is completely
opaque, no bin-accessor methods at all (D-8's `rec.String(bin)`-style
accessors are P2 and not implemented in the current stub), and
`Decode[T]` needs a typed struct, which this plain untyped dataset
doesn't use. Both demos can only confirm the UDF call was accepted, not
what it actually left in the record — a narrower facet of the same
Record-opacity gap already documented for generation/expiration
(`sdk/FUNCTIONAL_GAPS.md` finding #16), this time for bin values instead.

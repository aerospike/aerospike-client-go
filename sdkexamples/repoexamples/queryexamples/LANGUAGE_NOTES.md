# Language-level comparison notes

Unlike `DX GAP`/`GAP` comments (SDK-specific, fixable by changing `sdk/`),
these are Java vs Go differences inherent to the languages themselves —
nothing in an SDK's design changes them.

## Per-call error checks vs try/catch

Go has no exceptions, so every fallible call gets its own explicit check:

```go
bins, err := sdk.Marshal(original)
if err != nil {
    return fmt.Errorf("marshal customer %d: %w", id, err)
}
```

Java can wrap a whole sequence (`marshal` + `put` + `get` + `decode`) under
one `try/catch`. Go can't — see `objectmapping.go`'s `DemonstrateObjectMapping`,
four such checks in a row. This is normal, idiomatic Go, not boilerplate to
eliminate; no SDK change fixes it. `%w` wrapping and a per-step message
(`"marshal customer %d"` vs `"put customer %d"`) are the idiomatic
mitigation, both used throughout this package and ecommerce.

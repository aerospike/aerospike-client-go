// Package replaceifexists demonstrates ReplaceIfExists — the
// record-exists-policy entry verb that replaces a record's entire
// content only if it already exists, refusing to create it — matching
// the real SDK's own ReplaceTest.java (active): replaceOnly() (on a
// missing key, expects KEY_NOT_FOUND_ERROR) and
// replaceOnlyModifiesOpType() (on an existing key, replaces the whole
// record's bins, not merges them).
//
// Source: examples/ and the use-case-cookbook never touch
// ReplaceIfExists at all — confirmed by direct grep across both, same
// as every other package under sdktestexamples/. The only real usage
// found anywhere is this active test.
package replaceifexists

const (
	binName1 = "bin1"
	binName2 = "bin2"
	binName3 = "bin3"
)

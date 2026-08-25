/*
 * Copyright 2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Modify operations: mutate the bin in place. Each call below performs the
// modify op then re-reads the bin to display the new value.
// Requires server version 8.1.3 or later.

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func runOperateStringModify() error {
	key, err := as.NewKey(ns, set, "opstr_modify")
	if err != nil {
		return err
	}

	policy := as.DefaultStringPolicy

	// insert — splice value at codepoint index
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrInsertOp(policy, "text", 5, " beautiful")); err != nil {
		return err
	}
	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`insert(5, " beautiful") -> %q`, record.Bins["text"])

	// overwrite — replace codepoints starting at index
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrOverwriteOp(policy, "text", 6, "earth")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`overwrite(6, "earth") -> %q`, record.Bins["text"])

	// concat(value) — append a single string
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrConcatOp(policy, "text", "!")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`concat("!") -> %q`, record.Bins["text"])

	// concat(values) — append each list element in order
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrConcatListOp(policy, "text", []string{" ", "big", " world"})); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`concat([" ", "big", " world"]) -> %q`, record.Bins["text"])

	// append — Unicode-aware end-append (alongside the legacy AppendOp)
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrAppendOp(policy, "text", "!")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`append("!") -> %q`, record.Bins["text"])

	// prepend — Unicode-aware front-insert (alongside the legacy PrependOp)
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrPrependOp(policy, "text", "hello ")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`prepend("hello ") -> %q`, record.Bins["text"])

	// snip — remove half-open codepoint range
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello beautiful world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrSnipOp(policy, "text", 5, 15)); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("snip(5, 15) -> %q", record.Bins["text"])

	// replace — first occurrence only
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrReplaceOp(policy, "text", "world", "earth")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`replace("world", "earth") -> %q`, record.Bins["text"])

	// replaceAll — every occurrence
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "aabaa")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrReplaceAllOp(policy, "text", "a", "x")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`replaceAll("a", "x") -> %q`, record.Bins["text"])

	// upper
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrUpperOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("upper() -> %q", record.Bins["text"])

	// lower
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "HELLO WORLD")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrLowerOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("lower() -> %q", record.Bins["text"])

	// caseFold — locale-independent fold for comparison keys
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "HELLO World")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrCaseFoldOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("caseFold() -> %q", record.Bins["text"])

	// normalizeNFC — Unicode NFC normalization
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "café")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrNormalizeNFCOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("normalizeNFC() -> %q", record.Bins["text"])

	// trimStart — drop leading whitespace
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "  hello  ")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrTrimStartOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("trimStart() -> %q", record.Bins["text"])

	// trimEnd — drop trailing whitespace
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "  hello  ")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrTrimEndOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("trimEnd() -> %q", record.Bins["text"])

	// trim — drop both ends
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "  hello world  ")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrTrimOp(policy, "text")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("trim() -> %q", record.Bins["text"])

	// padStart — left-pad up to target codepoint length
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrPadStartOp(policy, "text", 10, "*")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`padStart(10, "*") -> %q`, record.Bins["text"])

	// padEnd — right-pad up to target codepoint length
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrPadEndOp(policy, "text", 10, ".")); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`padEnd(10, ".") -> %q`, record.Bins["text"])

	// repeat — repeat string n times
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "ab")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrRepeatOp(policy, "text", 3)); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("repeat(3) -> %q", record.Bins["text"])

	// regexReplace — pass GLOBAL to replace every match (default replaces first only)
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "abc123def456")); err != nil {
		return err
	}
	if _, err := client.Operate(nil, key, as.StrRegexReplaceOp(policy, "text", "[0-9]+", "NUM", as.StringRegexGlobal)); err != nil {
		return err
	}
	record, err = client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf(`regexReplace("[0-9]+", "NUM", GLOBAL) -> %q`, record.Bins["text"])
	return nil
}

//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package studentscores queries per-student score maps with an AEL map
// predicate.
//
// Port of the Java SDK's StudentScoresExample, by way of the Rust SDK's
// `student_scores`. Thirty student records carry a `scores` bin holding a map of
// subject to mark; the set is then scanned with the server-compiled AEL filter
// `$.scores.{=90:}.count() > 0` — "at least one score of 90 or better" — which
// needs no secondary index.
package studentscores

import (
	"fmt"
	"sort"

	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// subjects is the map's key set; every student is marked in all five.
var subjects = []string{"math", "english", "science", "history", "art"}

// Run executes the example.
func Run(env *exrun.Env) error {
	if !env.Cluster.SupportsServerCompiledAEL() {
		env.Printf("skipping student scores: AEL filters require server 8.1.3 or later")
		return nil
	}

	class10a, err := env.DataSet("class10a")
	if err != nil {
		return err
	}

	random := newJavaRandom(42)
	for i := 1; i <= 30; i++ {
		key := class10a.Key(fmt.Sprintf("student-%d", i))
		stream, err := env.Session.Upsert(key).
			Bin("name").SetTo(fmt.Sprintf("Student %d", i)).
			Bin("scores").SetTo(generateScores(random)).
			Execute()
		if err != nil {
			return err
		}
		if _, err := stream.FirstOrRaise(); err != nil {
			return err
		}
	}
	env.Printf("Wrote 30 student records to set %s", class10a.SetName())

	// AEL, compiled by the server: count the map values in [90, +inf) and keep
	// the record when that count is positive.
	env.Printf("Students with at least one score >= 90:")
	stream, err := env.Session.Query(class10a).
		Where("$.scores.{=90:}.count() > 0").
		Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	var lines []string
	for row := range stream.Iter() {
		record, err := row.RecordOrRaise()
		if err != nil {
			return err
		}
		lines = append(lines, fmt.Sprintf("  %v: %s",
			record.Bins["name"], exrun.Render(record.Bins["scores"])))
	}
	if err := stream.Err(); err != nil {
		return err
	}
	sort.Strings(lines)
	for _, line := range lines {
		env.Printf("%s", line)
	}
	env.Printf("  (%d of 30 matched)", len(lines))
	return nil
}

// generateScores marks every subject in [55, 100].
func generateScores(random *javaRandom) map[string]int {
	scores := make(map[string]int, len(subjects))
	for _, subject := range subjects {
		scores[subject] = 55 + int(random.nextInt(46))
	}
	return scores
}

// javaRandom is java.util.Random, reimplemented so this example produces
// exactly the same marks as the Java original (which seeds with 42).
type javaRandom struct {
	seed int64
}

const (
	javaRandomMultiplier = 0x5DEECE66D
	javaRandomMask       = (1 << 48) - 1
)

func newJavaRandom(seed int64) *javaRandom {
	return &javaRandom{seed: (seed ^ javaRandomMultiplier) & javaRandomMask}
}

func (r *javaRandom) next(bits uint) int32 {
	r.seed = (r.seed*javaRandomMultiplier + 0xB) & javaRandomMask
	return int32(r.seed >> (48 - bits))
}

// nextInt mirrors nextInt(bound), including the rejection loop that keeps the
// distribution uniform for bounds that are not powers of two.
func (r *javaRandom) nextInt(bound int32) int32 {
	limit := bound - 1
	if bound&limit == 0 {
		return int32((int64(bound) * int64(r.next(31))) >> 31)
	}
	for {
		bits := r.next(31)
		value := bits % bound
		if bits-value+limit >= 0 {
			return value
		}
	}
}

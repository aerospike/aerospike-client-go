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

// Package behaviorhierarchy shows hierarchical behaviors and what they resolve
// to.
//
// Port of the Java SDK's BehaviorHierarchicalExample, by way of the Rust SDK's
// `behavior_hierarchical`. It builds a small hierarchy — two roots plus a child
// that inherits from one of them — reports the settings each behavior resolves
// to for the operation coordinates the Java example prints, dumps one
// behavior's full resolution with Explain, and then runs a real write and read
// through a session bound to each behavior.
//
// The Java example drives the same hierarchy from a monitored YAML file. Here
// it is built programmatically with DeriveWithChanges, because the file-driven
// half needs the process-global AEROSPIKE_SDK_CONFIG_URL variable and so
// belongs in its own example.
package behaviorhierarchy

import (
	"fmt"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Run executes the example.
func Run(env *exrun.Env) error {
	set, err := env.DataSet("behavior_hierarchical")
	if err != nil {
		return err
	}

	// Two roots derived from DEFAULT, and one child of the first that overrides
	// only its batch settings.
	//
	// Write settings go in ScopeWritesRetryable to match the Java and Rust
	// examples, which is also what makes the reporting below comparable. Note
	// that in Go every write the SDK issues is classified non-retryable, so a
	// retryable-write patch resolves but never applies to a real operation:
	// ScopeWrites or ScopeWritesNonRetryable is where tuning belongs.
	highPerformance := sdk.DefaultBehavior().DeriveWithChanges("example-high-performance",
		map[sdk.Scope]sdk.Settings{
			sdk.ScopeAll: {
				TotalTimeout:  sdk.DurationPtr(2 * time.Second),
				SocketTimeout: sdk.DurationPtr(200 * time.Millisecond),
				MaxRetries:    sdk.IntPtr(2),
				RetryDelay:    sdk.DurationPtr(0),
			},
			sdk.ScopeReadsBatch: {
				MaxConcurrentNodes: sdk.IntPtr(8),
				AllowInline:        sdk.BoolPtr(true),
				AllowInlineSSD:     sdk.BoolPtr(true),
			},
			sdk.ScopeReadsQuery: {
				RecordQueueSize:    sdk.IntPtr(10_000),
				MaxConcurrentNodes: sdk.IntPtr(0),
			},
			sdk.ScopeWritesRetryable: {
				DurableDelete: sdk.BoolPtr(false),
				MaxRetries:    sdk.IntPtr(1),
			},
		})

	highReliability := sdk.DefaultBehavior().DeriveWithChanges("example-high-reliability",
		map[sdk.Scope]sdk.Settings{
			sdk.ScopeAll: {
				TotalTimeout:  sdk.DurationPtr(10 * time.Second),
				SocketTimeout: sdk.DurationPtr(2 * time.Second),
				MaxRetries:    sdk.IntPtr(5),
				RetryDelay:    sdk.DurationPtr(100 * time.Millisecond),
			},
			sdk.ScopeWritesRetryable: {
				DurableDelete: sdk.BoolPtr(true),
			},
			sdk.ScopeReadsSC: {
				ReadModeSC: sdk.ReadModeSCPtr(as.ReadModeSCLinearize),
			},
			sdk.ScopeReadsQuery: {
				RecordQueueSize: sdk.IntPtr(2_000),
			},
		})

	// A child inherits everything it does not restate: the point-read budget
	// below comes from example-high-performance, the batch numbers are this
	// behavior's own.
	batchOptimized := highPerformance.DeriveWithChanges("example-batch-optimized",
		map[sdk.Scope]sdk.Settings{
			sdk.ScopeReadsBatch: {
				MaxConcurrentNodes: sdk.IntPtr(16),
				AllowInline:        sdk.BoolPtr(false),
				AllowInlineSSD:     sdk.BoolPtr(true),
			},
			sdk.ScopeWritesBatch: {
				MaxConcurrentNodes: sdk.IntPtr(16),
				TotalTimeout:       sdk.DurationPtr(5 * time.Second),
			},
		})

	development := sdk.DefaultBehavior().DeriveWithChanges("example-development",
		map[sdk.Scope]sdk.Settings{
			sdk.ScopeAll: {
				TotalTimeout:  sdk.DurationPtr(30 * time.Second),
				SocketTimeout: sdk.DurationPtr(5 * time.Second),
				MaxRetries:    sdk.IntPtr(0),
			},
		})

	behaviors := []*sdk.Behavior{highPerformance, highReliability, batchOptimized, development}

	env.Printf("=== Resolved behavior settings ===")
	for _, behavior := range behaviors {
		displayBehaviorSettings(env, behavior)
	}

	// The DEFAULT behavior every derived one inherits from.
	env.Printf("")
	displayBehaviorSettings(env, sdk.DefaultBehavior())

	env.Printf("")
	env.Printf("=== Inheritance: example-batch-optimized (child) vs example-high-performance (parent) ===")
	parentBatch := highPerformance.Settings(sdk.OpRead, sdk.ShapeBatch, sdk.ModeAP)
	childBatch := batchOptimized.Settings(sdk.OpRead, sdk.ShapeBatch, sdk.ModeAP)
	env.Printf("  batch reads  max_concurrent_nodes: parent=%s  child=%s (overridden)",
		opt(parentBatch.MaxConcurrentNodes), opt(childBatch.MaxConcurrentNodes))
	env.Printf("  batch reads  allow_inline:         parent=%s  child=%s (overridden)",
		opt(parentBatch.AllowInline), opt(childBatch.AllowInline))
	parentPoint := highPerformance.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP)
	childPoint := batchOptimized.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP)
	env.Printf("  point reads  total_timeout:        parent=%s  child=%s (inherited)",
		dur(parentPoint.TotalTimeout), dur(childPoint.TotalTimeout))

	// Explain dumps the behavior's own patches plus the full resolved matrix.
	env.Printf("")
	env.Printf("=== Explain() for example-batch-optimized ===")
	env.Printf("%s", strings.TrimRight(batchOptimized.Explain(), "\n"))

	// A behavior is not a report: a session bound to one does real work under
	// exactly the settings printed above.
	env.Printf("")
	env.Printf("=== Operations per behavior ===")
	for _, behavior := range behaviors {
		session, err := env.Cluster.CreateSession(behavior)
		if err != nil {
			return err
		}
		key := set.Key(behavior.Name())
		stream, err := session.Upsert(key).
			Bin("behavior").SetTo(behavior.Name()).
			Bin("value").SetTo(42).
			Execute()
		if err != nil {
			return err
		}
		if _, err := stream.FirstOrRaise(); err != nil {
			return err
		}
		rec, err := session.Get(key, sdk.AllBins)
		if err != nil {
			return err
		}
		env.Printf("  %s: wrote and read back behavior=%v value=%v",
			behavior.Name(), rec.Bins["behavior"], rec.Bins["value"])
	}
	return nil
}

// displayBehaviorSettings prints the settings a behavior resolves to for the
// operation coordinates the Java example reports.
func displayBehaviorSettings(env *exrun.Env, behavior *sdk.Behavior) {
	env.Printf("")
	env.Printf("  %s:", behavior.Name())
	if parent := behavior.Parent(); parent != nil {
		env.Printf("    parent: %s", parent.Name())
	}

	allOps := behavior.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP)
	env.Printf("    point reads (AP):")
	env.Printf("      total_timeout:        %s", dur(allOps.TotalTimeout))
	env.Printf("      socket_timeout:       %s", dur(allOps.SocketTimeout))
	env.Printf("      max_retries:          %s", opt(allOps.MaxRetries))
	env.Printf("      retry_delay:          %s", dur(allOps.RetryDelay))

	writes := behavior.Settings(sdk.OpWriteRetryable, sdk.ShapePoint, sdk.ModeAP)
	env.Printf("    retryable writes:")
	env.Printf("      durable_delete:       %s", opt(writes.DurableDelete))
	env.Printf("      max_retries:          %s", opt(writes.MaxRetries))
	env.Printf("      commit_level:         %s", commitLevel(writes.CommitLevel))

	batchReads := behavior.Settings(sdk.OpRead, sdk.ShapeBatch, sdk.ModeAP)
	env.Printf("    batch reads:")
	env.Printf("      max_concurrent_nodes: %s", opt(batchReads.MaxConcurrentNodes))
	env.Printf("      allow_inline:         %s", opt(batchReads.AllowInline))
	env.Printf("      allow_inline_ssd:     %s", opt(batchReads.AllowInlineSSD))

	batchWrites := behavior.Settings(sdk.OpWriteRetryable, sdk.ShapeBatch, sdk.ModeAP)
	env.Printf("    batch writes:")
	env.Printf("      max_concurrent_nodes: %s", opt(batchWrites.MaxConcurrentNodes))
	env.Printf("      total_timeout:        %s", dur(batchWrites.TotalTimeout))

	query := behavior.Settings(sdk.OpRead, sdk.ShapeQuery, sdk.ModeAP)
	env.Printf("    queries:")
	env.Printf("      record_queue_size:    %s", opt(query.RecordQueueSize))
	env.Printf("      max_concurrent_nodes: %s", opt(query.MaxConcurrentNodes))

	scReads := behavior.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeSC)
	env.Printf("    point reads (SC):")
	env.Printf("      read_mode_sc:         %s", opt(scReads.ReadModeSC))
	env.Printf("      total_timeout:        %s", dur(scReads.TotalTimeout))
}

// --- Display helpers -------------------------------------------------------
//
// Every Settings field is a pointer, and nil is the load-bearing case: it means
// the behavior does not set the value, so an operation inherits it.

// dur renders a duration setting in milliseconds.
func dur(value *time.Duration) string {
	if value == nil {
		return "(unset)"
	}
	return fmt.Sprintf("%d ms", value.Milliseconds())
}

// opt renders any setting whose zero value is meaningful.
func opt[T any](value *T) string {
	if value == nil {
		return "(unset)"
	}
	return fmt.Sprintf("%v", *value)
}

// commitLevel names a commit level, which the core client leaves unnamed.
func commitLevel(value *as.CommitLevel) string {
	if value == nil {
		return "(unset)"
	}
	if *value == as.COMMIT_MASTER {
		return "COMMIT_MASTER"
	}
	return "COMMIT_ALL"
}

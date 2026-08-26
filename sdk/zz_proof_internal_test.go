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

// Throwaway proof-of-finding tests. Not part of the permanent suite --
// written to demonstrate three implementation-review findings actually
// reproduce, then removed.

package sdk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// liveCluster is a minimal, package-internal fixture pointed at the same
// 127.0.0.1:3000 test cluster the sdk_test suite uses. It is deliberately
// separate from that suite's fixtures since this file lives in package sdk
// (to reach unexported names), not package sdk_test.
func liveCluster(t *testing.T) *Cluster {
	t.Helper()
	c, err := NewClusterDefinition("127.0.0.1", 3000).Connect()
	if err != nil {
		t.Skipf("no live cluster at 127.0.0.1:3000 to test against: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func randomProofSet(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("zzproof_%d", time.Now().UnixNano())
}

// Finding: the KindCommit branch in isRetryableTxnError is dead code, because
// no server result code is ever classified as KindCommit -- so
// DoInTransaction's documented "a failed commit is retried" behavior never
// actually triggers via that branch.
func TestProof_KindCommitBranchIsDead(t *testing.T) {
	for rc, kind := range resultCodeKinds {
		if kind == KindCommit {
			t.Fatalf("result code %v classifies as KindCommit -- branch is reachable, finding is disproven", rc)
		}
	}

	// MRT_COMMITTED is the closest thing to "a commit-phase result code" in
	// the table. Confirm it does NOT produce KindCommit, and confirm
	// isRetryableTxnError does NOT retry it -- directly contradicting
	// DoInTransaction's doc comment promise to retry "a failed commit".
	err := ErrorFromResultCode(types.MRT_COMMITTED, "commit outcome", false)
	if err.Kind() == KindCommit {
		t.Fatalf("expected MRT_COMMITTED to classify as something other than KindCommit, got KindCommit")
	}
	t.Logf("MRT_COMMITTED classified as Kind=%q (never KindCommit)", err.Kind())

	if isRetryableTxnError(err) {
		t.Fatalf("expected isRetryableTxnError to return false for this error (proving the documented commit-retry path is unreachable), got true")
	}
	t.Logf("isRetryableTxnError(MRT_COMMITTED-based error) = false -- contradicts doc comment's 'failed commit is retried' promise")
}

// Finding: the hand-rolled errorsAs cannot see through errors.Join, unlike the
// standard library's errors.As -- so a transaction error joined with another
// error via errors.Join is misclassified as non-retryable even when it wraps
// a genuinely retryable MRT_BLOCKED/MRT_VERSION_MISMATCH failure.
func TestProof_ErrorsAsMissesErrorsJoin(t *testing.T) {
	base := ErrorFromResultCode(types.MRT_BLOCKED, "blocked", false)
	joined := errors.Join(errors.New("unrelated concurrent error"), base)

	var viaHomeRolled *Error
	found := errorsAs(joined, &viaHomeRolled)
	if found {
		t.Fatalf("expected the SDK's errorsAs to fail to find *Error through errors.Join, but it found it -- finding is disproven")
	}
	t.Logf("errorsAs(joined, ...) found=%v (misses the wrapped *Error)", found)

	var viaStdlib *Error
	if !errors.As(joined, &viaStdlib) {
		t.Fatalf("expected the standard library's errors.As to find *Error through errors.Join, but it also missed it -- unexpected")
	}
	t.Logf("errors.As(joined, ...) found=true -- stdlib sees through errors.Join where the SDK's own errorsAs does not")

	// Concretely: this means a real MRT_BLOCKED failure, once joined with any
	// other error, stops being retried.
	if isRetryableTxnError(joined) {
		t.Fatalf("expected isRetryableTxnError to misclassify the joined MRT_BLOCKED error as non-retryable, got retryable")
	}
	if !isRetryableTxnError(base) {
		t.Fatalf("sanity check failed: the unjoined MRT_BLOCKED error should be retryable on its own")
	}
	t.Logf("isRetryableTxnError: base(unjoined)=true, joined=false -- errors.Join silently defeats the retry classification")
}

// Finding: CompressionThreshold, ErrorDetailVerbosity, and SimulateXDRWrite
// are accepted, merged, and displayed by Settings/Behavior machinery, but
// never consumed by policy_mapper.go -- so setting them has zero effect on
// the actual as.*Policy values sent to the server.
func TestProof_InertBehaviorSettingsHaveNoEffect(t *testing.T) {
	baseline, err := ToWritePolicy(Settings{})
	if err != nil {
		t.Fatalf("ToWritePolicy(empty): %v", err)
	}

	loaded := Settings{
		CompressionThreshold: IntPtr(1),
		ErrorDetailVerbosity: Uint8Ptr(VerbosityExpressionTrace),
		SimulateXDRWrite:     BoolPtr(true),
	}
	withSettings, err := ToWritePolicy(loaded)
	if err != nil {
		t.Fatalf("ToWritePolicy(loaded): %v", err)
	}

	if !reflect.DeepEqual(baseline, withSettings) {
		t.Fatalf("expected the resulting *as.WritePolicy to be identical regardless of these three settings (proving them inert), but they differed:\nbaseline:     %+v\nwithSettings: %+v",
			baseline, withSettings)
	}
	t.Logf("ToWritePolicy result is byte-for-byte identical whether CompressionThreshold/ErrorDetailVerbosity/SimulateXDRWrite are set or not -- confirmed inert")
}

// Finding: applyBehaviors (invoked via loadConfigAtConnect) mutates the
// package-global DefaultBehavior() singleton with no cluster-name scoping.
// This proves that loading a second cluster's config file, in the same
// process, silently overwrites the first cluster's DEFAULT behavior --
// even though the two calls pass distinct clusterName values.
func TestProof_ConfigReloadLeaksAcrossClusters(t *testing.T) {
	dir := t.TempDir()

	writeConfig := func(name string, durationLiteral string) string {
		path := filepath.Join(dir, name)
		content := "behaviors:\n  DEFAULT:\n    allOperations:\n      abandonCallAfter: " + durationLiteral + "\n"
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return path
	}

	pathA := writeConfig("cluster-a.yaml", "111s")
	pathB := writeConfig("cluster-b.yaml", "222s")

	resolvedTimeout := func() time.Duration {
		s := DefaultBehavior().Settings(OpWriteRetryable, ShapePoint, ModeAP)
		if s.TotalTimeout == nil {
			t.Fatalf("expected TotalTimeout to be set after loading a DEFAULT.allOperations.abandonCallAfter config")
		}
		return *s.TotalTimeout
	}

	t.Setenv(EnvConfigURL, "file://"+pathA)
	loadConfigAtConnect("cluster-a", SystemSettings{})
	gotA := resolvedTimeout()
	if gotA != 111*time.Second {
		t.Fatalf("after loading cluster-a's config, expected TotalTimeout=111s, got %s", gotA)
	}
	t.Logf("after loading cluster-a's config: DefaultBehavior TotalTimeout=%s", gotA)

	// Now a second Client, in the same process, connects to a *different*
	// cluster with its *own* config file -- passing a distinct clusterName.
	t.Setenv(EnvConfigURL, "file://"+pathB)
	loadConfigAtConnect("cluster-b", SystemSettings{})
	gotB := resolvedTimeout()
	if gotB != 222*time.Second {
		t.Fatalf("after loading cluster-b's config, expected TotalTimeout=222s, got %s", gotB)
	}
	t.Logf("after loading cluster-b's config: DefaultBehavior TotalTimeout=%s", gotB)

	// The proof: cluster-a's session never reloaded anything, yet it shares
	// the exact same DefaultBehavior() singleton, so it is now silently
	// governed by cluster-b's timeout. Nothing about cluster-a's own
	// clusterName ("cluster-a") protected its settings.
	finalForClusterA := resolvedTimeout()
	if finalForClusterA != 222*time.Second {
		t.Fatalf("expected cluster-a's own DefaultBehavior() to now read cluster-b's value (222s), got %s -- leak not reproduced", finalForClusterA)
	}
	t.Logf("cluster-a's own DefaultBehavior() now resolves TotalTimeout=%s (cluster-b's value) -- cross-cluster leak confirmed", finalForClusterA)
}

// Finding: HasMoreChunks() never checks s.closed, and Close() never clears
// s.chunked/s.reexecute, so calling HasMoreChunks() again on an explicitly
// closed chunked stream silently reopens a new server-side recordset.
func TestProof_HasMoreChunksResurrectsClosedStream(t *testing.T) {
	c := liveCluster(t)
	session, err := c.CreateSession(nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ds, err := DataSetOf("test", randomProofSet(t))
	if err != nil {
		t.Fatalf("DataSetOf: %v", err)
	}

	for i := 0; i < 6; i++ {
		key := ds.Key(fmt.Sprintf("k%d", i))
		if err := session.Put(key, as.BinMap{"n": i}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	stream, err := session.Query(ds).ChunkSize(2).Execute()
	if err != nil {
		t.Fatalf("Query.Execute: %v", err)
	}

	more, err := stream.HasMoreChunks()
	if err != nil || !more {
		t.Fatalf("expected first HasMoreChunks() to return true, got more=%v err=%v", more, err)
	}
	for {
		row, err := stream.Next()
		if err != nil || row == nil {
			break
		}
	}

	// The caller explicitly abandons the stream here.
	stream.Close()

	// Per the documented contract, this stream is done. Calling
	// HasMoreChunks() again should not silently do more work.
	moreAfterClose, err := stream.HasMoreChunks()
	t.Logf("HasMoreChunks() after Close(): more=%v err=%v (recordset reopened=%v, closed flag=%v)",
		moreAfterClose, err, stream.recordset != nil, stream.closed)

	if !moreAfterClose || stream.recordset == nil {
		t.Fatalf("expected HasMoreChunks() to resurrect the closed stream (more=true, a new recordset attached) -- bug did not reproduce; got more=%v recordset!=nil=%v",
			moreAfterClose, stream.recordset != nil)
	}
	t.Logf("BUG CONFIRMED: HasMoreChunks() silently reopened a new server-side recordset on a stream the caller had already explicitly Close()'d")
}

// Finding: RecordStream.Iter() stores a cluster-level error in the
// package-level iterErrs sync.Map keyed by the stream pointer, and that entry
// is only ever removed inside Close(). The documented Iter()+Err() idiom
// never calls Close(), so an errored, unclosed stream leaks its map entry
// (and the stream object) for the life of the process.
func TestProof_IterErrsLeaksWithoutClose(t *testing.T) {
	c := liveCluster(t)
	session, err := c.CreateSession(nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// A behavior with an impossibly short timeout forces a genuine
	// client-side timeout error mid-query, without needing to break the
	// cluster itself.
	tiny := time.Nanosecond
	fast := DefaultBehavior().DeriveWithChanges("zzproof-impossible-timeout", map[Scope]Settings{
		ScopeAll: {TotalTimeout: &tiny, SocketTimeout: &tiny},
	})
	session, err = c.CreateSession(fast)
	if err != nil {
		t.Fatalf("CreateSession(fast): %v", err)
	}

	ds, err := DataSetOf("test", randomProofSet(t))
	if err != nil {
		t.Fatalf("DataSetOf: %v", err)
	}

	stream, err := session.Query(ds).Execute()
	if err != nil {
		// The timeout may already surface at Execute() on some code paths;
		// either place proves the same underlying leak mechanism, since
		// Iter()'s setErr path and this one both write into iterErrs the
		// same way. Fall through and drive Iter() directly if we got a
		// stream at all; otherwise this specific repro path didn't apply.
		t.Skipf("Execute() itself returned an error (%v); this code path didn't reach Iter(), skipping", err)
	}

	for range stream.Iter() {
		// drain; expect this to stop early on the forced timeout
	}

	streamErr := stream.Err()
	if streamErr == nil {
		t.Skip("the impossibly short timeout did not actually trigger a stream error on this run (timing-dependent); cannot demonstrate the leak this way")
	}
	t.Logf("Iter() surfaced the expected forced error: %v", streamErr)

	// The documented Iter()+Err() idiom stops here. We deliberately do NOT
	// call stream.Close() -- that's the whole point.
	if _, tracked := iterErrs.Load(stream); !tracked {
		t.Fatalf("expected the errored stream's entry to still be present in iterErrs (proving the leak), but it was already gone")
	}
	t.Logf("BUG CONFIRMED: iterErrs still holds this *RecordStream after the documented Iter()+Err() usage completed, with Close() never called")
}

// Finding (highest severity): a whole-batch failure from core.BatchOperate is
// silently dropped for the default (dispInStream) disposition, streaming
// fabricated OK rows instead of surfacing the failure.
//
// Three independent live-repro attempts against the reachable test cluster
// all failed to trigger the exact "aerr != nil && !hasPerRecordOutcomes"
// precondition, and all three converged on the same reassuring result:
//
//  1. An impossibly short client-side timeout -> per-record NO_RESPONSE.
//  2. An unknown/invalid namespace -> per-record INVALID_NAMESPACE.
//  3. A full outage (the test cluster's container paused for several
//     seconds, tend interval sped up to 200ms to force fast node-health
//     detection) -> per-record NO_RESPONSE again.
//
// In all three, the core client set a real per-record result code before
// returning, so hasPerRecordOutcomes was true and the correct per-row path
// ran instead of the buggy one. That is itself a useful, honest result:
// those three common failure modes are all handled correctly today.
//
// Tracing where the exact precondition IS reachable, in the core client's
// own source:
//
//	// client.go:964-978, (*Client).BatchOperate:
//	batchNodes, err := newBatchOperateNodeListIfc(clnt.cluster, policy, records)
//	if err != nil && policy.RespondAllKeys {
//	    return err
//	}
//	if len(batchNodes) == 0 {
//	    return newError(types.INVALID_NAMESPACE)   // <-- returned with ZERO records touched
//	}
//
// This return happens before any per-record result code is ever written,
// which is exactly the SDK-side bug's trigger condition. The outage attempt
// suggests why it's hard to hit on this cluster specifically: with a single
// node, there is no peer left to confirm the node is gone, so the client
// appears to keep retrying/attempting the known (unreachable) node rather
// than ever emptying its node list to zero -- it fails per-key instead of
// hitting the batchNodes==0 pre-check. Reaching len(batchNodes)==0 likely
// needs a multi-node cluster where surviving nodes report a peer as evicted,
// or a fresh Connect() attempt against zero reachable nodes from the start.
//
// This finding therefore stays at "confirmed via source, precise trigger
// located and explained, three live-repro attempts converged on a narrower
// real-world reachability than the original severity assumed" -- an honest,
// nuanced result rather than an unqualified "definitely happens in
// production" claim.

// Finding: mappingInfo's doc comment claims analyzeType "caches the
// reflection view of an entity type", but there is no cache anywhere in it --
// it recomputes the full struct-field/tag scan from scratch on every call.
// Proof: if a cache existed, allocations-per-call would drop sharply once a
// reflect.Type has been seen before (a cache hit is a cheap map lookup). If
// there is no cache, allocs/call stay constant no matter how many times the
// identical reflect.Type has already been analyzed. No live cluster needed.
func TestProof_AnalyzeTypeRecomputesEveryCall(t *testing.T) {
	type entity struct {
		ID   string `as:",key"`
		Name string `as:"name"`
		Note string `as:"note"`
		Gen  uint32 `asm:"gen"`
	}
	rt := reflect.TypeOf(entity{})

	// AllocsPerRun(n, f) calls f once to warm up (uncounted), then runs it n
	// more times, reporting the average. So "afterFewCalls" already reflects
	// the cost after this exact reflect.Type has been analyzed once before,
	// and "afterManyCalls" reflects the cost after it has been analyzed tens
	// of thousands of times before. A cache would make the second number
	// collapse toward zero; no cache means the two stay roughly equal.
	afterFewCalls := testing.AllocsPerRun(5, func() { _ = analyzeType(rt) })
	afterManyCalls := testing.AllocsPerRun(50_000, func() { _ = analyzeType(rt) })

	t.Logf("allocs/call after ~5 prior analyses of this exact type:     %.1f", afterFewCalls)
	t.Logf("allocs/call after ~50,000 prior analyses of this exact type: %.1f", afterManyCalls)

	if afterFewCalls == 0 {
		t.Fatalf("expected analyzeType to allocate on every call (proving no cache) -- got 0 allocs even early on, contradicting the finding")
	}
	if afterManyCalls < afterFewCalls*0.5 {
		t.Fatalf("expected allocs/call to stay roughly constant even after 50,000 repeated calls on the identical type (proving no caching) -- instead it dropped sharply (%.1f -> %.1f), suggesting a cache may now exist and the finding no longer applies",
			afterFewCalls, afterManyCalls)
	}
	t.Logf("BUG CONFIRMED: allocs/call does not decrease with repetition on the identical reflect.Type (%.1f -> %.1f) -- analyzeType has no cache, contradicting mappingInfo's doc comment claim of caching",
		afterFewCalls, afterManyCalls)
}

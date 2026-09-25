package queryexamples

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateBehaviorPresets shows selecting between the built-in Behavior
// presets, explaining each, and creating a session scoped to it.
//
// DX GAP: Java's real usage derives custom behaviors with selector-scoped
// tuning — Behavior.DEFAULT.deriveWithChanges(name, builder ->
// builder.on(Selectors.reads().ap(), ops -> ops.maximumNumberOfCallAttempts(3))...)
// — but sdk.BehaviorPatches (the type NewBehavior's patches parameter
// takes) has zero fields; the PRD's own catalog left this as an
// unexpanded "patches ..." ellipsis. Building the real selector/tunable
// system would mean inventing a subsystem the PRD never specified, so
// this demonstrates only the concrete, already-specified surface: the
// built-in presets and Explain(). Custom per-selector derivation is left
// out rather than guessed at.
func DemonstrateBehaviorPresets(ctx context.Context, cluster *sdk.Cluster) error {
	presets := []*sdk.Behavior{
		sdk.DefaultBehavior(),
		sdk.ReadFastBehavior(),
		sdk.StrictlyConsistentBehavior(),
		sdk.FastRackAwareBehavior(),
	}
	for _, b := range presets {
		fmt.Println(b.Explain())
		if _, err := cluster.CreateSession(ctx, b); err != nil {
			return fmt.Errorf("create session for behavior preset: %w", err)
		}
	}
	return nil
}

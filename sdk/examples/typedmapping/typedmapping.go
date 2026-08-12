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

// Package typedmapping shows typed object mapping end to end.
//
// Port of the Java SDK's TypedMappingExamples, by way of the Rust SDK's
// `typed_mapping`. Whole objects go in and come out through a TypedDataSet: no
// mapper argument anywhere, because the mapping travels with the type. The
// example writes objects, reads them back from a typed stream, follows a
// dependent record reference, and finishes with a heterogeneous batch whose
// rows are mapped to two different entity types.
//
// Rust resolves the mapper at compile time with `#[derive(RecordMapper)]`,
// which Go has no equivalent for. Go offers two routes instead, and this
// example shows both: [Widget] is mapped by reflection over its struct tags,
// while [Gadget] implements [sdk.RecordMapper] and does the work itself.
package typedmapping

import (
	"cmp"
	"fmt"
	"slices"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Widget is a simple inventory row, mapped by reflection over its `as` tags.
//
// `as:",key"` is what puts the type in entity mode: ID becomes the record's
// user key rather than a bin, and is recovered from the key on reads. The
// remaining tags keep the Java bin names. Rust models the optional reference as
// an Option; here a zero ID means "no reference", and `omitempty` keeps the bin
// off the wire in that case.
type Widget struct {
	ID              int64  `as:",key"`
	Label           string `as:"label"`
	Quantity        int64  `as:"qty"`
	RelatedGadgetID int64  `as:"rel_gadget_id,omitempty"`
	Generation      uint32 `asm:"gen"`
}

// String implements fmt.Stringer so the narration reads cleanly.
func (w *Widget) String() string {
	s := fmt.Sprintf("Widget{id=%d label=%q qty=%d", w.ID, w.Label, w.Quantity)
	if w.RelatedGadgetID != 0 {
		s += fmt.Sprintf(" rel_gadget_id=%d", w.RelatedGadgetID)
	}
	if w.Generation != 0 {
		s += fmt.Sprintf(" gen=%d", w.Generation)
	}
	return s + "}"
}

// Gadget is a feature-flag row that maps itself.
//
// Implementing [sdk.RecordMapper] on the pointer receiver overrides reflection
// entirely: no tags are consulted, and the type decides both the stored shape
// and how to recover from it. It is the Go counterpart of Java's hand-written
// RecordMapping, and the escape hatch for anything the tags cannot express.
// The key field cannot be called ID: the interface's ID method would collide
// with it.
type Gadget struct {
	GadgetID int64
	Name     string
	Enabled  bool
}

// ToBins reports the bins to write.
func (g *Gadget) ToBins() (as.BinMap, error) {
	return as.BinMap{"name": g.Name, "enabled": g.Enabled}, nil
}

// SetFromRecord rebuilds the gadget from a record.
//
// The generation is available here but this entity does not keep it. A boolean
// bin comes back as a bool from a modern server and as an integer from an older
// one, and a hand-written mapper is exactly where that gets normalized.
func (g *Gadget) SetFromRecord(bins as.BinMap, key *as.Key, generation uint32) error {
	if key != nil && key.Value() != nil {
		id, ok := key.Value().GetObject().(int64)
		if !ok {
			return fmt.Errorf("gadget key is %T, want int64", key.Value().GetObject())
		}
		g.GadgetID = id
	}
	if name, ok := bins["name"].(string); ok {
		g.Name = name
	}
	switch v := bins["enabled"].(type) {
	case bool:
		g.Enabled = v
	case int:
		g.Enabled = v != 0
	case int64:
		g.Enabled = v != 0
	}
	return nil
}

// ID reports the user key.
func (g *Gadget) ID() any { return g.GadgetID }

// String implements fmt.Stringer so the narration reads cleanly.
func (g *Gadget) String() string {
	return fmt.Sprintf("Gadget{id=%d name=%q enabled=%t}", g.GadgetID, g.Name, g.Enabled)
}

// Run executes the example.
func Run(env *exrun.Env) error {
	widgetSet, err := env.DataSet("typed_demo_widgets")
	if err != nil {
		return err
	}
	gadgetSet, err := env.DataSet("typed_demo_gadgets")
	if err != nil {
		return err
	}
	widgets := sdk.TypedDataSetFrom[Widget](widgetSet)
	gadgets := sdk.TypedDataSetFrom[Gadget](gadgetSet)

	env.Printf("*********************")
	env.Printf("* Typed object mapping")
	env.Printf("*********************")

	// Keys derive from each object's key field, so no key is named here. Java's
	// runtime RecordMappingFactory has no counterpart: the mapping is part of
	// the type. The gadget goes in first so the widget's reference resolves.
	if _, err := env.Session.InsertTyped(gadgets).
		Object(&Gadget{GadgetID: 1, Name: "notifications", Enabled: true}).
		Execute(); err != nil {
		return err
	}
	stream, err := env.Session.InsertTyped(widgets).
		Object(&Widget{ID: 1, Label: "alpha", Quantity: 10, RelatedGadgetID: 1}).
		Object(&Widget{ID: 2, Label: "beta", Quantity: 20}).
		Execute()
	if err != nil {
		return err
	}
	if _, err := stream.Collect(); err != nil {
		return err
	}

	// Single-row read through a typed query stream, filtered server-side.
	typed, err := env.Session.QueryTyped(widgets).Where("$.qty == 10").Limit(1).Execute()
	if err != nil {
		return err
	}
	one, err := typed.FirstObject()
	if err != nil {
		return err
	}
	env.Printf("Typed query FirstObject: %v", one)

	// Java resolves a related record inside the mapper's four-argument
	// fromMap, which receives a RecordReadContext. SetFromRecord has no
	// session, so a dependent read is an explicit step in the caller -- and
	// visibly one round trip, not a hidden one.
	related, err := loadRelatedGadget(env, gadgets, one)
	if err != nil {
		return err
	}
	if related != nil {
		env.Printf("  [dependent read] widget %d -> gadget name=%q, enabled=%t",
			one.ID, related.Name, related.Enabled)
	}

	typed, err = env.Session.QueryTyped(widgets).Where("$.qty == 20").Execute()
	if err != nil {
		return err
	}
	filtered, err := typed.IntoObjects()
	if err != nil {
		return err
	}
	env.Printf("Typed query (qty == 20 only): %v", filtered)

	// The whole set, as objects.
	typed, err = env.Session.QueryTyped(widgets).Limit(10).Execute()
	if err != nil {
		return err
	}
	all, err := typed.IntoObjects()
	if err != nil {
		return err
	}
	// A set-wide query returns records in partition order, so sort by the
	// recovered key to keep the narration stable across runs.
	slices.SortFunc(all, func(a, b *Widget) int { return cmp.Compare(a.ID, b.ID) })
	env.Printf("Typed query, all widgets:")
	for _, widget := range all {
		suffix := ""
		gadget, err := loadRelatedGadget(env, gadgets, widget)
		if err != nil {
			return err
		}
		if gadget != nil {
			suffix = fmt.Sprintf(" (related gadget %q)", gadget.Name)
		}
		env.Printf("  %v%s", widget, suffix)
	}

	typed, err = env.Session.QueryTyped(widgets).Where("$.label == 'alpha'").Execute()
	if err != nil {
		return err
	}
	byLabel, err := typed.FirstObject()
	if err != nil {
		return err
	}
	env.Printf("Typed query by label: %v", byLabel)

	return heterogeneousBatch(env, widgets, gadgets)
}

// heterogeneousBatch reads two entity types in one round trip.
//
// One stream cannot have two element types, so the chain is the untyped one and
// each row is mapped by whichever entity its set names. Gadget maps itself,
// because its RecordMapper is an ordinary exported method. Widget's reflection
// mapping has no per-row entry point, so the row is handed to the typed layer
// as a one-row stream -- the Go counterpart of Java's RecordResult.toObject().
func heterogeneousBatch(env *exrun.Env, widgets *sdk.TypedDataSet[Widget], gadgets *sdk.TypedDataSet[Gadget]) error {
	widgetKey := widgets.Key(int64(2))
	gadgetKey := gadgets.Key(int64(1))
	stream, err := env.Session.Query(widgetKey).
		Bins("label", "qty").
		Query([]*as.Key{gadgetKey}).
		Bins("name", "enabled").
		Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	env.Printf("Heterogeneous batch:")
	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}
		rec, err := row.RecordOrRaise()
		if err != nil {
			return err
		}
		if row.Key.SetName() == widgets.SetName() {
			widget, err := sdk.TypedStreamFrom[Widget](
				sdk.NewRecordStreamFromSingle(row.Key, rec)).FirstObject()
			if err != nil {
				return err
			}
			env.Printf("  widget: %v", widget)
			continue
		}
		gadget := &Gadget{}
		if err := gadget.SetFromRecord(rec.Bins, row.Key, rec.Generation); err != nil {
			return err
		}
		env.Printf("  gadget: %v", gadget)
	}
}

// loadRelatedGadget loads the gadget a widget points at, or nil when it points
// at none. Java hides this inside the mapper, through
// RecordReadContext.getSession; here it is an ordinary typed point read.
func loadRelatedGadget(env *exrun.Env, gadgets *sdk.TypedDataSet[Gadget], widget *Widget) (*Gadget, error) {
	if widget.RelatedGadgetID == 0 {
		return nil, nil
	}
	key := gadgets.Key(widget.RelatedGadgetID)
	typed, err := env.Session.QueryTypedKeys(gadgets, []*as.Key{key}).Execute()
	if err != nil {
		return nil, err
	}
	return typed.FirstObject()
}

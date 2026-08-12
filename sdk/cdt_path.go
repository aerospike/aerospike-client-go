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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// Path expressions address *every* matching place in a collection rather than
// one. They require server 8.1.1 or newer; gate on
// [Cluster.SupportsCDTPathExpressions] when you need to branch.
//
// The builders come in pairs, distinguished by what the last step selected:
//
//   - a map-like or not-yet-narrowed step yields a Map builder, which offers
//     the key-oriented terminals;
//   - a list step yields a List builder, which does not -- so asking for map
//     keys after descending into a list is a compile error, not a runtime one.

// pathState carries the accumulated path.
type pathState struct {
	bin    string
	ctx    []*as.CDTContext
	noFail bool
}

// push returns a copy with one more context appended.
func (p pathState) push(c *as.CDTContext) pathState {
	out := p
	out.ctx = append(append([]*as.CDTContext{}, p.ctx...), c)
	return out
}

// selectFlag assembles the select flag for a terminal.
func (p pathState) selectFlag(base as.SelectFlag) as.SelectFlag {
	if p.noFail {
		return base | as.EXP_PATH_SELECT_NO_FAIL
	}
	return base
}

// modifyFlag assembles the modify flag for a terminal.
func (p pathState) modifyFlag() as.ModifyFlag {
	if p.noFail {
		return as.EXP_PATH_MODIFY_NO_FAIL
	}
	return as.EXP_PATH_MODIFY_DEFAULT
}

// --- Entry points ---

// OnEachChild iterates every child at the current step.
func (b *QueryBinBuilder) OnEachChild() *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.bin}.push(as.CtxAllChildren()),
	}
}

// OnEachChildWhere iterates the children a filter accepts.
//
// Inside the filter, the loop-variable expressions refer to the candidate
// child.
func (b *QueryBinBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.bin}.push(as.CtxAllChildrenWithFilter(filter)),
	}
}

// OnEachChild iterates every child at the current step.
func (b *WriteBinBuilder) OnEachChild() *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.bin}.push(as.CtxAllChildren()),
	}
}

// OnEachChildWhere iterates the children a filter accepts.
func (b *WriteBinBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.bin}.push(as.CtxAllChildrenWithFilter(filter)),
	}
}

// --- Read path builders ---

// CdtPathReadMapBuilder is a read path whose last step was map-like or has not
// narrowed to a list. It offers the key-oriented terminals.
type CdtPathReadMapBuilder struct {
	parent *QueryBuilder
	path   pathState
}

// CdtPathReadListBuilder is a read path whose last step descended into a list.
// It deliberately lacks CollectKeys and CollectKeysAndValues.
type CdtPathReadListBuilder struct {
	parent *QueryBuilder
	path   pathState
}

// NoFail skips missing or type-mismatched segments instead of failing.
func (b *CdtPathReadMapBuilder) NoFail() *CdtPathReadMapBuilder {
	b.path.noFail = true
	return b
}

// NoFail skips missing or type-mismatched segments instead of failing.
func (b *CdtPathReadListBuilder) NoFail() *CdtPathReadListBuilder {
	b.path.noFail = true
	return b
}

// OnMapKey descends by map key, keeping the map-oriented terminals.
func (b *CdtPathReadMapBuilder) OnMapKey(key any) *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapKey(as.NewValue(key)))}
}

// OnMapValue descends by map value.
func (b *CdtPathReadMapBuilder) OnMapValue(value any) *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapValue(as.NewValue(value)))}
}

// OnMapKeysIn descends through several map keys, a context only path
// expressions accept.
func (b *CdtPathReadMapBuilder) OnMapKeysIn(keys ...any) *CdtPathReadMapBuilder {
	vals := make([]as.Value, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, as.NewValue(k))
	}
	return &CdtPathReadMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapKeysIn(vals...))}
}

// OnListIndex descends by list index, which narrows the terminals to the
// list-safe subset.
func (b *CdtPathReadMapBuilder) OnListIndex(index int) *CdtPathReadListBuilder {
	return &CdtPathReadListBuilder{parent: b.parent,
		path: b.path.push(as.CtxListIndex(index))}
}

// OnListValue descends by list value.
func (b *CdtPathReadMapBuilder) OnListValue(value any) *CdtPathReadListBuilder {
	return &CdtPathReadListBuilder{parent: b.parent,
		path: b.path.push(as.CtxListValue(as.NewValue(value)))}
}

// OnEachChild iterates every child at the current step, widening again.
func (b *CdtPathReadListBuilder) OnEachChild() *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{parent: b.parent, path: b.path.push(as.CtxAllChildren())}
}

// OnEachChild iterates every child at the current step.
func (b *CdtPathReadMapBuilder) OnEachChild() *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{parent: b.parent, path: b.path.push(as.CtxAllChildren())}
}

// OnEachChildWhere iterates the children a filter accepts.
func (b *CdtPathReadMapBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxAllChildrenWithFilter(filter))}
}

// CollectValues reports the values of the finally selected nodes.
func (b *CdtPathReadMapBuilder) CollectValues() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_VALUE)
}

// CollectValues reports the values of the finally selected nodes.
func (b *CdtPathReadListBuilder) CollectValues() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_VALUE)
}

// CollectTree reports a structure-preserving tree from the root to the matches.
func (b *CdtPathReadMapBuilder) CollectTree() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_MATCHING_TREE)
}

// CollectTree reports a structure-preserving tree from the root to the matches.
func (b *CdtPathReadListBuilder) CollectTree() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_MATCHING_TREE)
}

// CollectKeys reports the map keys of the selected entries.
//
// It exists only on the map builder: after a list step the server has no keys
// to report, so the call would be meaningless.
func (b *CdtPathReadMapBuilder) CollectKeys() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_MAP_KEY)
}

// CollectKeysAndValues reports the selected entries as interleaved keys and
// values. Map builder only, for the same reason as CollectKeys.
func (b *CdtPathReadMapBuilder) CollectKeysAndValues() *QueryBuilder {
	return pathSelect(b.parent, b.path, as.EXP_PATH_SELECT_MAP_KEY_VALUE)
}

// pathSelect issues the select operation.
func pathSelect(q *QueryBuilder, p pathState, flag as.SelectFlag) *QueryBuilder {
	q.c.pushOp(as.SelectByPath(p.bin, p.selectFlag(flag), p.ctx...))
	return q
}

// --- Write path builders ---

// CdtPathWriteMapBuilder is a write path whose last step was map-like or has
// not narrowed to a list.
type CdtPathWriteMapBuilder struct {
	parent *WriteSegmentBuilder
	path   pathState
}

// CdtPathWriteListBuilder is a write path whose last step descended into a
// list.
type CdtPathWriteListBuilder struct {
	parent *WriteSegmentBuilder
	path   pathState
}

// NoFail skips missing or type-mismatched segments instead of failing.
func (b *CdtPathWriteMapBuilder) NoFail() *CdtPathWriteMapBuilder {
	b.path.noFail = true
	return b
}

// NoFail skips missing or type-mismatched segments instead of failing.
func (b *CdtPathWriteListBuilder) NoFail() *CdtPathWriteListBuilder {
	b.path.noFail = true
	return b
}

// OnMapKey descends by map key.
func (b *CdtPathWriteMapBuilder) OnMapKey(key any) *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapKey(as.NewValue(key)))}
}

// OnMapValue descends by map value.
func (b *CdtPathWriteMapBuilder) OnMapValue(value any) *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapValue(as.NewValue(value)))}
}

// OnMapKeysIn descends through several map keys.
func (b *CdtPathWriteMapBuilder) OnMapKeysIn(keys ...any) *CdtPathWriteMapBuilder {
	vals := make([]as.Value, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, as.NewValue(k))
	}
	return &CdtPathWriteMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxMapKeysIn(vals...))}
}

// OnListIndex descends by list index.
func (b *CdtPathWriteMapBuilder) OnListIndex(index int) *CdtPathWriteListBuilder {
	return &CdtPathWriteListBuilder{parent: b.parent,
		path: b.path.push(as.CtxListIndex(index))}
}

// OnEachChild iterates every child at the current step.
func (b *CdtPathWriteMapBuilder) OnEachChild() *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{parent: b.parent, path: b.path.push(as.CtxAllChildren())}
}

// OnEachChildWhere iterates the children a filter accepts.
func (b *CdtPathWriteMapBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{parent: b.parent,
		path: b.path.push(as.CtxAllChildrenWithFilter(filter))}
}

// OnEachChild iterates every child at the current step, widening again.
func (b *CdtPathWriteListBuilder) OnEachChild() *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{parent: b.parent, path: b.path.push(as.CtxAllChildren())}
}

// ModifyBy applies an expression at every match.
func (b *CdtPathWriteMapBuilder) ModifyBy(exp *as.Expression) *WriteSegmentBuilder {
	return pathModify(b.parent, b.path, exp)
}

// ModifyBy applies an expression at every match.
func (b *CdtPathWriteListBuilder) ModifyBy(exp *as.Expression) *WriteSegmentBuilder {
	return pathModify(b.parent, b.path, exp)
}

// CollectValues reports the values of the finally selected nodes.
func (b *CdtPathWriteMapBuilder) CollectValues() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.SelectByPath(b.path.bin,
		b.path.selectFlag(as.EXP_PATH_SELECT_VALUE), b.path.ctx...))
	return b.parent
}

// CollectKeys reports the map keys of the selected entries.
func (b *CdtPathWriteMapBuilder) CollectKeys() *WriteSegmentBuilder {
	b.parent.c.pushOp(as.SelectByPath(b.path.bin,
		b.path.selectFlag(as.EXP_PATH_SELECT_MAP_KEY), b.path.ctx...))
	return b.parent
}

// pathModify issues the modify operation.
func pathModify(w *WriteSegmentBuilder, p pathState, exp *as.Expression) *WriteSegmentBuilder {
	if exp == nil {
		w.c.deferErr(NewError(KindInvalidArgument, "ModifyBy needs an expression"))
		return w
	}
	w.c.pushOp(as.ModifyByPath(p.bin, p.modifyFlag(), exp, p.ctx...))
	return w
}

// --- Entering a path from a fixed navigation step ---
//
// A path chain need not start at the bin root. Fixed navigation and path
// iteration compose: descend to a known place with the ordinary selectors, then
// switch into path mode to address every match beneath it. This is why the
// each-child methods live on the navigable CDT builders as well as on the bin
// builders.

// OnEachChild iterates every child at the selected path.
func (b *CdtReadBuilder) OnEachChild() *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.path.bin, ctx: b.contexts()}.push(as.CtxAllChildren()),
	}
}

// OnEachChildWhere iterates the children at the selected path that a filter
// accepts.
func (b *CdtReadBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathReadMapBuilder {
	return &CdtPathReadMapBuilder{
		parent: b.parent,
		path: pathState{bin: b.path.bin, ctx: b.contexts()}.
			push(as.CtxAllChildrenWithFilter(filter)),
	}
}

// OnEachChild iterates every child at the selected path.
func (b *CdtWriteBuilder) OnEachChild() *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{
		parent: b.parent,
		path:   pathState{bin: b.path.bin, ctx: b.contexts()}.push(as.CtxAllChildren()),
	}
}

// OnEachChildWhere iterates the children at the selected path that a filter
// accepts.
func (b *CdtWriteBuilder) OnEachChildWhere(filter *as.Expression) *CdtPathWriteMapBuilder {
	return &CdtPathWriteMapBuilder{
		parent: b.parent,
		path: pathState{bin: b.path.bin, ctx: b.contexts()}.
			push(as.CtxAllChildrenWithFilter(filter)),
	}
}

// --- Removal and expression-read terminals ---

// RemoveMatches removes every node the path selected.
//
// It is the modification whose expression discards the match, which is why it
// reads as a removal rather than a modify.
func (b *CdtPathWriteMapBuilder) RemoveMatches() *WriteSegmentBuilder {
	return pathModify(b.parent, b.path, as.ExpRemoveResult())
}

// RemoveMatches removes every node the path selected.
func (b *CdtPathWriteListBuilder) RemoveMatches() *WriteSegmentBuilder {
	return pathModify(b.parent, b.path, as.ExpRemoveResult())
}

// CollectValuesAsExpressionRead evaluates the selection as an expression read
// stored under this chain's bin name.
//
// The two types describe the source bin and the expected result; set
// ignoreEvalFailure to tolerate a selection that does not evaluate rather than
// failing the operation.
func (b *CdtPathReadMapBuilder) CollectValuesAsExpressionRead(
	binType, resultType as.ExpType, ignoreEvalFailure bool,
) *QueryBuilder {
	return pathExpressionRead(b.parent, b.path, binType, resultType, ignoreEvalFailure)
}

// CollectValuesAsExpressionRead evaluates the selection as an expression read.
func (b *CdtPathReadListBuilder) CollectValuesAsExpressionRead(
	binType, resultType as.ExpType, ignoreEvalFailure bool,
) *QueryBuilder {
	return pathExpressionRead(b.parent, b.path, binType, resultType, ignoreEvalFailure)
}

// pathExpressionRead issues the selection as an expression read.
func pathExpressionRead(
	q *QueryBuilder, p pathState,
	binType, resultType as.ExpType, ignoreEvalFailure bool,
) *QueryBuilder {
	bin := binExpressionOfType(binType, p.bin)
	if bin == nil {
		q.c.deferErr(NewError(KindInvalidArgument,
			"unsupported source bin type for an expression read"))
		return q
	}
	exp := as.ExpSelectByPath(resultType, p.selectFlag(as.EXP_PATH_SELECT_VALUE), bin, p.ctx...)
	flags := as.ExpReadFlagDefault
	if ignoreEvalFailure {
		flags = as.ExpReadFlagEvalNoFail
	}
	q.c.pushOp(as.ExpReadOp(p.bin, exp, flags))
	return q
}

// binExpressionOfType builds the bin expression the source type calls for.
func binExpressionOfType(t as.ExpType, bin string) *as.Expression {
	switch t {
	case as.ExpTypeMAP:
		return as.ExpMapBin(bin)
	case as.ExpTypeLIST:
		return as.ExpListBin(bin)
	case as.ExpTypeINT:
		return as.ExpIntBin(bin)
	case as.ExpTypeSTRING:
		return as.ExpStringBin(bin)
	case as.ExpTypeFLOAT:
		return as.ExpFloatBin(bin)
	case as.ExpTypeBLOB:
		return as.ExpBlobBin(bin)
	default:
		return nil
	}
}

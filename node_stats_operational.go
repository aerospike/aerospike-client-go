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

package aerospike

import (
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	hist "github.com/aerospike/aerospike-client-go/v8/types/histogram"
)

func (n *nodeStats) newNsOperationalMetrics() *nsOperationalMetrics {
	m := &nsOperationalMetrics{}
	cols, shift := n.latencyShape()
	for i := range m.Latencies {
		m.Latencies[i] = *hist.NewSyncLatencyBuckets(cols, shift)
	}
	return m
}

func (n *nodeStats) latencyShape() (columns, shift int) {
	columns = defaultLatencyColumns
	shift = defaultLatencyShift
	if policy := n.getMetricPolicy(); policy != nil {
		if policy.LatencyColumns > 0 {
			columns = policy.LatencyColumns
		}
		if policy.LatencyShift > 0 {
			shift = policy.LatencyShift
		}
	}
	return columns, shift
}

func (n *nodeStats) getOrCreateNsOp(namespace string) *nsOperationalMetrics {
	if val, ok := n.OperationalNamespaces.Load(namespace); ok {
		return val.(*nsOperationalMetrics)
	}
	created := n.newNsOperationalMetrics()
	actual, _ := n.OperationalNamespaces.LoadOrStore(namespace, created)
	return actual.(*nsOperationalMetrics)
}

func (n *nodeStats) addSharedLatency(namespace string, lt LatencyType, elapsed uint64) {
	if namespace == "" || lt == LatencyConn {
		// Connection-open latency is node-scoped (addConnLatency), not namespace.
		return
	}
	op := n.getOrCreateNsOp(namespace)
	op.Latencies[lt].Add(elapsed)
}

// addConnLatency records node.latency.conn (successful open). Not sampler-gated.
func (n *nodeStats) addConnLatency(elapsed uint64) {
	if n == nil {
		return
	}
	n.ConnLatency.Add(elapsed)
}

func (n *nodeStats) snapshotConnLatency() LatencySnapshot {
	c := n.ConnLatency.Clone()
	return LatencySnapshot{
		Type:         LatencyConn,
		BucketCounts: append([]uint64(nil), c.Buckets...),
		Min:          c.Min,
		Max:          c.Max,
		Sum:          c.Sum,
		Count:        c.Count,
	}
}

func (n *nodeStats) addSharedBytes(namespace string, in, out int64) {
	if namespace == "" {
		return
	}
	op := n.getOrCreateNsOp(namespace)
	if in != 0 {
		op.BytesIn.AddAndGet(int(in))
	}
	if out != 0 {
		op.BytesOut.AddAndGet(int(out))
	}
}

// addSharedError updates shared namespace.* error/timeout split counters.
// Call once per failed attempt (Decision 14 / collection v36), including
// intermediate failures that later succeed via retry.
func (n *nodeStats) addSharedError(namespace string, resultCode types.ResultCode, clientTimeout bool) {
	if namespace == "" {
		return
	}
	op := n.getOrCreateNsOp(namespace)
	if clientTimeout {
		op.ClientTimeouts.IncrementAndGet()
		return
	}
	switch resultCode {
	case types.OK:
		return
	case types.TIMEOUT, types.QUERY_TIMEOUT:
		op.ServerTimeouts.IncrementAndGet()
	case types.KEY_BUSY, types.XDR_KEY_BUSY:
		op.KeyBusy.IncrementAndGet()
	case types.RECORD_TOO_BIG:
		op.RecordTooBig.IncrementAndGet()
	case types.DEVICE_OVERLOAD:
		op.DeviceOverload.IncrementAndGet()
	default:
		op.Errors.IncrementAndGet()
	}
}

func (n *nodeStats) addElapsedForCommand(namespace string, ct commandType, d time.Duration) {
	policy := n.getMetricPolicy()
	if policy == nil {
		return
	}
	n.addSharedLatency(namespace, latencyTypeForCommand(ct), policy.elapsedUnits(d))
}

func (src *nsOperationalMetrics) clone(n *nodeStats) *nsOperationalMetrics {
	dst := n.newNsOperationalMetrics()
	dst.Errors.Set(src.Errors.Get())
	dst.ClientTimeouts.Set(src.ClientTimeouts.Get())
	dst.ServerTimeouts.Set(src.ServerTimeouts.Get())
	dst.KeyBusy.Set(src.KeyBusy.Get())
	dst.RecordTooBig.Set(src.RecordTooBig.Get())
	dst.DeviceOverload.Set(src.DeviceOverload.Get())
	dst.BytesIn.Set(src.BytesIn.Get())
	dst.BytesOut.Set(src.BytesOut.Get())
	for i := range src.Latencies {
		cloned := src.Latencies[i].Clone()
		dst.Latencies[i].Merge(cloned)
	}
	return dst
}

func (src *nsOperationalMetrics) cloneAndReset(n *nodeStats) *nsOperationalMetrics {
	dst := n.newNsOperationalMetrics()
	e := src.Errors.CloneAndSet(0)
	dst.Errors.Set(e.Get())
	ct := src.ClientTimeouts.CloneAndSet(0)
	dst.ClientTimeouts.Set(ct.Get())
	st := src.ServerTimeouts.CloneAndSet(0)
	dst.ServerTimeouts.Set(st.Get())
	kb := src.KeyBusy.CloneAndSet(0)
	dst.KeyBusy.Set(kb.Get())
	rt := src.RecordTooBig.CloneAndSet(0)
	dst.RecordTooBig.Set(rt.Get())
	do := src.DeviceOverload.CloneAndSet(0)
	dst.DeviceOverload.Set(do.Get())
	bi := src.BytesIn.CloneAndSet(0)
	dst.BytesIn.Set(bi.Get())
	bo := src.BytesOut.CloneAndSet(0)
	dst.BytesOut.Set(bo.Get())
	for i := range src.Latencies {
		cloned := src.Latencies[i].CloneAndReset()
		dst.Latencies[i].Merge(cloned)
	}
	return dst
}

func (dst *nsOperationalMetrics) aggregate(src *nsOperationalMetrics) {
	dst.Errors.AddAndGet(src.Errors.Get())
	dst.ClientTimeouts.AddAndGet(src.ClientTimeouts.Get())
	dst.ServerTimeouts.AddAndGet(src.ServerTimeouts.Get())
	dst.KeyBusy.AddAndGet(src.KeyBusy.Get())
	dst.RecordTooBig.AddAndGet(src.RecordTooBig.Get())
	dst.DeviceOverload.AddAndGet(src.DeviceOverload.Get())
	dst.BytesIn.AddAndGet(src.BytesIn.Get())
	dst.BytesOut.AddAndGet(src.BytesOut.Get())
	for i := range src.Latencies {
		cloned := src.Latencies[i].Clone()
		dst.Latencies[i].Merge(cloned)
	}
}

func (n *nodeStats) cloneOperationalInto(target *nodeStats) {
	n.OperationalNamespaces.Range(func(key, value any) bool {
		ns := key.(string)
		src := value.(*nsOperationalMetrics)
		target.OperationalNamespaces.Store(ns, src.clone(n))
		return true
	})
}

func (n *nodeStats) cloneAndResetOperationalInto(target *nodeStats) {
	n.OperationalNamespaces.Range(func(key, value any) bool {
		ns := key.(string)
		src := value.(*nsOperationalMetrics)
		target.OperationalNamespaces.Store(ns, src.cloneAndReset(n))
		return true
	})
}

func (n *nodeStats) mergeOperational(other *nodeStats) {
	other.OperationalNamespaces.Range(func(key, value any) bool {
		ns := key.(string)
		src := value.(*nsOperationalMetrics)
		dst := n.getOrCreateNsOp(ns)
		dst.aggregate(src)
		return true
	})
}

func (n *nodeStats) reshapeOperational() {
	cols, shift := n.latencyShape()
	n.ConnLatency.Reshape(cols, shift)
	n.OperationalNamespaces.Range(func(_, value any) bool {
		op := value.(*nsOperationalMetrics)
		for i := range op.Latencies {
			if LatencyType(i) == LatencyConn {
				continue
			}
			op.Latencies[i].Reshape(cols, shift)
		}
		return true
	})
}

func (n *nodeStats) snapshotNamespaces() []NamespaceMetricsSnapshot {
	byNS := map[string]int{}
	var out []NamespaceMetricsSnapshot

	n.OperationalNamespaces.Range(func(key, value any) bool {
		ns := key.(string)
		op := value.(*nsOperationalMetrics)
		item := NamespaceMetricsSnapshot{
			Namespace:      ns,
			Errors:         int64(op.Errors.Get()),
			ClientTimeouts: int64(op.ClientTimeouts.Get()),
			ServerTimeouts: int64(op.ServerTimeouts.Get()),
			KeyBusy:        int64(op.KeyBusy.Get()),
			RecordTooBig:   int64(op.RecordTooBig.Get()),
			DeviceOverload: int64(op.DeviceOverload.Get()),
			BytesIn:        int64(op.BytesIn.Get()),
			BytesOut:       int64(op.BytesOut.Get()),
			Latencies:      make([]LatencySnapshot, latencyTypeMax),
		}
		for i := range op.Latencies {
			c := op.Latencies[i].Clone()
			item.Latencies[i] = LatencySnapshot{
				Type:         LatencyType(i),
				BucketCounts: append([]uint64(nil), c.Buckets...),
				Min:          c.Min,
				Max:          c.Max,
				Sum:          c.Sum,
				Count:        c.Count,
			}
		}
		byNS[ns] = len(out)
		out = append(out, item)
		return true
	})

	// Overlay language-optional per-command phase histograms (Decision 5).
	n.DetailedMetrics.Range(func(key, value any) bool {
		ns := key.(string)
		arr := value.(*detailedMetricsArray)
		cmds := snapshotCommandMetrics(arr)
		if len(cmds) == 0 {
			return true
		}
		if idx, ok := byNS[ns]; ok {
			out[idx].Commands = cmds
			return true
		}
		byNS[ns] = len(out)
		out = append(out, NamespaceMetricsSnapshot{
			Namespace: ns,
			Latencies: make([]LatencySnapshot, latencyTypeMax),
			Commands:  cmds,
		})
		return true
	})
	return out
}

func snapshotCommandMetrics(arr *detailedMetricsArray) []CommandMetricsSnapshot {
	var out []CommandMetricsSnapshot
	for ct := commandType(0); ct < ttMaxCommandTypes; ct++ {
		cm := arr[ct].Load()
		if cm == nil {
			continue
		}
		out = append(out, CommandMetricsSnapshot{
			Command:       ct.String(),
			ConnectionAq:  snapshotSyncHist(&cm.ConnectionAq),
			Latency:       snapshotSyncHist(&cm.Latency),
			Parsing:       snapshotSyncHist(&cm.Parsing),
			BytesSent:     snapshotSyncHist(&cm.BytesSent),
			BytesReceived: snapshotSyncHist(&cm.BytesReceived),
		})
	}
	return out
}

func snapshotSyncHist(h *hist.SyncHistogram[uint64]) HistogramSnapshot {
	if h == nil {
		return HistogramSnapshot{}
	}
	c := h.Clone()
	return HistogramSnapshot{
		BucketCounts: append([]uint64(nil), c.Buckets...),
		Min:          uint64(c.Min),
		Max:          uint64(c.Max),
		Sum:          c.Sum,
		Count:        c.Count,
	}
}

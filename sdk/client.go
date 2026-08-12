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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Client is the low-level primitive behind [Cluster]. It holds the connected
// core client plus SDK state: the per-namespace consistency-mode cache, the
// resolved system settings, and the configuration monitor.
//
// Most applications never construct one directly.
type Client struct {
	core        *as.Client
	clusterName string

	settings atomic.Pointer[SystemSettings]

	nsModeMu sync.RWMutex
	nsMode   map[string]Mode

	monitor   *configMonitor
	monitorMu sync.Mutex

	closed atomic.Bool
}

// newClient wraps a connected core client.
func newClient(core *as.Client, clusterName string, sys SystemSettings) *Client {
	c := &Client{
		core:        core,
		clusterName: clusterName,
		nsMode:      map[string]Mode{},
	}
	c.settings.Store(&sys)
	return c
}

// UnderlyingClient returns the core client, the escape hatch for anything the
// SDK does not wrap.
func (c *Client) UnderlyingClient() (*as.Client, error) {
	if c.closed.Load() {
		return nil, NewError(KindConnection, "client is closed")
	}
	return c.core, nil
}

// IsConnected reports whether the connection is live.
func (c *Client) IsConnected() bool {
	return !c.closed.Load() && c.core.IsConnected()
}

// ClusterName reports the configured cluster name, if any.
func (c *Client) ClusterName() string { return c.clusterName }

// SystemSettings reports the resolved system settings in effect.
func (c *Client) SystemSettings() SystemSettings { return *c.settings.Load() }

// setSystemSettings replaces the settings snapshot wholesale.
func (c *Client) setSystemSettings(s SystemSettings) { c.settings.Store(&s) }

// Close closes the connection and stops background work.
func (c *Client) Close() {
	if c.closed.Swap(true) {
		return
	}
	c.monitorMu.Lock()
	if c.monitor != nil {
		c.monitor.stop()
		c.monitor = nil
	}
	c.monitorMu.Unlock()
	c.core.Close()
}

// CreateSession opens a session bound to a behavior. A nil behavior selects
// [DefaultBehavior].
func (c *Client) CreateSession(b *Behavior) (*Session, error) {
	if c.closed.Load() {
		return nil, NewError(KindConnection, "client is closed")
	}
	if b == nil {
		b = DefaultBehavior()
	}
	return newSession(c, b, nil)
}

// namespaceMode resolves and caches a namespace's consistency mode.
func (c *Client) namespaceMode(namespace string) Mode {
	if namespace == "" {
		return ModeAP
	}
	c.nsModeMu.RLock()
	m, ok := c.nsMode[namespace]
	c.nsModeMu.RUnlock()
	if ok {
		return m
	}

	mode := ModeAP
	if sc, err := c.namespaceIsSC(namespace); err == nil && sc {
		mode = ModeSC
	}

	c.nsModeMu.Lock()
	c.nsMode[namespace] = mode
	c.nsModeMu.Unlock()
	return mode
}

// namespaceIsSC asks the cluster whether a namespace is strong-consistency.
func (c *Client) namespaceIsSC(namespace string) (bool, error) {
	nodes := c.core.GetNodes()
	if len(nodes) == 0 {
		return false, NewError(KindConnection, "cluster has no nodes")
	}
	cmd := "namespace/" + namespace
	for _, n := range nodes {
		info, err := n.RequestInfo(as.NewInfoPolicy(), cmd)
		if err != nil {
			continue
		}
		exists, sc := parseNamespaceInfoBody(info[cmd])
		if exists {
			return sc, nil
		}
	}
	return false, NewError(KindInvalidNamespace, "namespace %q is unknown to every node", namespace)
}

// parseNamespaceInfoBody extracts existence and the strong-consistency flag
// from a namespace info body.
func parseNamespaceInfoBody(body string) (exists bool, sc bool) {
	if body == "" || strings.HasPrefix(body, "ERROR") {
		return false, false
	}
	for _, kv := range strings.Split(body, ";") {
		name, value, found := strings.Cut(kv, "=")
		if !found {
			continue
		}
		switch name {
		case "type":
			if value == "unknown" {
				return false, false
			}
			exists = true
		case "strong-consistency":
			sc = value == "true"
			exists = true
		}
	}
	return exists, sc
}

// serverVersion reports the lowest server version across the cluster, as a
// comparable triple. A node whose version cannot be parsed makes the answer
// zero, which fails every capability probe.
func (c *Client) minServerVersion() (major, minor, patch int, ok bool) {
	nodes := c.core.GetNodes()
	if len(nodes) == 0 {
		return 0, 0, 0, false
	}
	major, minor, patch = 1<<30, 1<<30, 1<<30
	for _, n := range nodes {
		info, err := n.RequestInfo(as.NewInfoPolicy(), "build")
		if err != nil {
			return 0, 0, 0, false
		}
		ma, mi, pa, parsed := parseVersion(info["build"])
		if !parsed {
			return 0, 0, 0, false
		}
		if compareVersion(ma, mi, pa, major, minor, patch) < 0 {
			major, minor, patch = ma, mi, pa
		}
	}
	return major, minor, patch, true
}

// parseVersion parses a dotted server build string.
func parseVersion(s string) (major, minor, patch int, ok bool) {
	parts := strings.SplitN(s, ".", 4)
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	nums := make([]int, 3)
	for i := range 3 {
		// Trim any trailing build suffix such as "0-55" or "3c".
		field := parts[i]
		end := 0
		for end < len(field) && field[end] >= '0' && field[end] <= '9' {
			end++
		}
		if end == 0 {
			return 0, 0, 0, false
		}
		v, err := strconv.Atoi(field[:end])
		if err != nil {
			return 0, 0, 0, false
		}
		nums[i] = v
	}
	return nums[0], nums[1], nums[2], true
}

// compareVersion orders two version triples.
func compareVersion(aMa, aMi, aPa, bMa, bMi, bPa int) int {
	switch {
	case aMa != bMa:
		return aMa - bMa
	case aMi != bMi:
		return aMi - bMi
	default:
		return aPa - bPa
	}
}

// supportsVersion reports whether every node is at least the given version.
func (c *Client) supportsVersion(major, minor, patch int) bool {
	ma, mi, pa, ok := c.minServerVersion()
	if !ok {
		return false
	}
	return compareVersion(ma, mi, pa, major, minor, patch) >= 0
}

// SupportsMRT reports whether every node supports multi-record transactions
// (server 8.0+).
func (c *Client) SupportsMRT() bool { return c.supportsVersion(8, 0, 0) }

// SupportsCDTPathExpressions reports whether every node supports CDT path
// expressions (server 8.1.1+).
func (c *Client) SupportsCDTPathExpressions() bool { return c.supportsVersion(8, 1, 1) }

// SupportsBlobIndex reports whether every node supports blob secondary indexes
// (server 7.0+).
func (c *Client) SupportsBlobIndex() bool { return c.supportsVersion(7, 0, 0) }

// SupportsExpressionIndex reports whether every node supports
// expression-based secondary indexes (server 8.1.2+).
func (c *Client) SupportsExpressionIndex() bool { return c.supportsVersion(8, 1, 2) }

// SupportsStringOperations reports whether every node supports the server-side
// string operations (server 8.1.3+).
func (c *Client) SupportsStringOperations() bool { return c.supportsVersion(8, 1, 3) }

// SupportsExtendedErrorDetail reports whether every node returns error
// subcodes, server messages and expression traces (server 8.1.3+).
func (c *Client) SupportsExtendedErrorDetail() bool { return c.supportsVersion(8, 1, 3) }

// SupportsServerCompiledAEL reports whether every node compiles Aerospike
// Expression Language filter text (server 8.1.3+).
func (c *Client) SupportsServerCompiledAEL() bool { return c.supportsVersion(8, 1, 3) }

// Cluster owns one connection to the database. Closing it closes the
// connection for every session derived from it.
type Cluster struct {
	client *Client
}

// Client returns the SDK client behind the cluster.
func (c *Cluster) Client() *Client { return c.client }

// CreateSession opens a session bound to a behavior. A nil behavior selects
// [DefaultBehavior].
func (c *Cluster) CreateSession(b *Behavior) (*Session, error) { return c.client.CreateSession(b) }

// IsConnected reports whether the connection is live.
func (c *Cluster) IsConnected() bool { return c.client.IsConnected() }

// ClusterName reports the configured cluster name, if any.
func (c *Cluster) ClusterName() string { return c.client.ClusterName() }

// SystemSettings reports the resolved system settings in effect.
func (c *Cluster) SystemSettings() SystemSettings { return c.client.SystemSettings() }

// Close closes the connection and stops background work.
func (c *Cluster) Close() { c.client.Close() }

// SupportsMRT reports whether multi-record transactions are available.
func (c *Cluster) SupportsMRT() bool { return c.client.SupportsMRT() }

// SupportsCDTPathExpressions reports whether CDT path expressions are available.
func (c *Cluster) SupportsCDTPathExpressions() bool { return c.client.SupportsCDTPathExpressions() }

// SupportsStringOperations reports whether server-side string operations are
// available.
func (c *Cluster) SupportsStringOperations() bool { return c.client.SupportsStringOperations() }

// SupportsExtendedErrorDetail reports whether extended error detail is available.
func (c *Cluster) SupportsExtendedErrorDetail() bool { return c.client.SupportsExtendedErrorDetail() }

// SupportsServerCompiledAEL reports whether AEL filter text is available.
func (c *Cluster) SupportsServerCompiledAEL() bool { return c.client.SupportsServerCompiledAEL() }

// SupportsBlobIndex reports whether blob secondary indexes are available.
func (c *Cluster) SupportsBlobIndex() bool { return c.client.SupportsBlobIndex() }

// EnableMetrics starts client metrics collection.
func (c *Cluster) EnableMetrics(p *as.MetricsPolicy) error {
	core, err := c.client.UnderlyingClient()
	if err != nil {
		return err
	}
	core.EnableMetrics(p)
	return nil
}

// DisableMetrics stops client metrics collection.
func (c *Cluster) DisableMetrics() error {
	core, err := c.client.UnderlyingClient()
	if err != nil {
		return err
	}
	core.DisableMetrics()
	return nil
}

// MetricsEnabled reports whether metrics collection is on.
func (c *Cluster) MetricsEnabled() bool {
	core, err := c.client.UnderlyingClient()
	if err != nil {
		return false
	}
	return core.MetricsEnabled()
}

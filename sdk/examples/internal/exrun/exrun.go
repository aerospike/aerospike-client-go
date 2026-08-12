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

// Package exrun is the shared harness for the SDK examples.
//
// Every example exposes a Run(*Env) error, so the same code can be executed two
// ways: standalone from its own main, and from the integration suite, which
// calls each Run against a live cluster. That is what keeps the examples from
// silently rotting as the API evolves — a compile break or a behavior change
// fails the test run, not just a stale file nobody executes.
package exrun

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// Env is what an example needs from its caller: a live session, the namespaces
// to use, and somewhere to write its narration.
type Env struct {
	Session *sdk.Session
	Cluster *sdk.Cluster

	// Namespace is the ordinary (availability-mode) namespace.
	Namespace string
	// SCNamespace is a strong-consistency namespace, empty when none was
	// supplied. An example that needs one should skip cleanly.
	SCNamespace string

	// Out receives the example's narration.
	Out io.Writer
}

// Printf writes a line of narration.
func (e *Env) Printf(format string, args ...any) {
	fmt.Fprintf(e.Out, format+"\n", args...)
}

// SkipSC reports whether a strong-consistency example should stand down, and
// says so.
func (e *Env) SkipSC(what string) bool {
	if e.SCNamespace == "" {
		e.Printf("skipping %s: no strong-consistency namespace configured", what)
		return true
	}
	if !e.Cluster.SupportsMRT() {
		e.Printf("skipping %s: cluster does not support multi-record transactions", what)
		return true
	}
	return false
}

// DataSet mints a dataset in the ordinary namespace and truncates it, so a
// re-run starts from an empty set and set names do not accumulate.
func (e *Env) DataSet(set string) (*sdk.DataSet, error) {
	ds, err := sdk.DataSetOf(e.Namespace, set)
	if err != nil {
		return nil, err
	}
	if err := e.Session.Truncate(ds, 0); err != nil {
		return nil, err
	}
	return ds, nil
}

// SCDataSet is [Env.DataSet] for the strong-consistency namespace.
func (e *Env) SCDataSet(set string) (*sdk.DataSet, error) {
	ds, err := sdk.DataSetOf(e.SCNamespace, set)
	if err != nil {
		return nil, err
	}
	if err := e.Session.Truncate(ds, 0); err != nil {
		return nil, err
	}
	return ds, nil
}

// Dump prints every record in a set, sorted by user key.
//
// The default Behavior sends the user key with each write, so a set-wide query
// can report which record is which.
func (e *Env) Dump(ds *sdk.DataSet) error {
	stream, err := e.Session.Query(ds).Execute()
	if err != nil {
		return err
	}
	defer stream.Close()

	var lines []string
	for {
		row, err := stream.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		if row.Record == nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("  id %s: {%s}",
			KeyString(row.Key), BinsString(row.Record.Bins)))
	}
	sort.Strings(lines)
	e.Printf("  (%d record(s))", len(lines))
	for _, l := range lines {
		e.Printf("%s", l)
	}
	return nil
}

// KeyString renders a key's user value, or a placeholder when the server did
// not keep one.
func KeyString(key *as.Key) string {
	if key == nil || key.Value() == nil {
		return "<no key>"
	}
	return fmt.Sprintf("%v", key.Value().GetObject())
}

// BinsString renders a bin map with its names in sorted order, so output is
// stable across runs.
func BinsString(bins as.BinMap) string {
	names := make([]string, 0, len(bins))
	for n := range bins {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%v", n, Render(bins[n])))
	}
	return strings.Join(parts, ", ")
}

// Render formats a bin value, normalizing the collection shapes the server may
// return so output does not depend on a map's ordering.
func Render(v any) string {
	switch t := v.(type) {
	case nil:
		return "nil"
	case []as.MapPair:
		pairs := make([]string, 0, len(t))
		for _, p := range t {
			pairs = append(pairs, fmt.Sprintf("%v=%v", p.Key, Render(p.Value)))
		}
		sort.Strings(pairs)
		return "{" + strings.Join(pairs, ", ") + "}"
	case map[any]any:
		pairs := make([]string, 0, len(t))
		for k, val := range t {
			pairs = append(pairs, fmt.Sprintf("%v=%v", k, Render(val)))
		}
		sort.Strings(pairs)
		return "{" + strings.Join(pairs, ", ") + "}"
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, Render(e))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case []byte:
		return "0x" + strconv.QuoteToASCII(string(t))
	default:
		return fmt.Sprintf("%v", t)
	}
}

// Main is the standalone entry point every example's main delegates to.
//
// It reads the same environment the Rust and Java examples use, so the two
// suites are driven identically.
func Main(name string, run func(*Env) error) {
	hosts := os.Getenv("AEROSPIKE_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1:3000"
	}
	namespace := os.Getenv("AEROSPIKE_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}

	def := sdk.WithHosts(parseHosts(hosts)...)
	if v := strings.ToLower(os.Getenv("AEROSPIKE_USE_SERVICES_ALTERNATE")); v == "true" || v == "1" {
		def = def.UsingServicesAlternate()
	}
	cluster, err := def.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: connect: %v\n", name, err)
		os.Exit(1)
	}
	defer cluster.Close()

	session, err := cluster.CreateSession(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: session: %v\n", name, err)
		os.Exit(1)
	}

	env := &Env{
		Session:     session,
		Cluster:     cluster,
		Namespace:   namespace,
		SCNamespace: os.Getenv("AEROSPIKE_SC_NAMESPACE"),
		Out:         os.Stdout,
	}
	if err := run(env); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

// parseHosts splits a comma-separated host:port list.
func parseHosts(spec string) []*as.Host {
	var out []*as.Host
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.LastIndex(part, ":")
		if idx < 0 {
			out = append(out, as.NewHost(part, 3000))
			continue
		}
		port, err := strconv.Atoi(part[idx+1:])
		if err != nil {
			port = 3000
		}
		out = append(out, as.NewHost(part[:idx], port))
	}
	return out
}

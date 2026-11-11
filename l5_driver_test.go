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

package aerospike_test

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	buildinfo "runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// L5 driver for aerospike-client-conformance l5-aws/DRIVER.md, contract 1.
// rows.py makes every assertion. This process only measures.
//
// Java's L5Driver in the pluggable-discovery worktree is the behavioural
// reference, with two harness rules that Java's local driver has not caught
// up to: ready.contract is 1 (rows.py rejects any other value), and watch
// dials only the first address of one lookup. Java still dials every address
// for three rounds and reports contract 3, which this harness fails.

const (
	l5Contract       = 1
	l5Partitions     = 4096
	l5Poll           = 10 * time.Millisecond
	l5Heartbeat      = 100 * time.Millisecond
	l5ReachEvery     = 500 * time.Millisecond
	l5ReachTimeout   = time.Second
	l5ProbeEvery     = 10 * time.Millisecond
	l5ProbeTimeout   = 50 * time.Millisecond
	l5WatchBound     = 60 * time.Second
	l5QuitWait       = 30 * time.Second
	l5KeySearchLimit = 1000000
	l5Usage          = "usage: test/discovery/l5_driver.sh --seed HOST:PORT --shape pass-through|port-map " +
		"[--port-map NODE=HOST:PORT,...] [--on-miss strict|pass-through] [--user USER] --tend-interval MS " +
		"--timeout MS --namespace NS --set SET [--workers N] [--keys N]"
)

// TestL5Driver is the harness entry. Ordinary go test runs skip it.
// The launcher sets L5_DRIVER_RUN=1 and passes driver flags after --.
func TestL5Driver(t *testing.T) {
	if os.Getenv("L5_DRIVER_RUN") != "1" {
		t.Skip("L5 driver runs only from test/discovery/l5_driver.sh")
	}
	// Exit from the test so the testing package writes nothing to stdout.
	os.Exit(runL5Driver(flag.Args()))
}

func runL5Driver(args []string) int {
	opts, err := parseL5Options(args, os.LookupEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n%s\n", err, l5Usage)
		return 2
	}

	clock := newL5Clock()
	out := captureL5Stdout()
	debugLogs := os.Getenv("L5_DRIVER_DEBUG") == "1"
	installL5Logger(clock, out, debugLogs)

	client, err := as.NewClientWithPolicyAndHost(opts.policy(), opts.seed)
	if err != nil {
		msg := "client not created: " + err.Error()
		out.writeNow(l5Message{Type: "error", T: clock.ms(), Message: msg})
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", msg)
		return 3
	}

	driver := newL5Driver(opts, clock, out, client)
	driver.run()
	return 0
}

type l5Driver struct {
	opts      *l5Options
	clock     l5Clock
	out       *l5Output
	client    *as.Client
	load      *l5Load
	routing   *l5Routing
	reachable sync.Map
	checking  sync.Map
	watch     atomic.Pointer[l5Watch]
	reachStop chan struct{}
	reachDone chan struct{}
}

func newL5Driver(opts *l5Options, clock l5Clock, out *l5Output, client *as.Client) *l5Driver {
	driver := &l5Driver{
		opts:      opts,
		clock:     clock,
		out:       out,
		client:    client,
		reachStop: make(chan struct{}),
		reachDone: make(chan struct{}),
	}
	driver.load = newL5Load(client, clock, opts, driver.ready)
	driver.routing = newL5Routing(driver)
	return driver
}

// l5Store is the part of the client that the load and serve call.
type l5Store interface {
	PutBins(policy *as.WritePolicy, key *as.Key, bins ...*as.Bin) as.Error
	Get(policy *as.BasePolicy, key *as.Key, binNames ...string) (*as.Record, as.Error)
}

func (d *l5Driver) run() {
	go d.reachLoop()
	d.load.start()
	d.readCommands(os.Stdin)
	d.quit()
}

func (d *l5Driver) ready() {
	d.out.open(l5Ready{
		Type:          "ready",
		T:             d.clock.ms(),
		Contract:      l5Contract,
		EpochMs:       time.Now().UnixMilli(),
		Client:        "go",
		ClientVersion: l5ClientVersion(),
		Shape:         d.opts.shape,
	})
	go d.routing.loop()
}

func (d *l5Driver) readCommands(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if !d.command(scanner.Text()) {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin: %s\n", err)
	}
}

func (d *l5Driver) command(line string) bool {
	words := strings.Fields(line)
	if len(words) == 0 {
		d.fail("unknown command: " + line)
		return true
	}
	switch words[0] {
	case "serve":
		if len(words) != 2 {
			d.fail("malformed command: " + line)
		} else {
			served := l5Serve(d.client, d.opts, words[1])
			served.T = d.clock.ms()
			d.out.write(served)
		}
	case "reset":
		if len(words) != 1 {
			d.fail("malformed command: " + line)
		} else {
			t := d.clock.ms()
			d.load.latency.reset(d.clock.nano())
			d.routing.gap.reset(t)
			d.out.write(l5Reset{Type: "reset", T: t})
		}
	case "watch":
		if len(words) != 3 {
			d.fail("malformed command: " + line)
			return true
		}
		host, port, ok := l5HostPort(words[1])
		if !ok {
			d.fail("malformed command: " + line)
			return true
		}
		watch := newL5Watch(words[1], host, port, words[2])
		if !d.watch.CompareAndSwap(nil, watch) {
			d.fail("a watch is already running: " + line)
			return true
		}
		go watch.run(d)
	case "quit":
		if len(words) != 1 {
			d.fail("malformed command: " + line)
			return true
		}
		return false
	default:
		d.fail("unknown command: " + line)
	}
	return true
}

func (d *l5Driver) fail(message string) {
	d.out.write(l5Message{Type: "error", T: d.clock.ms(), Message: message})
}

func (d *l5Driver) quit() {
	deadline := time.Now().Add(l5QuitWait)
	d.load.stop(deadline)
	if watch := d.watch.Load(); watch != nil {
		watch.stop(deadline)
	}
	close(d.reachStop)
	select {
	case <-d.reachDone:
	case <-time.After(time.Second):
	}
	d.routing.stop()
	d.client.Close()
}

// l5ServeKeys returns, for each partition, the first of STEP-0, STEP-1, ...
// that falls in it, or nil when the search limit runs out first.
func l5ServeKeys(namespace, set, step string) []*as.Key {
	keys := make([]*as.Key, l5Partitions)
	found := 0
	for i := 0; i < l5KeySearchLimit && found < l5Partitions; i++ {
		key, err := as.NewKey(namespace, set, step+"-"+strconv.Itoa(i))
		if err != nil || keys[key.PartitionId()] != nil {
			continue
		}
		keys[key.PartitionId()] = key
		found++
	}
	return keys
}

// l5Serve puts and gets one key per partition, one attempt each. The caller
// sets t when it writes the reply.
func l5Serve(store l5Store, opts *l5Options, step string) l5Served {
	keys := l5ServeKeys(opts.namespace, opts.set, step)
	start := time.Now()
	served := l5Served{Type: "served", Step: step, Partitions: l5Partitions}
	note := func(partition int, message string) {
		served.Failures++
		if served.FirstFailure == nil {
			message = "partition " + strconv.Itoa(partition) + message
			served.FirstFailure = &message
		}
	}
	for partition, key := range keys {
		if key == nil {
			note(partition, ": no key in "+strconv.Itoa(l5KeySearchLimit)+" candidates")
			continue
		}
		value := step + "-" + strconv.Itoa(partition)
		if err := store.PutBins(opts.write, key, as.NewBin("v", value)); err != nil {
			note(partition, " put: "+err.Error())
			continue
		}
		served.Written++
		record, err := store.Get(opts.read, key, "v")
		switch {
		case err != nil && !l5NotFound(err):
			note(partition, " get: "+err.Error())
		case err == nil && l5StringBin(record, "v") == value:
			served.Read++
		default:
			served.Mismatched++
		}
	}
	served.ElapsedMs = time.Since(start).Milliseconds()
	return served
}

// l5NotFound reports a missing record. Go's Get returns KEY_NOT_FOUND_ERROR
// where Java's get returns a null record.
func l5NotFound(err as.Error) bool {
	return err != nil && err.Matches(types.KEY_NOT_FOUND_ERROR)
}

// l5StringBin is the bin's string value, or "" when the record or bin is
// missing or not a string. serve values are never empty.
func l5StringBin(record *as.Record, name string) string {
	if record == nil {
		return ""
	}
	value, _ := record.Bins[name].(string)
	return value
}

func (d *l5Driver) reachLoop() {
	defer close(d.reachDone)
	ticker := time.NewTicker(l5ReachEvery)
	defer ticker.Stop()
	d.checkReachable()
	for {
		select {
		case <-d.reachStop:
			return
		case <-ticker.C:
			d.checkReachable()
		}
	}
}

func (d *l5Driver) checkReachable() {
	nodes := d.client.GetNodes()
	live := make(map[*as.Node]struct{}, len(nodes))
	for _, node := range nodes {
		live[node] = struct{}{}
		if _, checking := d.checking.LoadOrStore(node, struct{}{}); checking {
			continue
		}
		go func(node *as.Node) {
			defer d.checking.Delete(node)
			d.reachable.Store(node, l5Reachable(node))
		}(node)
	}
	d.reachable.Range(func(key, _ any) bool {
		if _, ok := live[key.(*as.Node)]; !ok {
			d.reachable.Delete(key)
		}
		return true
	})
}

// l5Reachable asks for the node's name on a pooled connection, as Java's
// Info.request does. Node.RequestInfo would run on the tend connection, so a
// slow check would delay the tend it measures and close its connection.
func l5Reachable(node *as.Node) bool {
	conn, err := node.GetConnection(l5ReachTimeout)
	if err != nil {
		return false
	}
	if err := conn.SetTimeout(time.Now().Add(l5ReachTimeout), l5ReachTimeout); err != nil {
		node.InvalidateConnection(conn)
		return false
	}
	info, err := conn.RequestInfo("node")
	if err != nil {
		node.InvalidateConnection(conn)
		return false
	}
	node.PutConnection(conn)
	return info["node"] == node.GetName()
}

func (d *l5Driver) usable(name string) bool {
	nodes := d.client.GetNodes()
	held := l5NodeByName(nodes, name)
	if held == nil || !held.IsActive() {
		return false
	}
	unowned, _ := l5PartitionView(d.client, nodes, d.opts.namespace)
	return unowned == 0
}

func l5NodeByName(nodes []*as.Node, name string) *as.Node {
	for _, node := range nodes {
		if node.GetName() == name {
			return node
		}
	}
	return nil
}

// l5PartitionView returns how many partitions are unowned and the master of
// each owned partition. A partition is unowned when its master is missing,
// inactive, or not the node the client currently holds under that name.
func l5PartitionView(client *as.Client, nodes []*as.Node, namespace string) (int, []*as.Node) {
	byName := make(map[string]*as.Node, len(nodes))
	for _, node := range nodes {
		byName[node.GetName()] = node
	}
	masters := as.PartitionMasters(client, namespace)
	unowned := 0
	for i, master := range masters {
		if master == nil || !master.IsActive() || byName[master.GetName()] != master {
			masters[i] = nil
			unowned++
		}
	}
	return unowned, masters
}

// l5Routing runs on one goroutine. Only gap is shared, with reset.
type l5Routing struct {
	driver *l5Driver
	gap    l5Gap
	seen   bool
	last   string
	next   int64
	peerIP map[*as.Node]string
	stopCh chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newL5Routing(driver *l5Driver) *l5Routing {
	return &l5Routing{
		driver: driver, gap: l5Gap{since: -1}, peerIP: map[*as.Node]string{},
		stopCh: make(chan struct{}), done: make(chan struct{}),
	}
}

func (r *l5Routing) loop() {
	defer close(r.done)
	ticker := time.NewTicker(l5Poll)
	defer ticker.Stop()
	r.poll()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.poll()
		}
	}
}

func (r *l5Routing) stop() {
	r.once.Do(func() { close(r.stopCh) })
	select {
	case <-r.done:
	case <-time.After(time.Second):
	}
}

func (r *l5Routing) poll() {
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintf(os.Stderr, "routing poll: %v\n", recovered)
		}
	}()
	t := r.driver.clock.ms()
	nodes := append([]*as.Node(nil), r.driver.client.GetNodes()...)
	slices.SortFunc(nodes, func(a, b *as.Node) int { return strings.Compare(a.GetName(), b.GetName()) })
	unowned, masters := l5PartitionView(r.driver.client, nodes, r.driver.opts.namespace)
	longest := r.gap.observe(t, unowned)
	signature := r.signature(unowned, nodes)
	change, heartbeat := r.note(signature, t)
	if change {
		r.snapshot("change", t, nodes, masters, unowned, longest)
	}
	if heartbeat {
		r.snapshot("heartbeat", t, nodes, masters, unowned, longest)
	}
}

func (r *l5Routing) signature(unowned int, nodes []*as.Node) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(unowned))
	for _, node := range nodes {
		host, port := l5Connected(node)
		fmt.Fprintf(&b, "|%s,%s,%d,%t", node.GetName(), host, port, node.IsActive())
	}
	return b.String()
}

func (r *l5Routing) note(signature string, t int64) (change, heartbeat bool) {
	if !r.seen {
		r.next = t
		r.seen = true
	} else if signature != r.last {
		change = true
	}
	r.last = signature
	if t >= r.next {
		heartbeat = true
		r.next = max(r.next+l5Heartbeat.Milliseconds(), t+1)
	}
	return change, heartbeat
}

func (r *l5Routing) snapshot(reason string, t int64, nodes []*as.Node, masters []*as.Node, unowned int, longest int64) {
	views := make([]l5NodeView, 0, len(nodes))
	for _, node := range nodes {
		host, port := l5Connected(node)
		views = append(views, l5NodeView{
			Name: node.GetName(), Host: host, Port: port, Address: r.peerAddress(node, host),
			Active: node.IsActive(), Reachable: r.driver.reachOf(node),
		})
	}
	owners := map[string]int{}
	for _, master := range masters {
		if master != nil {
			owners[master.GetName()]++
		}
	}
	r.driver.out.write(l5Snapshot{
		Type: "snapshot", T: t, Reason: reason, Nodes: views,
		Unowned: unowned, Owners: owners, LongestUnownedMs: longest,
		Succeeded: r.driver.load.succeeded.Load(), Failed: r.driver.load.failed.Load(),
		MaxLatencyMs: r.driver.load.latency.ms(), LastError: r.driver.load.lastError.Load(),
	})
}

func (d *l5Driver) reachOf(node *as.Node) *bool {
	value, ok := d.reachable.Load(node)
	if !ok {
		return nil
	}
	reachable := value.(bool)
	return &reachable
}

// l5Connected is the host and port the client stored for the node. On a
// pass-through cluster that host is the advertised name, not the IP the
// socket connected to.
func l5Connected(node *as.Node) (string, int) {
	host := node.GetHost()
	if host == nil {
		return "", 0
	}
	return host.Name, host.Port
}

// peerAddress is the IPv4 of the node's live tend connection. W-5 passes it to
// endpoint-zone. A missed read keeps the previous IP so a tend blip does not
// report the hostname. Only the routing goroutine calls this.
func (r *l5Routing) peerAddress(node *as.Node, host string) string {
	if ip := as.TendConnectionIPv4(node); ip != "" {
		r.peerIP[node] = ip
		return ip
	}
	if ip := r.peerIP[node]; ip != "" {
		return ip
	}
	return host
}

// l5Gap is the longest stretch since reset with unowned partitions, including
// a stretch that is still open.
type l5Gap struct {
	mu     sync.Mutex
	since  int64
	closed int64
}

func (g *l5Gap) observe(t int64, unowned int) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if unowned > 0 && g.since < 0 {
		g.since = t
	} else if unowned == 0 && g.since >= 0 {
		g.closed = max(g.closed, t-g.since)
		g.since = -1
	}
	if g.since >= 0 {
		return max(g.closed, t-g.since)
	}
	return g.closed
}

func (g *l5Gap) reset(t int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = 0
	if g.since >= 0 {
		g.since = t
	}
}

// l5Load runs one command at a time per worker and issues a failed command
// again after one tend interval until it succeeds. Failures reach rows.py only
// through the snapshot's failed and lastError: contract 1 has no failure line.
type l5Load struct {
	store     l5Store
	clock     l5Clock
	opts      *l5Options
	onReady   func()
	succeeded atomic.Int64
	failed    atomic.Int64
	lastError atomic.Pointer[string]
	latency   l5Latency
	stopCh    chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	readyOnce sync.Once
}

func newL5Load(store l5Store, clock l5Clock, opts *l5Options, onReady func()) *l5Load {
	return &l5Load{store: store, clock: clock, opts: opts, onReady: onReady,
		stopCh: make(chan struct{}), done: make(chan struct{})}
}

func (l *l5Load) start() {
	var wg sync.WaitGroup
	wg.Add(l.opts.workers)
	for worker := 0; worker < l.opts.workers; worker++ {
		go func() {
			defer wg.Done()
			l.run(worker)
		}()
	}
	go func() {
		wg.Wait()
		close(l.done)
	}()
}

func (l *l5Load) run(worker int) {
	for i := int64(0); !l.stopped(); i++ {
		first := l.clock.nano()
		for {
			err := l.command(worker, i)
			if err == nil {
				break
			}
			l.failed.Add(1)
			message := err.Error()
			l.lastError.Store(&message)
			if !l.sleep() {
				return
			}
		}
		l.succeeded.Add(1)
		l.latency.record(first, l.clock.nano())
		l.readyOnce.Do(l.onReady)
	}
}

// command is iteration i of worker: a put on even i, a get on odd i. A get of
// a key not written yet succeeds, as Java's get returning null does.
func (l *l5Load) command(worker int, i int64) as.Error {
	key, err := as.NewKey(l.opts.namespace, l.opts.set, l5LoadKey(worker, i, l.opts.workers, l.opts.keys))
	if err != nil {
		return err
	}
	if i%2 == 0 {
		return l.store.PutBins(l.opts.write, key, as.NewBin("v", i))
	}
	if _, err := l.store.Get(l.opts.read, key); err != nil && !l5NotFound(err) {
		return err
	}
	return nil
}

func l5LoadKey(worker int, iteration int64, workers, keys int) string {
	k := (int64(worker) + iteration*int64(workers)) % int64(keys)
	return "load-" + strconv.FormatInt(k, 10)
}

func (l *l5Load) sleep() bool {
	timer := time.NewTimer(l.opts.tend)
	defer timer.Stop()
	select {
	case <-l.stopCh:
		return false
	case <-timer.C:
		return !l.stopped()
	}
}

func (l *l5Load) stopped() bool {
	select {
	case <-l.stopCh:
		return true
	default:
		return false
	}
}

// stop lets each worker finish its current attempt, giving up at deadline.
func (l *l5Load) stop(deadline time.Time) {
	l.stopOnce.Do(func() { close(l.stopCh) })
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-l.done:
	case <-timer.C:
	}
}

type l5Latency struct {
	mu      sync.Mutex
	resetAt int64
	max     int64
}

// record counts a command from its first attempt, and only if that attempt
// started at or after the last reset.
func (l *l5Latency) record(first, end int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if first >= l.resetAt {
		l.max = max(l.max, end-first)
	}
}

func (l *l5Latency) reset(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resetAt = now
	l.max = 0
}

func (l *l5Latency) ms() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.max / int64(time.Millisecond)
}

type l5Watch struct {
	address string
	node    string
	probe   *l5Probe
	stopCh  chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newL5Watch(address, host string, port int, node string) *l5Watch {
	return &l5Watch{
		address: address,
		node:    node,
		probe:   newL5Probe(host, port, l5Lookup, l5Dial),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (w *l5Watch) run(d *l5Driver) {
	defer close(w.done)
	defer d.watch.CompareAndSwap(w, nil)
	defer func() {
		if recovered := recover(); recovered != nil {
			d.fail(fmt.Sprintf("watch %s %s: %v", w.address, w.node, recovered))
		}
	}()
	for {
		begin := time.Now()
		w.probeIfDown(d.clock.ms())
		if w.probe.servingAt != nil {
			t := d.clock.ms()
			if d.usable(w.node) {
				w.reply(d, &t)
				return
			}
			if t-*w.probe.servingAt >= l5WatchBound.Milliseconds() {
				w.reply(d, nil)
				return
			}
		}
		wait := l5ProbeEvery - time.Since(begin)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-w.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// probeIfDown dials until one attempt connects. Later checks only ask whether
// the node is usable, so a dead second address cannot move servingAt.
func (w *l5Watch) probeIfDown(t int64) {
	if w.probe.servingAt == nil {
		w.probe.attempt(t)
	}
}

func (w *l5Watch) reply(d *l5Driver, usableAt *int64) {
	var usableMs *int64
	if usableAt != nil && w.probe.servingAt != nil {
		span := *usableAt - *w.probe.servingAt
		usableMs = &span
	}
	d.out.write(l5WatchReply{
		Type: "watch", T: d.clock.ms(), Address: w.address, Node: w.node,
		ServingAt: w.probe.servingAt, LastClosedAt: w.probe.lastClosedAt,
		UsableAt: usableAt, UsableMs: usableMs, OwnershipChecked: true,
	})
}

// stop waits until the watch replies or the deadline passes, then cancels it.
// A watch that has already found the listener gets to report usable.
func (w *l5Watch) stop(deadline time.Time) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-w.done:
		return
	case <-timer.C:
		w.once.Do(func() { close(w.stopCh) })
	}
	select {
	case <-w.done:
	case <-time.After(time.Second):
	}
}

// l5Probe dials one resolved address. Lookup runs until it returns an address,
// then that first address is the only one dialed for the rest of the watch.
// Dialing the name would let Go walk every address and split the 50 ms timeout.
type l5Probe struct {
	host         string
	port         int
	lookup       func(string) ([]string, error)
	dial         func(addr string, timeout time.Duration) error
	ip           string
	servingAt    *int64
	lastClosedAt *int64
}

func newL5Probe(host string, port int, lookup func(string) ([]string, error), dial func(string, time.Duration) error) *l5Probe {
	return &l5Probe{host: host, port: port, lookup: lookup, dial: dial}
}

func (p *l5Probe) attempt(t int64) {
	if p.ip == "" {
		addrs, err := p.lookup(p.host)
		if err != nil || len(addrs) == 0 {
			closed := t
			p.lastClosedAt = &closed
			return
		}
		p.ip = addrs[0]
	}
	if err := p.dial(net.JoinHostPort(p.ip, strconv.Itoa(p.port)), l5ProbeTimeout); err != nil {
		closed := t
		p.lastClosedAt = &closed
		return
	}
	if p.servingAt == nil {
		opened := t
		p.servingAt = &opened
	}
}

func l5Lookup(host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(context.Background(), host)
}

func l5Dial(addr string, timeout time.Duration) error {
	conn, err := (&net.Dialer{Timeout: timeout}).Dial("tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

type l5Options struct {
	seed       *as.Host
	shape      string
	portMap    map[string]string
	onMiss     as.TranslationMissMode
	user       string
	password   string
	translator as.AddressTranslator
	tend       time.Duration
	timeout    time.Duration
	namespace  string
	set        string
	workers    int
	keys       int
	read       *as.BasePolicy
	write      *as.WritePolicy
}

func (o *l5Options) policy() *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.Timeout = o.timeout
	policy.TendInterval = o.tend
	policy.User = o.user
	policy.Password = o.password
	policy.TranslationOnMiss = o.onMiss
	policy.UseServicesAlternate = o.shape == "pass-through"
	policy.AddressTranslator = o.translator
	return policy
}

type l5Arg struct {
	name  string
	value string
}

func parseL5Options(args []string, getenv func(string) (string, bool)) (*l5Options, error) {
	var parsed []l5Arg
	seen := map[string]struct{}{}
	for i := 0; i < len(args); i += 2 {
		if !strings.HasPrefix(args[i], "--") {
			return nil, fmt.Errorf("unexpected argument: %s", args[i])
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("%s needs a value", args[i])
		}
		if _, exists := seen[args[i]]; exists {
			return nil, fmt.Errorf("%s given twice", args[i])
		}
		seen[args[i]] = struct{}{}
		parsed = append(parsed, l5Arg{name: args[i], value: args[i+1]})
	}

	opts := &l5Options{onMiss: as.TranslationMissStrict, workers: 4, keys: 1000}
	for _, arg := range parsed {
		name, value := arg.name, arg.value
		switch name {
		case "--seed":
			host, port, ok := l5HostPort(value)
			if !ok {
				return nil, fmt.Errorf("--seed must be HOST:PORT: %s", value)
			}
			opts.seed = as.NewHost(host, port)
		case "--shape":
			if value != "pass-through" && value != "port-map" {
				return nil, fmt.Errorf("--shape must be pass-through or port-map: %s", value)
			}
			opts.shape = value
		case "--port-map":
			parsed, err := l5PortMap(value)
			if err != nil {
				return nil, err
			}
			opts.portMap = parsed
		case "--on-miss":
			switch value {
			case "strict":
				opts.onMiss = as.TranslationMissStrict
			case "pass-through":
				opts.onMiss = as.TranslationMissPassThrough
			default:
				return nil, fmt.Errorf("--on-miss must be strict or pass-through: %s", value)
			}
		case "--user":
			password, ok := getenv("AEROSPIKE_ADMIN_PASSWORD")
			if !ok {
				return nil, fmt.Errorf("--user needs AEROSPIKE_ADMIN_PASSWORD in the environment")
			}
			opts.user = value
			opts.password = password
		case "--tend-interval":
			n, err := l5Positive(name, value)
			if err != nil {
				return nil, err
			}
			opts.tend = time.Duration(n) * time.Millisecond
		case "--timeout":
			n, err := l5Positive(name, value)
			if err != nil {
				return nil, err
			}
			opts.timeout = time.Duration(n) * time.Millisecond
		case "--namespace":
			opts.namespace = value
		case "--set":
			opts.set = value
		case "--workers":
			n, err := l5Positive(name, value)
			if err != nil {
				return nil, err
			}
			opts.workers = n
		case "--keys":
			n, err := l5Positive(name, value)
			if err != nil {
				return nil, err
			}
			opts.keys = n
		default:
			return nil, fmt.Errorf("unknown flag: %s", name)
		}
	}
	if opts.seed == nil {
		return nil, fmt.Errorf("missing --seed")
	}
	if opts.shape == "" {
		return nil, fmt.Errorf("missing --shape")
	}
	if opts.tend <= 0 {
		return nil, fmt.Errorf("missing --tend-interval")
	}
	if opts.timeout <= 0 {
		return nil, fmt.Errorf("missing --timeout")
	}
	if opts.namespace == "" {
		return nil, fmt.Errorf("missing --namespace")
	}
	if opts.set == "" {
		return nil, fmt.Errorf("missing --set")
	}
	if opts.shape == "port-map" {
		if len(opts.portMap) == 0 {
			return nil, fmt.Errorf("missing --port-map with --shape port-map")
		}
		translator, err := as.NewPortMapAddressTranslator(opts.portMap)
		if err != nil {
			return nil, fmt.Errorf("--port-map: %s", err.Error())
		}
		opts.translator = translator
	} else if opts.portMap != nil {
		return nil, fmt.Errorf("--port-map only with --shape port-map")
	} else {
		opts.translator = as.NewPassThroughAddressTranslator()
	}

	opts.read = as.NewPolicy()
	opts.read.SocketTimeout = opts.timeout
	opts.read.TotalTimeout = opts.timeout
	opts.read.MaxRetries = 0
	opts.write = as.NewWritePolicy(0, 0)
	opts.write.SocketTimeout = opts.timeout
	opts.write.TotalTimeout = opts.timeout
	opts.write.MaxRetries = 0
	return opts, nil
}

func l5Positive(flag, value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer: %s", flag, value)
	}
	return n, nil
}

func l5HostPort(value string) (string, int, bool) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil || host == "" {
		return "", 0, false
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, false
	}
	return host, port, true
}

func l5PortMap(value string) (map[string]string, error) {
	parsed := map[string]string{}
	for _, entry := range strings.Split(value, ",") {
		eq := strings.IndexByte(entry, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("--port-map entry must be NODE=HOST:PORT: %s", entry)
		}
		name := entry[:eq]
		target := entry[eq+1:]
		if _, _, ok := l5HostPort(target); !ok {
			return nil, fmt.Errorf("--port-map entry must be NODE=HOST:PORT: %s", entry)
		}
		if _, exists := parsed[name]; exists {
			return nil, fmt.Errorf("--port-map names %s twice", name)
		}
		parsed[name] = target
	}
	return parsed, nil
}

type l5Clock struct{ start time.Time }

func newL5Clock() l5Clock { return l5Clock{start: time.Now()} }

func (c l5Clock) ms() int64   { return time.Since(c.start).Milliseconds() }
func (c l5Clock) nano() int64 { return time.Since(c.start).Nanoseconds() }

type l5Output struct {
	mu     sync.Mutex
	stdout io.Writer
	held   [][]byte
	opened bool
}

func captureL5Stdout() *l5Output {
	stdout := os.Stdout
	os.Stdout = os.Stderr
	return &l5Output{stdout: stdout}
}

func (o *l5Output) write(v any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	line := l5JSON(v)
	if !o.opened {
		o.held = append(o.held, line)
		return
	}
	o.print(line)
}

func (o *l5Output) writeNow(v any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.print(l5JSON(v))
}

func (o *l5Output) open(ready any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.print(l5JSON(ready))
	for _, line := range o.held {
		o.print(line)
	}
	o.held = nil
	o.opened = true
}

func (o *l5Output) print(line []byte) {
	_, _ = o.stdout.Write(append(line, '\n'))
}

func l5JSON(v any) []byte {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "json: %s\n", err)
		return []byte(`{"type":"error","t":0,"message":"json encode failed"}`)
	}
	return []byte(strings.TrimRight(buf.String(), "\n"))
}

type l5Ready struct {
	Type          string `json:"type"`
	T             int64  `json:"t"`
	Contract      int    `json:"contract"`
	EpochMs       int64  `json:"epochMs"`
	Client        string `json:"client"`
	ClientVersion string `json:"clientVersion"`
	Shape         string `json:"shape"`
}

type l5Message struct {
	Type    string `json:"type"`
	T       int64  `json:"t"`
	Message string `json:"message"`
}

type l5Reset struct {
	Type string `json:"type"`
	T    int64  `json:"t"`
}

type l5Served struct {
	Type         string  `json:"type"`
	T            int64   `json:"t"`
	Step         string  `json:"step"`
	Partitions   int     `json:"partitions"`
	Written      int     `json:"written"`
	Read         int     `json:"read"`
	Mismatched   int     `json:"mismatched"`
	Failures     int     `json:"failures"`
	FirstFailure *string `json:"firstFailure"`
	ElapsedMs    int64   `json:"elapsedMs"`
}

type l5WatchReply struct {
	Type             string `json:"type"`
	T                int64  `json:"t"`
	Address          string `json:"address"`
	Node             string `json:"node"`
	ServingAt        *int64 `json:"servingAt"`
	LastClosedAt     *int64 `json:"lastClosedAt"`
	UsableAt         *int64 `json:"usableAt"`
	UsableMs         *int64 `json:"usableMs"`
	OwnershipChecked bool   `json:"ownershipChecked"`
}

type l5NodeView struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Address   string `json:"address"`
	Active    bool   `json:"active"`
	Reachable *bool  `json:"reachable"`
}

type l5Snapshot struct {
	Type             string         `json:"type"`
	T                int64          `json:"t"`
	Reason           string         `json:"reason"`
	Nodes            []l5NodeView   `json:"nodes"`
	Unowned          int            `json:"unowned"`
	Owners           map[string]int `json:"owners"`
	LongestUnownedMs int64          `json:"longestUnownedMs"`
	Succeeded        int64          `json:"succeeded"`
	Failed           int64          `json:"failed"`
	MaxLatencyMs     int64          `json:"maxLatencyMs"`
	LastError        *string        `json:"lastError"`
}

type l5LogLine struct {
	Type    string `json:"type"`
	T       int64  `json:"t"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type l5Logger struct {
	clock l5Clock
	out   *l5Output
	debug bool
}

// installL5Logger forwards WARNING and above, all labelled WARN, because the
// logger hook is not told each line's level. Java also forwards INFO node
// events. Here a node move shows only as a host change in the next snapshot,
// and as a "Move node" line under L5_DRIVER_DEBUG=1.
func installL5Logger(clock l5Clock, out *l5Output, debug bool) {
	hook := &l5Logger{clock: clock, out: out, debug: debug}
	logger.Logger.SetLogger(hook)
	if debug {
		logger.Logger.SetLevel(logger.DEBUG)
		return
	}
	logger.Logger.SetLevel(logger.WARNING)
}

func (l *l5Logger) Printf(format string, v ...any) {
	message := strings.ReplaceAll(fmt.Sprintf(format, v...), "\n", " ")
	if l.debug {
		fmt.Fprintf(os.Stderr, "t=%d %s\n", l.clock.ms(), message)
		return
	}
	l.out.write(l5LogLine{Type: "log", T: l.clock.ms(), Level: "WARN", Message: message})
}

func l5ClientVersion() string {
	info, ok := buildinfo.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "(devel)"
	}
	return info.Main.Version
}

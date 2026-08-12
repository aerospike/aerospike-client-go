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
	"bytes"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

// configPollInterval is how often the monitor checks the file.
const configPollInterval = time.Second

// configMonitor watches the SDK configuration file and applies changes.
//
// It uses three gates, each cheaper than the work it guards: modification time,
// then raw-byte equality, then resolved-settings equality. A failed reload
// keeps the last-good settings, so a broken edit never reverts a running
// client to defaults.
type configMonitor struct {
	client       *Client
	path         string
	clusterName  string
	programmatic SystemSettings

	lastMtime time.Time
	lastRaw   []byte
	current   SystemSettings

	stopOnce sync.Once
	done     chan struct{}
}

// startConfigMonitor arms the hot-reload monitor.
func (c *Client) startConfigMonitor(loaded loadedConfig, clusterName string, programmatic SystemSettings) {
	m := &configMonitor{
		client:       c,
		path:         loaded.path,
		clusterName:  clusterName,
		programmatic: programmatic,
		lastRaw:      loaded.raw,
		current:      c.SystemSettings(),
		done:         make(chan struct{}),
	}
	if fi, err := os.Stat(loaded.path); err == nil {
		m.lastMtime = fi.ModTime()
	}

	c.monitorMu.Lock()
	c.monitor = m
	c.monitorMu.Unlock()

	go m.run()
}

// run polls until stopped or the client closes.
func (m *configMonitor) run() {
	ticker := time.NewTicker(configPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			if m.client.closed.Load() {
				return
			}
			m.pollOnce()
		}
	}
}

// stop ends the monitor. It is idempotent.
func (m *configMonitor) stop() {
	m.stopOnce.Do(func() { close(m.done) })
}

// pollOnce runs the three gates and applies a real change.
func (m *configMonitor) pollOnce() {
	fi, err := os.Stat(m.path)
	if err != nil {
		// Deleted or unreadable: keep the last-good settings, and deliberately
		// do not update lastRaw, so restoring the file is noticed.
		logger.Logger.Warn("sdk config: cannot stat %q: %s; keeping last-good settings", m.path, err)
		return
	}
	if !fi.ModTime().After(m.lastMtime) {
		return
	}
	m.lastMtime = fi.ModTime()

	raw, err := os.ReadFile(m.path)
	if err != nil {
		logger.Logger.Warn("sdk config: cannot read %q: %s; keeping last-good settings", m.path, err)
		return
	}
	if bytes.Equal(raw, m.lastRaw) {
		return
	}

	loaded := readConfigFile(m.path)
	if loaded.system == nil && loaded.behaviors == nil && len(raw) > 0 {
		// Parsed to nothing from non-empty content: treat as a broken edit.
		logger.Logger.Warn("sdk config: %q parsed to an empty configuration; keeping last-good settings", m.path)
		return
	}
	m.lastRaw = raw

	applyBehaviors(loaded.behaviors)

	resolved := resolveSystemSettings(loaded, m.clusterName, m.programmatic)
	if reflect.DeepEqual(resolved, m.current) {
		return
	}
	m.current = resolved
	m.client.setSystemSettings(resolved)
	logger.Logger.Info("sdk config: reloaded %q", m.path)
}

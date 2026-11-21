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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// line is one JSON output object.
type line map[string]any

// driverOutput writes JSON lines with t stamped under its lock, so t never
// goes back. Until ready is written, lines go to stderr.
type driverOutput struct {
	start time.Time

	lock    sync.Mutex
	stdout  io.Writer
	stderr  io.Writer
	open    bool
	encoded bytes.Buffer
}

func newDriverOutput(stdout, stderr io.Writer) *driverOutput {
	return &driverOutput{start: time.Now(), stdout: stdout, stderr: stderr}
}

// elapsed is the time on the monotonic clock since the driver started.
func (output *driverOutput) elapsed() time.Duration {
	return time.Since(output.start)
}

// now is elapsed in whole milliseconds, the t of every line.
func (output *driverOutput) now() int64 {
	return output.elapsed().Milliseconds()
}

// sleepUntil sleeps until elapsed reaches at.
func (output *driverOutput) sleepUntil(at time.Duration) {
	time.Sleep(time.Until(output.start.Add(at)))
}

// nullable is the value value points to, or JSON null when it is nil.
func nullable[T any](value *T) any {
	if value == nil {
		return nil
	}
	return *value
}

// write stamps and writes a line, to stderr until ready is out.
func (output *driverOutput) write(kind string, build func(t int64) line) {
	output.atomically(func(t int64, emit func(string, line)) {
		emit(kind, build(t))
	})
}

// atomically runs body under the output lock with one t, so a state change
// and the lines that report it are ordered with every other line.
func (output *driverOutput) atomically(body func(t int64, emit func(kind string, object line))) {
	output.lock.Lock()
	defer output.lock.Unlock()
	target := output.stderr
	if output.open {
		target = output.stdout
	}
	t := output.now()
	body(t, func(kind string, object line) { output.print(target, t, kind, object) })
}

// writeIfOpen writes a line once ready is out and drops it before.
func (output *driverOutput) writeIfOpen(kind string, build func(t int64) line) {
	output.lock.Lock()
	defer output.lock.Unlock()
	if output.open {
		t := output.now()
		output.print(output.stdout, t, kind, build(t))
	}
}

// writeNow writes a line to stdout whether or not ready is out.
func (output *driverOutput) writeNow(kind string, build func(t int64) line) {
	output.lock.Lock()
	defer output.lock.Unlock()
	t := output.now()
	output.print(output.stdout, t, kind, build(t))
}

// openWith writes ready as the first stdout line and opens stdout. It
// returns false when ready was already written.
func (output *driverOutput) openWith(build func(t int64) line) bool {
	output.lock.Lock()
	defer output.lock.Unlock()
	if output.open {
		return false
	}
	t := output.now()
	output.print(output.stdout, t, "ready", build(t))
	output.open = true
	return true
}

func (output *driverOutput) print(target io.Writer, t int64, kind string, object line) {
	if object == nil {
		object = line{}
	}
	object["type"] = kind
	object["t"] = t
	output.encoded.Reset()
	encoder := json.NewEncoder(&output.encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(object); err != nil {
		output.encoded.Reset()
		_ = encoder.Encode(line{"type": "error", "t": t, "message": fmt.Sprintf("encode %s: %v", kind, err)})
	}
	_, _ = target.Write(output.encoded.Bytes())
}

// logForwarder passes the client's warnings and errors to the output.
type logForwarder struct {
	output *driverOutput
}

// Printf receives only warnings and errors: the client logger is set to the
// warning level and does not pass the level on.
func (forwarder logForwarder) Printf(format string, values ...any) {
	message := fmt.Sprintf(format, values...)
	forwarder.output.write("log", func(int64) line {
		return line{"level": "WARN", "message": message}
	})
}

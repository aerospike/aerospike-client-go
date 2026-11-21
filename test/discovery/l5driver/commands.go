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
	"strconv"
	"strings"
	"sync"
)

// commandWords is the number of words of each command, the command included.
var commandWords = map[string]int{"quit": 1, "serve": 2, "large": 4, "reset": 1, "watch": 3}

// commandRunner answers the stdin commands.
type commandRunner struct {
	options *driverOptions
	output  *driverOutput
	store   recordStore
	stats   *loadStats
	monitor *routingMonitor

	watchLock sync.Mutex
	watchDone chan struct{}
}

// handle runs one command line. It returns false on quit.
func (runner *commandRunner) handle(commandLine string) bool {
	words := strings.Fields(commandLine)
	if len(words) == 0 {
		runner.replyMalformed(commandLine)
		return true
	}
	count, known := commandWords[words[0]]
	if !known {
		runner.replyError("unknown command: " + commandLine)
		return true
	}
	if len(words) != count {
		runner.replyMalformed(commandLine)
		return true
	}

	switch words[0] {
	case "quit":
		return false
	case "serve":
		reply, err := serve(runner.store, runner.options.namespace, runner.options.set, words[1], runner.output.elapsed)
		if err != nil {
			runner.replyError(err.Error())
			return true
		}
		runner.output.write("served", func(int64) line { return reply })
	case "large":
		step, size, keys, ok := parseLarge(words)
		if !ok {
			runner.replyMalformed(commandLine)
			return true
		}
		reply, err := writeReadLarge(runner.store, runner.options.namespace, runner.options.set, step, size, keys, runner.output.elapsed)
		if err != nil {
			runner.replyError(err.Error())
			return true
		}
		runner.output.write("large", func(int64) line { return reply })
	case "reset":
		runner.output.atomically(func(t int64, emit func(string, line)) {
			runner.stats.reset(runner.output.elapsed())
			runner.monitor.reset(t)
			emit("reset", line{})
		})
	case "watch":
		host, port, err := parseHostPort(words[1])
		if err != nil {
			runner.replyMalformed(commandLine)
			return true
		}
		if !runner.startWatch(words[1], host, port, words[2]) {
			runner.replyError("a watch is already running: " + commandLine)
		}
	}
	return true
}

func (runner *commandRunner) replyError(message string) {
	runner.output.write("error", func(int64) line { return line{"message": message} })
}

func (runner *commandRunner) replyMalformed(commandLine string) {
	runner.replyError("malformed command: " + commandLine)
}

// parseLarge reads large STEP BYTES KEYS. Every key name must fit in BYTES.
func parseLarge(words []string) (step string, size int, keys int, ok bool) {
	size, sizeErr := strconv.Atoi(words[2])
	keys, keysErr := strconv.Atoi(words[3])
	if sizeErr != nil || keysErr != nil || size <= 0 || keys <= 0 {
		return "", 0, 0, false
	}
	step = words[1]
	if len(step+"-"+strconv.Itoa(keys-1)) > size {
		return "", 0, 0, false
	}
	return step, size, keys, true
}

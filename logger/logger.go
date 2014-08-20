// Copyright 2013-2014 Aerospike, Inc.
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

package logger

import (
	"log"
	"os"
	"sync"
)

type LogPriority int

const (
	DEBUG LogPriority = iota - 1
	INFO
	WARNING
	ERR
	OFF LogPriority = 999
)

type logger struct {
	*log.Logger

	level LogPriority
	mutex sync.RWMutex
}

var Logger = newLogger()

func newLogger() *logger {
	return &logger{
		Logger: log.New(os.Stdout, "", log.LstdFlags),
		level:  OFF,
	}
}

// Specify the *log.Logger object where log messages should be sent to.
func (lgr *logger) SetLogger(l *log.Logger) {
	lgr.mutex.Lock()
	defer lgr.mutex.Unlock()

	lgr.Logger = l
}

// Sets logging level. Default is ERR
func (lgr *logger) SetLevel(level LogPriority) {
	lgr.mutex.Lock()
	defer lgr.mutex.Unlock()

	lgr.level = level
}

func (lgr *logger) Debug(format string, v ...interface{}) {
	if lgr.level <= DEBUG {
		lgr.Logger.Printf(format, v...)
	}
}

func (lgr *logger) Info(format string, v ...interface{}) {
	if lgr.level <= INFO {
		lgr.Logger.Printf(format, v...)
	}
}

func (lgr *logger) Warn(format string, v ...interface{}) {
	if lgr.level <= WARNING {
		lgr.Logger.Printf(format, v...)
	}
}

func (lgr *logger) Error(format string, v ...interface{}) {
	if lgr.level <= ERR {
		lgr.Logger.Printf(format, v...)
	}
}

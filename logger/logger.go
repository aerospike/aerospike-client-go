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
	"log/syslog"
	"os"
)

const (
	DEBUG = syslog.LOG_DEBUG
	INFO  = syslog.LOG_INFO
	WARN  = syslog.LOG_WARNING
	ERR   = syslog.LOG_ERR
)

type logger struct {
	*log.Logger

	level syslog.Priority
}

var Logger = newLogger()

func newLogger() *logger {
	return &logger{
		Logger: log.New(os.Stdout, "", log.LstdFlags),
		level:  syslog.LOG_ERR,
	}
}

// Specify the *log.Logger object where log messages should be sent to.
func (this *logger) SetLogger(l *log.Logger) {
	this.Logger = l
}

// Sets logging level. Default is ERR
func (this *logger) SetLevel(level syslog.Priority) {
	this.level = level
}

func (this *logger) Debug(format string, v ...interface{}) {
	if this.level >= syslog.LOG_DEBUG {
		this.Logger.Printf(format, v...)
	}
}

func (this *logger) Info(format string, v ...interface{}) {
	if this.level >= syslog.LOG_INFO {
		this.Logger.Printf(format, v...)
	}
}

func (this *logger) Warn(format string, v ...interface{}) {
	if this.level >= syslog.LOG_WARNING {
		this.Logger.Printf(format, v...)
	}
}

func (this *logger) Error(format string, v ...interface{}) {
	if this.level >= syslog.LOG_ERR {
		this.Logger.Printf(format, v...)
	}
}

// Copyright 2014-2022 Aerospike, Inc.
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

package atomic

import (
	"sync"
)

// Bool implements a synchronized boolean value
type Bool struct {
	m   sync.Mutex
	val bool
}

// NewBool generates a new Boolean instance.
func NewBool(value bool) *Bool {
	return &Bool{
		val: value,
	}
}

// String implements the Stringer interface
func (ab *Bool) String() string {
	res := ab.Get()
	if res {
		return "true"
	}
	return "false"
}

// GomegaString implements the GomegaStringer interface
// to prevent race conditions in tests
func (ab *Bool) GomegaString() string {
	return ab.String()
}

// Get atomically retrieves the boolean value.
func (ab *Bool) Get() bool {
	ab.m.Lock()
	res := ab.val
	ab.m.Unlock()
	return res
}

// Set atomically sets the boolean value.
func (ab *Bool) Set(newVal bool) {
	ab.m.Lock()
	ab.val = newVal
	ab.m.Unlock()
}

// Or atomically applies OR operation to the boolean value.
func (ab *Bool) Or(newVal bool) bool {
	if !newVal {
		return ab.Get()
	}
	ab.Set(newVal)
	return true
}

// CompareAndToggle atomically sets the boolean value if the current value is equal to updated value.
func (ab *Bool) CompareAndToggle(expect bool) bool {
	res := false
	ab.m.Lock()
	if ab.val == expect {
		res = true
		ab.val = !ab.val
	}
	ab.m.Unlock()
	return res
}

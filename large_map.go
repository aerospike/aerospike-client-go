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

package aerospike

// Create and manage a map within a single bin.
type LargeMap struct {
	baseLargeObject
}

// Initialize large map operator.
//
// client        client
// policy        generic configuration parameters, pass in nil for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass nil for default set
func NewLargeMap(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeMap {
	return &LargeMap{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (lm *LargeMap) packageName() string {
	return "lmap"
}

// Add entry to map.  If the map does not exist, create it using specified userModule configuration.
//
// name        entry key
// value       entry value
func (lm *LargeMap) Put(name interface{}, value interface{}) error {
	_, err := lm.client.Execute(lm.policy, lm.key, lm.packageName(), "put", lm.binName, NewValue(name), NewValue(value), lm.userModule)
	return err
}

// Add map values to map.  If the map does not exist, create it using specified userModule configuration.
//
// map       map values to push
func (lm *LargeMap) PutMap(theMap map[interface{}]interface{}) error {
	_, err := lm.client.Execute(lm.policy, lm.key, lm.packageName(), "put_all", lm.binName, NewMapValue(theMap), lm.userModule)
	return err
}

// Get value from map given name key.
//
// name        key.
// return          map of items selected
func (lm *LargeMap) Get(name interface{}) (map[interface{}]interface{}, error) {
	res, err := lm.client.Execute(lm.policy, lm.key, lm.packageName(), "get", lm.binName, NewValue(name))

	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.(map[interface{}]interface{}), err
}

// Return all objects in the list.
func (lm *LargeMap) Scan() (map[interface{}]interface{}, error) {
	res, err := lm.client.Execute(lm.policy, lm.key, lm.packageName(), "scan", lm.binName)
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.(map[interface{}]interface{}), err
}

// Select items from map.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// return          list of items selected
func (lm *LargeMap) Filter(filterName string, filterArgs ...interface{}) (map[interface{}]interface{}, error) {
	res, err := lm.client.Execute(lm.policy, lm.key, lm.packageName(), "filter", lm.binName, lm.userModule, NewStringValue(filterName), ToValueArray(filterArgs))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.(map[interface{}]interface{}), err
}

// Delete bin containing the list.
func (lm *LargeMap) Destroy() error {
	return lm.destroy(lm)
}

// Return size of list.
func (lm *LargeMap) Size() (int, error) {
	return lm.size(lm)
}

// Return map of list configuration parameters.
func (lm *LargeMap) GetConfig() (map[interface{}]interface{}, error) {
	return lm.getConfig(lm)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (lm *LargeMap) SetCapacity(capacity int) error {
	return lm.setCapacity(lm, capacity)
}

// Return maximum number of entries in the list.
func (lm *LargeMap) GetCapacity() (int, error) {
	return lm.getCapacity(lm)
}

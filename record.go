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

import (
	"fmt"
)

// Record is a container object for records.  Records are equivalent to rows.
type Record struct {
	// Record's Key. Might be empty, or only consist of digest only.
	Key *Key

	// Node from which the Record is originating from.
	Node *Node

	// Map of requested name/value bins.
	Bins BinMap

	// List of all duplicate records (if any) for a given key.  Duplicates are only created when
	// the server configuration option "allow-versions" is true (default is false) and client
	// RecordExistsAction.DUPLICATE policy flag is set and there is a generation error.
	// Almost always nil.
	Duplicates []BinMap

	// Record modification count.
	Generation int

	// Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
	Expiration int
}

// newRecord creates a new record with the given parameter, and ensures Bins is not nil afterwards. 
func newRecord(node *Node, key *Key, bins BinMap, duplicates []BinMap, generation int, expiration int) *Record {
	r := &Record{
		Node:       node,
		Key:        key,
		Bins:       bins,
		Duplicates: duplicates,
		Generation: generation,
		Expiration: expiration,
	}

	// always assign a map of length zero if Bins is nil
	if r.Bins == nil {
		r.Bins = make(BinMap, 0)
	}

	return r
}

// String returns the string representation of record.
func (rc *Record) String() string {
	return fmt.Sprintf("%v %v", *rc.Key, rc.Bins)
}

// GetString returns the string-value for the given binName
func (rc *Record) GetString(binName string) (string, error) {
	if bin, ok := rc.Bins[binName]; ok {
		if bin_str, ok := bin.(string); ok {
			return bin_str, nil
		} else {
			return "", fmt.Errorf("Bin %s did not contain a string value", binName)
		}
	} else {
		return "", fmt.Errorf("No bin found with name %s", binName)
	}
}

// GetStringArray returns the []string-value for the given binName. Any non-string values in the array are ignored. 
func (rc *Record) GetStringArray(binName string) ([]string, error) {
	if bin, ok := rc.Bins[binName]; ok {
		if bin_arr, ok := bin.([]interface{}); ok {
			list := []string{}
			for _, val_interface := range bin_arr {
				if bin_str, ok := val_interface.(string); ok {
					list = append(list, bin_str)
				}
			}
			return list, nil
		} else {
			return []string{}, fmt.Errorf("Bin %s did not contain an array", binName)
		}
	} else {
		return []string{}, fmt.Errorf("No bin found with name %s", binName)
	}
}

// GetInt returns the string-value for the given binName
func (rc *Record) GetInt(binName string) (int, error) {
	if bin, ok := rc.Bins[binName]; ok {
		if bin_int, ok := bin.(int); ok {
			return bin_int, nil
		} else {
			return 0, fmt.Errorf("Bin %s did not contain an integer value", binName)
		}
	} else {
		return 0, fmt.Errorf("No bin found with name %s", binName)
	}
}

// GetIntArray returns the []int-value for the given binName. Any non-int values in the array are ignored. 
func (rc *Record) GetIntArray(binName string) ([]int, error) {
	if bin, ok := rc.Bins[binName]; ok {
		if bin_arr, ok := bin.([]interface{}); ok {
			list := []int{}
			for _, val_interface := range bin_arr {
				if bin_int, ok := val_interface.(int); ok {
					list = append(list, bin_int)
				}
			}
			return list, nil
		} else {
			return []int{}, fmt.Errorf("Bin %s did not contain an array", binName)
		}
	} else {
		return []int{}, fmt.Errorf("No bin found with name %s", binName)
	}
}

// GetInterface returns the interface{}-value for the given binName. Much like calling Bins directly
func (rc *Record) GetInterface(binName string) (interface{}, error) {
	if bin, ok := rc.Bins[binName]; ok {
		return bin, nil
	} else {
		return "", fmt.Errorf("No bin found with name %s", binName)
	}
}

// GetInterfaceArray returns the []interface{}-value for the given binName.  
func (rc *Record) GetInterfaceArray(binName string) ([]interface{}, error) {
	if bin, ok := rc.Bins[binName]; ok {
		if bin_arr, ok := bin.([]interface{}); ok {
			return bin_arr, nil
		} else {
			return []interface{}{}, fmt.Errorf("Bin %s did not contain an array", binName)
		}
	} else {
		return []interface{}{}, fmt.Errorf("No bin found with name %s", binName)
	}
}

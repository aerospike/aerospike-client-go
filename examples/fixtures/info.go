/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Fixture factories for the server information examples.

package fixtures

import (
	"errors"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func Info() Fixture {
	return Fixture{
		// The example prints the server's info map. Verify independently
		// that the node answers info commands and reports a build version.
		Validate: func() error {
			infoMap, err := client.GetNodes()[0].RequestInfo(as.NewInfoPolicy(), "build")
			if err != nil {
				return err
			}
			if infoMap["build"] == "" {
				return errors.New("server returned no build version")
			}
			return nil
		},
	}
}

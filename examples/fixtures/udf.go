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

// Fixture factories for the UDF examples.

package fixtures

func UDF() Fixture {
	keys := numberedKeys("udfkey", 6)
	return Fixture{
		Setup: func() error { return DeleteKeys(keys...) },
		Validate: func() error {
			// writeBin via UDF.
			if err := AssertBin("udfkey1", "udfbin1", "string value"); err != nil {
				return err
			}
			// writeIfGenerationNotChanged succeeded with a matching generation.
			if err := AssertBin("udfkey2", "udfbin2", "string value"); err != nil {
				return err
			}
			// writeUnique kept the first value.
			if err := AssertBin("udfkey3", "udfbin3", "first"); err != nil {
				return err
			}
			// writeWithValidation kept the valid value and rejected the invalid one.
			return AssertBin("udfkey4", "udfbin4", 4)
		},
		Cleanup: func() error {
			if err := DeleteKeys(keys...); err != nil {
				return err
			}
			if task, err := client.RemoveUDF(nil, "record_example.lua"); err == nil {
				<-task.OnComplete()
			}
			return nil
		},
	}
}

/*
 * Copyright 2026 Aerospike, Inc.
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

// Fixture factories for string operation examples.

package fixtures

func OperateStringRead() Fixture {
	const key = "opstr_read"
	return Fixture{
		Setup:   func() error { return DeleteKeys(key) },
		Validate: func() error { return AssertBin(key, "text", "HELLO") },
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

func OperateStringModify() Fixture {
	const key = "opstr_modify"
	return Fixture{
		Setup:   func() error { return DeleteKeys(key) },
		Validate: func() error { return AssertBin(key, "text", "abcNUMdefNUM") },
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

func OperateStringToString() Fixture {
	const key = "opstr_tostring"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		Validate: func() error {
			// StrToStringOp returns the converted value in the operate response;
			// the stored bin remains the original particle type.
			return AssertBin(key, "n", 42)
		},
		Cleanup: func() error { return DeleteKeys(key) },
	}
}

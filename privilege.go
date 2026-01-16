// Copyright 2014-2022 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

import (
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

type privilegeCode string

// Privilege determines user access granularity.
type Privilege struct {
	// Role
	Code privilegeCode

	// Namespace determines namespace scope. Apply permission to this namespace only.
	// If namespace is zero value, the privilege applies to all namespaces.
	Namespace string

	// Set name scope. Apply permission to this set within namespace only.
	// If set is zero value, the privilege applies to all sets within namespace.
	SetName string
}

func (p *Privilege) code() int {
	switch p.Code {
	// User can edit/remove other users.  Global scope only.
	case UserAdmin:
		return 0

	// User can perform systems administration functions on a database that do not involve user
	// administration.  Examples include server configuration.
	// Global scope only.
	case SysAdmin:
		return 1

	// User can perform UDF and SINDEX administration actions. Global scope only.
	case DataAdmin:
		return 2

	// User can perform user defined function(UDF) administration actions.
	// Examples include create/drop UDF. Global scope only.
	// Requires server version 6+
	case UDFAdmin:
		return 3

	// User can perform secondary index administration actions.
	// Examples include create/drop index. Global scope only.
	// Requires server version 6+
	case SIndexAdmin:
		return 4

	// User can read data only.
	case Read:
		return 10

	// User can read and write data.
	case ReadWrite:
		return 11

	// User can read and write data through user defined functions.
	case ReadWriteUDF:
		return 12

	// User can read and write data through user defined functions.
	case Write:
		return 13

	// User can truncate data only.
	// Requires server version 6+
	case Truncate:
		return 14

	// User can manage masking policies. Requires server version >= 8.1.1.
	case MaskingAdmin:
		return 15

	// User can read data with masking policies applied. Requires server version >= 8.1.1.
	case ReadMasked:
		return 16

	// User can write data with masking policies applied. Requires server version >= 8.1.1.
	case WriteMasked:
		return 17

	// Unknown privilege code from server (forward compatibility).
	case Unknown:
		return -1

	default:
		// This should never happen if privilegeFrom() is used correctly
		logger.Logger.Warn("Invalid privilege code in Privilege.code(): %s", p.Code)
		return -1
	}
}

func privilegeFrom(code uint8) privilegeCode {
	switch code {
	// User can edit/remove other users.  Global scope only.
	case 0:
		return UserAdmin

	// User can perform systems administration functions on a database that do not involve user
	// administration.  Examples include server configuration.
	// Global scope only.
	case 1:
		return SysAdmin

	// User can perform data administration functions on a database that do not involve user
	// administration.  Examples include index and user defined function management.
	// Global scope only.
	case 2:
		return DataAdmin

	// User can perform user defined function(UDF) administration actions.
	// Examples include create/drop UDF. Global scope only.
	// Requires server version 6+
	case 3:
		return UDFAdmin

	// User can perform secondary index administration actions.
	// Examples include create/drop index. Global scope only.
	// Requires server version 6+
	case 4:
		return SIndexAdmin

	// User can read data.
	case 10:
		return Read

	// User can read and write data.
	case 11:
		return ReadWrite

	// User can read and write data through user defined functions.
	case 12:
		return ReadWriteUDF

	// User can only write data.
	case 13:
		return Write

	// User can truncate data only.
	// Requires server version 6+
	case 14:
		return Truncate

	// User can manage masking policies. Requires server version >= 8.1.1.
	case 15:
		return MaskingAdmin

	// User can read data with masking policies applied. Requires server version >= 8.1.1.
	case 16:
		return ReadMasked

	// User can write data with masking policies applied. Requires server version >= 8.1.1.
	case 17:
		return WriteMasked

	default:
		// Return Unknown for forward compatibility with new privilege codes
		// from future server versions. This allows the client to work with
		// newer servers without breaking when new privileges are added.
		logger.Logger.Warn("Unknown privilege code received from server: %d. Using Unknown.", code)
		return Unknown
	}
}

func (p *Privilege) canScope() bool {
	// Unknown privileges (code = -1) cannot be scoped since we don't know their characteristics.
	// MaskingAdmin (code = 15) is global only.
	// Data privileges (code >= 10) can be scoped except MaskingAdmin.
	return p.code() >= 10 && p.code() != 15
}

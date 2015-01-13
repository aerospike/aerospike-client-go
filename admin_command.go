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
	"time"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
	"github.com/jameskeane/bcrypt"
)

const (
	// Commands
	_AUTHENTICATE    byte = 0
	_CREATE_USER     byte = 1
	_DROP_USER       byte = 2
	_SET_PASSWORD    byte = 3
	_CHANGE_PASSWORD byte = 4
	_GRANT_ROLES     byte = 5
	_REVOKE_ROLES    byte = 6
	_REPLACE_ROLES   byte = 7
	//_CREATE_ROLE byte = 8;
	_QUERY_USERS byte = 9
	//_QUERY_ROLES byte =  10;

	// Field IDs
	_USER         byte = 0
	_PASSWORD     byte = 1
	_OLD_PASSWORD byte = 2
	_CREDENTIAL   byte = 3
	_ROLES        byte = 10
	//_PRIVILEGES byte =  11;

	// Misc
	_MSG_VERSION int64 = 0
	_MSG_TYPE    int64 = 2

	_HEADER_SIZE      int = 24
	_HEADER_REMAINING int = 16
	_RESULT_CODE      int = 9
	_QUERY_END        int = 50
)

type AdminCommand struct {
	dataBuffer []byte
	dataOffset int
}

func newAdminCommand() *AdminCommand {
	return &AdminCommand{
		dataBuffer: bufPool.Get(),
		dataOffset: 8,
	}
}

func (this *AdminCommand) authenticate(conn *Connection, user string, password []byte) error {

	this.setAuthenticate(user, password)
	if _, err := conn.Write(this.dataBuffer[:this.dataOffset]); err != nil {
		return err
	}

	if _, err := conn.Read(this.dataBuffer, _HEADER_SIZE); err != nil {
		return err
	}

	result := this.dataBuffer[_RESULT_CODE]
	if result != 0 {
		return NewAerospikeError(ResultCode(result), "Authentication failed")
	}

	bufPool.Put(this.dataBuffer)

	return nil
}

func (this *AdminCommand) setAuthenticate(user string, password []byte) int {
	this.writeHeader(_AUTHENTICATE, 2)
	this.writeFieldStr(_USER, user)
	this.writeFieldBytes(_CREDENTIAL, password)
	this.writeSize()

	return this.dataOffset
}

func (this *AdminCommand) createUser(cluster *Cluster, policy *AdminPolicy, user string, password []byte, roles []string) error {
	this.writeHeader(_CREATE_USER, 3)
	this.writeFieldStr(_USER, user)
	this.writeFieldBytes(_PASSWORD, password)
	this.writeRoles(roles)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) dropUser(cluster *Cluster, policy *AdminPolicy, user string) error {
	this.writeHeader(_DROP_USER, 1)
	this.writeFieldStr(_USER, user)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) setPassword(cluster *Cluster, policy *AdminPolicy, user string, password []byte) error {
	this.writeHeader(_SET_PASSWORD, 2)
	this.writeFieldStr(_USER, user)
	this.writeFieldBytes(_PASSWORD, password)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) changePassword(cluster *Cluster, policy *AdminPolicy, user string, password []byte) error {
	this.writeHeader(_CHANGE_PASSWORD, 3)
	this.writeFieldStr(_USER, user)
	this.writeFieldBytes(_OLD_PASSWORD, cluster.password)
	this.writeFieldBytes(_PASSWORD, password)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) grantRoles(cluster *Cluster, policy *AdminPolicy, user string, roles []string) error {
	this.writeHeader(_GRANT_ROLES, 2)
	this.writeFieldStr(_USER, user)
	this.writeRoles(roles)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) revokeRoles(cluster *Cluster, policy *AdminPolicy, user string, roles []string) error {
	this.writeHeader(_REVOKE_ROLES, 2)
	this.writeFieldStr(_USER, user)
	this.writeRoles(roles)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) replaceRoles(cluster *Cluster, policy *AdminPolicy, user string, roles []string) error {
	this.writeHeader(_REPLACE_ROLES, 2)
	this.writeFieldStr(_USER, user)
	this.writeRoles(roles)
	return this.executeCommand(cluster, policy)
}

func (this *AdminCommand) queryUser(cluster *Cluster, policy *AdminPolicy, user string) (*UserRoles, error) {
	// TODO: Remove the workaround in the future
	time.Sleep(time.Millisecond * 10)
	defer bufPool.Put(this.dataBuffer)

	this.writeHeader(_QUERY_USERS, 1)
	this.writeFieldStr(_USER, user)
	list, err := this.readUsers(cluster, policy)
	if err != nil {
		return nil, err
	}

	if len(list) > 0 {
		return list[0], nil
	}

	return nil, nil
}

func (this *AdminCommand) queryUsers(cluster *Cluster, policy *AdminPolicy) ([]*UserRoles, error) {
	// TODO: Remove the workaround in the future
	time.Sleep(time.Millisecond * 10)
	defer bufPool.Put(this.dataBuffer)

	this.writeHeader(_QUERY_USERS, 0)
	list, err := this.readUsers(cluster, policy)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (this *AdminCommand) writeRoles(roles []string) {
	offset := this.dataOffset + int(_FIELD_HEADER_SIZE)
	this.dataBuffer[offset] = byte(len(roles))
	offset++

	for _, role := range roles {
		len := copy(this.dataBuffer[offset+1:], role)
		this.dataBuffer[offset] = byte(len)
		offset += len + 1
	}

	size := offset - this.dataOffset - int(_FIELD_HEADER_SIZE)
	this.writeFieldHeader(_ROLES, size)
	this.dataOffset = offset
}

func (this *AdminCommand) writeSize() {
	// Write total size of message which is the current offset.
	var size = int64(this.dataOffset-8) | (_MSG_VERSION << 56) | (_MSG_TYPE << 48)
	Buffer.Int64ToBytes(size, this.dataBuffer, 0)
}

func (this *AdminCommand) writeHeader(command byte, fieldCount int) {
	// Authenticate header is almost all zeros
	for i := this.dataOffset; i < this.dataOffset+16; i++ {
		this.dataBuffer[i] = 0
	}
	this.dataBuffer[this.dataOffset+2] = command
	this.dataBuffer[this.dataOffset+3] = byte(fieldCount)
	this.dataOffset += 16
}

func (this *AdminCommand) writeFieldStr(id byte, str string) {
	len := copy(this.dataBuffer[this.dataOffset+int(_FIELD_HEADER_SIZE):], str)
	this.writeFieldHeader(id, len)
	this.dataOffset += len
}

func (this *AdminCommand) writeFieldBytes(id byte, bytes []byte) {
	copy(this.dataBuffer[this.dataOffset+int(_FIELD_HEADER_SIZE):], bytes)
	this.writeFieldHeader(id, len(bytes))
	this.dataOffset += len(bytes)
}

func (this *AdminCommand) writeFieldHeader(id byte, size int) {
	Buffer.Int32ToBytes(int32(size+1), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = id
	this.dataOffset++
}

func (this *AdminCommand) executeCommand(cluster *Cluster, policy *AdminPolicy) error {
	// TODO: Remove the workaround in the future
	defer time.Sleep(time.Millisecond * 10)

	defer bufPool.Put(this.dataBuffer)

	this.writeSize()
	node, err := cluster.GetRandomNode()
	if err != nil {
		return nil
	}
	timeout := 1 * time.Second
	if policy != nil && policy.Timeout > 0 {
		timeout = policy.Timeout
	}

	conn, err := node.GetConnection(timeout)
	if err != nil {
		return err
	}

	if _, err := conn.Write(this.dataBuffer[:this.dataOffset]); err != nil {
		conn.Close()
		return err
	}

	if _, err := conn.Read(this.dataBuffer, _HEADER_SIZE); err != nil {
		conn.Close()
		return err
	}

	node.PutConnection(conn)

	result := this.dataBuffer[_RESULT_CODE]
	if result != 0 {
		return NewAerospikeError(ResultCode(result))
	}

	return nil
}

func (this *AdminCommand) readUsers(cluster *Cluster, policy *AdminPolicy) ([]*UserRoles, error) {
	this.writeSize()
	node, err := cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}
	timeout := 1 * time.Second
	if policy != nil && policy.Timeout > 0 {
		timeout = policy.Timeout
	}

	conn, err := node.GetConnection(timeout)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write(this.dataBuffer[:this.dataOffset]); err != nil {
		conn.Close()
		return nil, err
	}

	status, list, err := this.readUserBlocks(conn)
	if err != nil {
		return nil, err
	}
	node.PutConnection(conn)

	if status > 0 {
		return nil, NewAerospikeError(ResultCode(status))
	}
	return list, nil
}

func (this *AdminCommand) readUserBlocks(conn *Connection) (status int, rlist []*UserRoles, err error) {

	var list []*UserRoles

	for status == 0 {
		if _, err = conn.Read(this.dataBuffer, 8); err != nil {
			return -1, nil, err
		}

		size := Buffer.BytesToInt64(this.dataBuffer, 0)
		receiveSize := (size & 0xFFFFFFFFFFFF)

		if receiveSize > 0 {
			if receiveSize > int64(len(this.dataBuffer)) {
				this.dataBuffer = make([]byte, receiveSize)
			}
			if _, err = conn.Read(this.dataBuffer, int(receiveSize)); err != nil {
				return -1, nil, err
			}
			status, list, err = this.parseUsers(int(receiveSize))
			if err != nil {
				return -1, nil, err
			}
			rlist = append(rlist, list...)
		} else {
			break
		}
	}
	return status, rlist, nil
}

func (this *AdminCommand) parseUsers(receiveSize int) (int, []*UserRoles, error) {
	this.dataOffset = 0
	list := make([]*UserRoles, 0, 100)

	for this.dataOffset < receiveSize {
		resultCode := int(this.dataBuffer[this.dataOffset+1])

		if resultCode != 0 {
			if resultCode == _QUERY_END {
				return -1, nil, nil
			}
			return resultCode, nil, nil
		}

		userRoles := &UserRoles{}
		fieldCount := int(this.dataBuffer[this.dataOffset+3])
		this.dataOffset += _HEADER_REMAINING

		for i := 0; i < fieldCount; i++ {
			len := int(Buffer.BytesToInt32(this.dataBuffer, this.dataOffset))
			this.dataOffset += 4
			id := this.dataBuffer[this.dataOffset]
			this.dataOffset++
			len--

			if id == _USER {
				userRoles.User = string(this.dataBuffer[this.dataOffset : this.dataOffset+len])
				this.dataOffset += len
			} else if id == _ROLES {
				this.parseRoles(userRoles)
			} else {
				this.dataOffset += len
			}
		}

		if userRoles.User == "" && userRoles.Roles == nil {
			continue
		}

		if userRoles.Roles == nil {
			userRoles.Roles = make([]string, 0)
		}
		list = append(list, userRoles)
	}

	return 0, list, nil
}

func (this *AdminCommand) parseRoles(userRoles *UserRoles) {
	size := int(this.dataBuffer[this.dataOffset])
	this.dataOffset++
	userRoles.Roles = make([]string, 0, size)

	for i := 0; i < size; i++ {
		len := int(this.dataBuffer[this.dataOffset])
		this.dataOffset++
		role := string(this.dataBuffer[this.dataOffset : this.dataOffset+len])
		this.dataOffset += len
		userRoles.Roles = append(userRoles.Roles, role)
	}
}

func hashPassword(password string) ([]byte, error) {
	// Hashing the password with the cost of 10, with a static salt
	const salt = "$2a$10$7EqJtq98hPqEX7fNZaFWoO"
	hashedPassword, err := bcrypt.Hash(password, salt)
	if err != nil {
		return nil, err
	}
	return []byte(hashedPassword), nil
}

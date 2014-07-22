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
	"bytes"
	"encoding/binary"
	"strings"

	. "github.com/citrusleaf/go-client/logger"
	. "github.com/citrusleaf/go-client/types"
)

const (
	_DEFAULT_TIMEOUT = 2000
)

// Access server's info monitoring protocol.
type Info struct {
	msg *Message
}

// Get many info values by name from the specified database server node,
// using host name and port.
func RequestInfoForHostName(hostname string, port int, names ...string) map[string]string {
	return nil
}

// Get info values by name from the specified database server node.
func RequestInfoForNode(node Node, name ...string) (string, error) {
	return "", nil
}

// Send multiple commands to server and store results.
func NewInfo(conn *Connection, commands ...string) (*Info, error) {
	commandStr := strings.Trim(strings.Join(commands, "\n"), " ")
	if commandStr != "" {
		commandStr += "\n"
	}
	newInfo := &Info{
		msg: NewMessage(MSG_INFO, []byte(commandStr)),
	}

	if err := newInfo.sendCommand(conn); err != nil {
		return nil, err
	}
	return newInfo, nil
}

// Get info values by name from the specified connection
func RequestInfo(conn *Connection, names ...string) (map[string]string, error) {
	if info, err := NewInfo(conn, names...); err != nil {
		return nil, err
	} else {
		return info.parseMultiResponse()
	}
}

// Parse response in name/value pair format:
// <command>\t<name1>=<value1>;<name2>=<value2>;...
func (this *Info) parseNameValues() map[string]string {
	return nil
}

// Return single value from response buffer.
func (this *Info) GetValue() string {
	return ""
}

// Issue request and set results buffer. This method is used internally.
// The static request methods should be used instead.
func (this *Info) sendCommand(conn *Connection) error {
	// Write.
	if _, err := conn.Write(this.msg.Serialize()); err != nil {
		Logger.Debug("Failed to send command.")
		return err
	}

	// Read - reuse input buffer.
	header := bytes.NewBuffer(make([]byte, MSG_HEADER_SIZE))
	conn.Read(header.Bytes(), MSG_HEADER_SIZE)
	if err := binary.Read(header, binary.BigEndian, &this.msg.MessageHeader); err != nil {
		Logger.Debug("Failed to read command response.")
		return err
	}

	// Logger.Debug("Header Response: %v %v %v %v", t.Type, t.Version, t.Length(), t.DataLen)
	this.msg.Resize(this.msg.Length())
	_, err := conn.Read(this.msg.Data, len(this.msg.Data))
	Logger.Debug("Data Len: %v", this.msg.Length())
	return err
}

func (this *Info) parseSingleResponse(name string) (string, error) {
	return "-", nil
}

func (this *Info) parseMultiResponse() (map[string]string, error) {
	responses := make(map[string]string)
	offset := int64(0)
	begin := int64(0)

	dataLen := int64(len(this.msg.Data))

	// Create reusable StringBuilder for performance.
	for offset < dataLen {
		b := this.msg.Data[offset]

		if b == '\t' {
			name := this.msg.Data[begin:offset]
			offset++
			begin = offset

			// Parse field value.
			for offset < dataLen {
				if this.msg.Data[offset] == '\n' {
					break
				}
				offset++
			}

			if offset > begin {
				value := this.msg.Data[begin:offset]
				responses[string(name)] = string(value)
				// } else {
				// 	responses[string(name)] = nil
			}
			offset++
			begin = offset
		} else if b == '\n' {
			if offset > begin {
				// name := this.msg.Data[begin : offset-begin]
				// responses[string(name)] = nil
			}
			offset++
			begin = offset
		} else {
			offset++
		}
	}

	if offset > begin {
		// name := this.msg.Data[begin : offset-begin]
		// responses[string(name)] = nil
	}
	Logger.Debug("%v", responses)
	return responses, nil
}

// Copyright 2014-2026 Aerospike, Inc.
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

package aws

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

type describeInstancesResponse struct {
	NextToken    string        `xml:"nextToken"`
	Reservations []reservation `xml:"reservationSet>item"`
}

type reservation struct {
	Instances []instance `xml:"instancesSet>item"`
}

type instance struct {
	PrivateIP  string `xml:"privateIpAddress"`
	PublicIP   string `xml:"ipAddress"`
	PrivateDNS string `xml:"privateDnsName"`
	PublicDNS  string `xml:"dnsName"`
}

func parseDescribeInstances(body []byte) (describeInstancesResponse, error) {
	if err := rejectDOCTYPE(body); err != nil {
		return describeInstancesResponse{}, err
	}
	var doc describeInstancesResponse
	if err := xml.Unmarshal(body, &doc); err != nil {
		return describeInstancesResponse{}, err
	}
	return doc, nil
}

func apiError(action string, status int, body []byte, headerRequestID string, secrets ...string) error {
	fields := xmlTexts(body, "Code", "Message", "RequestID", "RequestId")
	code := fields["Code"]
	message := fields["Message"]
	requestID := fields["RequestID"]
	if requestID == "" {
		requestID = fields["RequestId"]
	}
	if requestID == "" {
		requestID = headerRequestID
	}

	msg := fmt.Sprintf("AWS EC2 seed provider: %s failed: HTTP %d", action, status)
	if code != "" {
		msg += " " + code
	}
	if message != "" {
		msg += ": " + message
	}
	if requestID != "" {
		msg += " (request id " + requestID + ")"
	}
	return fmt.Errorf("%s", redact(msg, secrets...))
}

func rejectDOCTYPE(body []byte) error {
	upper := bytes.ToUpper(body)
	if bytes.Contains(upper, []byte("<!DOCTYPE")) {
		return fmt.Errorf("DOCTYPE is not allowed")
	}
	return nil
}

func xmlTexts(body []byte, names ...string) map[string]string {
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	found := make(map[string]string, len(names))
	if err := rejectDOCTYPE(body); err != nil {
		return found
	}

	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	var stack []string
	for {
		token, err := decoder.Token()
		if err != nil {
			return found
		}
		switch t := token.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) == 0 {
				continue
			}
			name := stack[len(stack)-1]
			if _, ok := wanted[name]; !ok {
				continue
			}
			if _, exists := found[name]; exists {
				continue
			}
			found[name] = strings.TrimSpace(string(t))
		}
	}
}

func redact(text string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "<redacted>")
		}
	}
	return text
}

func (inst instance) addressFor(field AddressField) string {
	switch field {
	case AddressFieldPublicIP:
		return inst.PublicIP
	case AddressFieldPrivateDNS:
		return inst.PrivateDNS
	case AddressFieldPublicDNS:
		return inst.PublicDNS
	default:
		return inst.PrivateIP
	}
}

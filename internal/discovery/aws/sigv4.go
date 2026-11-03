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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

const algorithm = "AWS4-HMAC-SHA256"

const upperHexDigits = "0123456789ABCDEF"

func amzDate(millis int64) string {
	return time.UnixMilli(millis).UTC().Format("20060102T150405Z")
}

func authorization(
	accessKeyID, secretAccessKey, method, path, query string,
	headers map[string]string,
	payload []byte,
	region, service, date string,
) string {
	credScope := credentialScope(date[:8], region, service)
	request := canonicalRequest(method, path, query, headers, hex.EncodeToString(sha256Sum(payload)))
	sig := requestSignature(signingKey(secretAccessKey, date[:8], region, service), stringToSign(date, credScope, request))
	return algorithm + " Credential=" + accessKeyID + "/" + credScope + ", SignedHeaders=" + signedHeaders(headers) +
		", Signature=" + sig
}

func canonicalRequest(method, path, query string, headers map[string]string, payloadHash string) string {
	var b strings.Builder
	b.Grow(256)
	b.WriteString(method)
	b.WriteByte('\n')
	b.WriteString(path)
	b.WriteByte('\n')
	b.WriteString(query)
	b.WriteByte('\n')
	for _, name := range sortedHeaderNames(headers) {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.TrimSpace(headers[name]))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(signedHeaders(headers))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

func signedHeaders(headers map[string]string) string {
	return strings.Join(sortedHeaderNames(headers), ";")
}

func credentialScope(date, region, service string) string {
	return date + "/" + region + "/" + service + "/aws4_request"
}

func stringToSign(date, credScope, canonical string) string {
	return algorithm + "\n" + date + "\n" + credScope + "\n" + hex.EncodeToString(sha256Sum([]byte(canonical)))
}

func signingKey(secretAccessKey, date, region, service string) []byte {
	key := hmacSHA256([]byte("AWS4"+secretAccessKey), date)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	return hmacSHA256(key, "aws4_request")
}

func requestSignature(key []byte, toSign string) string {
	return hex.EncodeToString(hmacSHA256(key, toSign))
}

func encode(value string) string {
	raw := []byte(value)
	var b strings.Builder
	b.Grow(len(raw) + 16)
	for _, c := range raw {
		if unreserved(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(upperHexDigits[c>>4])
		b.WriteByte(upperHexDigits[c&0xf])
	}
	return b.String()
}

func sha256Sum(bytes []byte) []byte {
	sum := sha256.Sum256(bytes)
	return sum[:]
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func sortedHeaderNames(headers map[string]string) []string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func unreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~'
}

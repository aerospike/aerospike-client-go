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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxHTTPBody = 16 * 1024 * 1024

var errHTTPTimeout = errors.New("timed out")

var httpClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives:   true,
		ForceAttemptHTTP2:   false,
		TLSHandshakeTimeout: 0,
		IdleConnTimeout:     0,
	},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// Response is one HTTP response.
type Response struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

func (response Response) text() string {
	return string(response.Body)
}

func hostHeader(uri *url.URL) string {
	if uri.Port() == "" {
		return uri.Hostname()
	}
	return net.JoinHostPort(uri.Hostname(), uri.Port())
}

func request(ctx context.Context, method, rawURL string, headers map[string]string, body []byte) (Response, error) {
	if err := remaining(ctx); err != nil {
		return Response{}, err
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return Response{}, err
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, parsed.String(), reader)
	if err != nil {
		return Response{}, err
	}
	req.Host = hostHeader(parsed)
	req.Header.Del("Accept-Encoding")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if _, ok := headers["host"]; ok {
		req.Host = headers["host"]
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		if isTimeout(err) {
			return Response{}, errHTTPTimeout
		}
		return Response{}, err
	}
	defer resp.Body.Close()

	limited := io.LimitReader(resp.Body, maxHTTPBody+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		if isTimeout(err) {
			return Response{}, errHTTPTimeout
		}
		return Response{}, err
	}
	if len(payload) > maxHTTPBody {
		return Response{}, fmt.Errorf("HTTP response exceeds %d bytes", maxHTTPBody)
	}

	out := Response{
		Status:  resp.StatusCode,
		Headers: map[string]string{},
		Body:    payload,
	}
	for name, values := range resp.Header {
		if len(values) > 0 {
			out.Headers[strings.ToLower(name)] = values[0]
		}
	}
	return out, nil
}

func remaining(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	if time.Until(deadline) <= 0 {
		return errHTTPTimeout
	}
	return nil
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errHTTPTimeout) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

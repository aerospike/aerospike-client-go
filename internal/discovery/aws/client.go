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
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

// Candidate is one seed address from DescribeInstances.
type Candidate struct {
	Address string
	Port    int
	TLSName string
}

// Client lists running EC2 instances matching the configured filters.
type Client struct {
	cfg        Config
	uri        *url.URL
	hostHeader string
	path       string
	query      string
	chain      *credentialChain
}

// NewClient validates config and returns a client.
func NewClient(cfg Config) (*Client, error) {
	normalized, uri, host, path, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:        normalized,
		uri:        uri,
		hostHeader: host,
		path:       path,
		query:      describeQuery(normalized.Filters),
		chain:      newCredentialChain(normalized.Credentials, normalized.CredentialsHook, normalized.Env, normalized.Region),
	}, nil
}

// Config returns the validated settings.
func (client *Client) Config() Config {
	return client.cfg
}

// Source returns the credential source that answered, or empty before the first refresh.
func (client *Client) Source() string {
	return client.chain.source
}

// String names the provider for logs.
func (client *Client) String() string {
	return "aws-tags " + client.cfg.Region + " " + formatFilters(client.cfg.Filters)
}

// Lookup lists matching running instances. budget bounds credentials and
// every DescribeInstances page of this refresh.
func (client *Client) Lookup(budget time.Duration) ([]Candidate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	credentials, err := client.chain.get(ctx)
	if err != nil {
		return nil, err
	}

	addresses := map[string]struct{}{}
	var nextToken string
	instances := 0
	skipped := 0
	pages := 0

	for {
		root, err := client.describe(ctx, credentials, nextToken, budget)
		if err != nil {
			return nil, err
		}
		pages++
		for _, reservation := range root.Reservations {
			for _, inst := range reservation.Instances {
				instances++
				address := inst.addressFor(client.cfg.AddressField)
				if address == "" {
					skipped++
					continue
				}
				addresses[address] = struct{}{}
			}
		}
		nextToken = strings.TrimSpace(root.NextToken)
		if nextToken == "" {
			break
		}
	}

	sorted := make([]string, 0, len(addresses))
	for address := range addresses {
		sorted = append(sorted, address)
	}
	sort.Strings(sorted)

	candidates := make([]Candidate, 0, len(sorted))
	for _, address := range sorted {
		candidates = append(candidates, Candidate{
			Address: address,
			Port:    client.cfg.Port,
			TLSName: client.cfg.TLSName,
		})
	}

	if skipped > 0 {
		logger.Logger.Debug("AWS EC2 seed provider skipped %d instances without %s", skipped, client.cfg.AddressField.Element())
	}
	logger.Logger.Debug("AWS EC2 seed provider %s returned [%s] from %d instances in %d pages",
		client.cfg.Region, strings.Join(sorted, ", "), instances, pages)
	return candidates, nil
}

func (client *Client) describe(ctx context.Context, credentials *Credentials, nextToken string, budget time.Duration) (describeInstancesResponse, error) {
	body := client.query
	if nextToken != "" {
		body += "&NextToken=" + encode(nextToken)
	}
	payload := []byte(body)
	date := amzDate(client.cfg.Env.Now().UnixMilli())
	headers := map[string]string{
		"content-type": formType,
		"host":         client.hostHeader,
		"x-amz-date":   date,
	}
	if credentials.SessionToken != "" {
		headers["x-amz-security-token"] = credentials.SessionToken
	}

	authz := authorization(credentials.AccessKeyID, credentials.SecretAccessKey, "POST", client.path, "",
		headers, payload, client.cfg.Region, ec2Service, date)
	signature := authz[strings.LastIndex(authz, "=")+1:]
	headers["authorization"] = authz

	response, err := request(ctx, "POST", client.uri.String(), headers, payload)
	if err != nil {
		if isTimeout(err) {
			return describeInstancesResponse{}, fmt.Errorf("AWS EC2 seed provider: DescribeInstances timed out after %dms", budget.Milliseconds())
		}
		return describeInstancesResponse{}, fmt.Errorf("AWS EC2 seed provider: DescribeInstances failed: %s",
			redact(err.Error(), credentials.SecretAccessKey, credentials.SessionToken, signature))
	}
	if response.Status != 200 {
		return describeInstancesResponse{}, apiError("DescribeInstances", response.Status, response.Body,
			response.Headers["x-amzn-requestid"], credentials.SecretAccessKey, credentials.SessionToken, signature)
	}
	parsed, err := parseDescribeInstances(response.Body)
	if err != nil {
		return describeInstancesResponse{}, fmt.Errorf("AWS EC2 seed provider: invalid DescribeInstances response: %s", err.Error())
	}
	return parsed, nil
}

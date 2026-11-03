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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const (
	refreshMargin = 5 * time.Minute
	imdsTokenTTL  = "21600"
	imdsCredsPath = "/latest/meta-data/iam/security-credentials/"
)

const (
	sourceConfig      = "config"
	sourceEnvironment = "environment"
	sourceProfile     = "profile"
	sourceWebIdentity = "web-identity"
	sourceECS         = "ecs"
	sourceIMDS        = "imds"
	sourceHook        = "hook"
)

type credentialChain struct {
	config   *Credentials
	hook     CredentialsHook
	env      Environment
	region   string
	source   string
	cached   *Credentials
	imdsRole string
}

func newCredentialChain(config *Credentials, hook CredentialsHook, env Environment, region string) *credentialChain {
	return &credentialChain{
		config: config,
		hook:   hook,
		env:    env,
		region: region,
	}
}

func (chain *credentialChain) get(ctx context.Context) (*Credentials, error) {
	now := chain.env.Now()
	if chain.cached != nil && (chain.cached.Expiration.IsZero() || now.Before(chain.cached.Expiration.Add(-refreshMargin))) {
		return chain.cached, nil
	}

	var credentials *Credentials
	var err error
	if chain.source == "" {
		credentials, err = chain.resolve(ctx)
	} else {
		credentials, err = chain.load(ctx, chain.source)
	}
	if err != nil {
		if chain.cached != nil && now.Before(chain.cached.Expiration) {
			logger.Logger.Warn("AWS EC2 seed provider: credentials refresh from %s failed, using cached credentials until they expire: %s",
				chain.source, err.Error())
			return chain.cached, nil
		}
		return nil, err
	}
	chain.cached = credentials
	return credentials, nil
}

func (chain *credentialChain) resolve(ctx context.Context) (*Credentials, error) {
	found := ""
	switch {
	case chain.hook != nil:
		found = sourceHook
	case chain.config != nil:
		found = sourceConfig
	case chain.environment() != nil:
		found = sourceEnvironment
	default:
		profileCreds, profileErr := chain.profile()
		if profileErr != nil {
			return nil, profileErr
		}
		switch {
		case profileCreds != nil:
			found = sourceProfile
		case chain.present("AWS_WEB_IDENTITY_TOKEN_FILE") && chain.present("AWS_ROLE_ARN"):
			found = sourceWebIdentity
		case chain.present("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") || chain.present("AWS_CONTAINER_CREDENTIALS_FULL_URI"):
			found = sourceECS
		case !strings.EqualFold(chain.env.Getenv("AWS_EC2_METADATA_DISABLED"), "true"):
			found = sourceIMDS
		default:
			return nil, fmt.Errorf("AWS EC2 seed provider: no AWS credentials found in config, environment, shared credentials file, web identity or ECS, and IMDS is disabled")
		}
	}

	credentials, err := chain.load(ctx, found)
	if err != nil {
		if found == sourceIMDS {
			return nil, fmt.Errorf("AWS EC2 seed provider: no AWS credentials found in config, environment, shared credentials file, web identity or ECS, and %s",
				strings.TrimPrefix(err.Error(), "AWS EC2 seed provider: "))
		}
		return nil, err
	}

	chain.source = found
	logger.Logger.Info("AWS EC2 seed provider credentials from %s", found)
	return credentials, nil
}

func (chain *credentialChain) load(ctx context.Context, from string) (*Credentials, error) {
	switch from {
	case sourceHook:
		return chain.fromHook()
	case sourceConfig:
		return chain.config, nil
	case sourceEnvironment:
		return chain.environment(), nil
	case sourceProfile:
		return chain.profile()
	case sourceWebIdentity:
		return chain.webIdentity(ctx)
	case sourceECS:
		return chain.ecs(ctx)
	default:
		return chain.imds(ctx)
	}
}

func (chain *credentialChain) fromHook() (credentials *Credentials, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			credentials = nil
			err = fmt.Errorf("AWS EC2 seed provider: credentials hook failed: %T", recovered)
		}
	}()

	credentials, err = chain.hook()
	if err != nil {
		return nil, fmt.Errorf("AWS EC2 seed provider: credentials hook failed: %T", err)
	}
	if credentials == nil {
		return nil, fmt.Errorf("AWS EC2 seed provider: credentials hook returned nil")
	}
	if err := validateCredentials(*credentials); err != nil {
		return nil, err
	}
	return credentials, nil
}

func (chain *credentialChain) environment() *Credentials {
	key := chain.env.Getenv("AWS_ACCESS_KEY_ID")
	secret := chain.env.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" || secret == "" {
		return nil
	}
	return &Credentials{
		AccessKeyID:     key,
		SecretAccessKey: secret,
		SessionToken:    chain.env.Getenv("AWS_SESSION_TOKEN"),
	}
}

func (chain *credentialChain) profile() (*Credentials, error) {
	file := chain.env.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	var path string
	switch {
	case file == "":
		path = filepath.Join(chain.env.UserHome(), ".aws", "credentials")
	case strings.HasPrefix(file, "~/"):
		path = filepath.Join(chain.env.UserHome(), file[2:])
	default:
		path = file
	}

	opened, err := os.Open(path)
	if err != nil {
		return nil, nil
	}
	defer opened.Close()

	profile := chain.env.Getenv("AWS_PROFILE")
	if profile == "" {
		profile = "default"
	}

	keys := map[string]string{}
	inProfile := false
	scanner := bufio.NewScanner(opened)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inProfile = strings.TrimSpace(line[1:len(line)-1]) == profile
			continue
		}
		eq := strings.IndexByte(line, '=')
		if inProfile && eq > 0 {
			keys[strings.TrimSpace(line[:eq])] = strings.TrimSpace(line[eq+1:])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("AWS EC2 seed provider: failed to read %s: %v", path, err)
	}

	key := keys["aws_access_key_id"]
	secret := keys["aws_secret_access_key"]
	if key == "" || secret == "" {
		return nil, nil
	}
	return &Credentials{
		AccessKeyID:     key,
		SecretAccessKey: secret,
		SessionToken:    keys["aws_session_token"],
	}, nil
}

func (chain *credentialChain) webIdentity(ctx context.Context) (*Credentials, error) {
	tokenBytes, err := os.ReadFile(chain.env.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"))
	if err != nil {
		return nil, fmt.Errorf("AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: %v", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	session := chain.env.Getenv("AWS_ROLE_SESSION_NAME")
	if session == "" {
		session = fmt.Sprintf("aerospike-client-%d", chain.env.Now().UnixMilli())
	}

	body := "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=" +
		encode(chain.env.Getenv("AWS_ROLE_ARN")) + "&RoleSessionName=" + encode(session) +
		"&WebIdentityToken=" + encode(token)
	headers := map[string]string{"content-type": formType}
	response, err := chain.send(ctx, "STS AssumeRoleWithWebIdentity", "POST", chain.env.STSEndpoint(chain.region), headers, []byte(body), token)
	if err != nil {
		return nil, err
	}
	if response.Status != 200 {
		return nil, apiError("STS AssumeRoleWithWebIdentity", response.Status, response.Body, response.Headers["x-amzn-requestid"], token)
	}

	fields := xmlTexts(response.Body, "AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
	if fields["AccessKeyId"] == "" {
		return nil, fmt.Errorf("AWS EC2 seed provider: STS AssumeRoleWithWebIdentity response has no credentials")
	}
	return temporary("STS AssumeRoleWithWebIdentity", fields["AccessKeyId"], fields["SecretAccessKey"], fields["SessionToken"], fields["Expiration"])
}

func (chain *credentialChain) ecs(ctx context.Context) (*Credentials, error) {
	relative := chain.env.Getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
	uri := chain.env.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI")
	if relative != "" {
		uri = chain.env.ECSEndpoint() + relative
	}

	var authorization string
	file := chain.env.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
	if file != "" {
		tokenBytes, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("AWS EC2 seed provider: failed to read AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: %v", err)
		}
		authorization = strings.TrimSpace(string(tokenBytes))
	} else {
		authorization = chain.env.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN")
	}

	headers := map[string]string{}
	if authorization != "" {
		headers["authorization"] = authorization
	}

	response, err := chain.send(ctx, "ECS credentials", "GET", uri, headers, nil, authorization)
	if err != nil {
		return nil, err
	}
	if response.Status != 200 {
		return nil, fmt.Errorf("AWS EC2 seed provider: ECS credentials failed: HTTP %d", response.Status)
	}
	parsed, err := parseCredsJSON(response, "ECS credentials")
	if err != nil {
		return nil, err
	}
	return temporary("ECS credentials", parsed.AccessKeyID, parsed.SecretAccessKey, parsed.Token, parsed.Expiration)
}

func (chain *credentialChain) imds(ctx context.Context) (*Credentials, error) {
	base := chain.env.IMDSEndpoint()
	headers := map[string]string{"x-aws-ec2-metadata-token-ttl-seconds": imdsTokenTTL}
	response, err := chain.send(ctx, "IMDS token", "PUT", base+"/latest/api/token", headers, []byte{}, "")
	if err != nil {
		return nil, err
	}
	if response.Status != 200 {
		return nil, fmt.Errorf("AWS EC2 seed provider: IMDS token failed: HTTP %d", response.Status)
	}

	token := strings.TrimSpace(response.text())
	headers = map[string]string{"x-aws-ec2-metadata-token": token}

	if chain.imdsRole == "" {
		response, err = chain.send(ctx, "IMDS role", "GET", base+imdsCredsPath, headers, nil, token)
		if err != nil {
			return nil, err
		}
		if response.Status != 200 {
			return nil, fmt.Errorf("AWS EC2 seed provider: IMDS role failed: HTTP %d (no instance profile attached?)", response.Status)
		}
		role := strings.TrimSpace(response.text())
		if i := strings.IndexByte(role, '\n'); i >= 0 {
			role = strings.TrimSpace(role[:i])
		}
		if role == "" {
			return nil, fmt.Errorf("AWS EC2 seed provider: IMDS returned no instance profile role")
		}
		chain.imdsRole = role
	}

	response, err = chain.send(ctx, "IMDS credentials", "GET", base+imdsCredsPath+chain.imdsRole, headers, nil, token)
	if err != nil {
		return nil, err
	}
	if response.Status != 200 {
		return nil, fmt.Errorf("AWS EC2 seed provider: IMDS credentials failed: HTTP %d", response.Status)
	}

	parsed, err := parseCredsJSON(response, "IMDS credentials")
	if err != nil {
		return nil, err
	}
	if parsed.Code != "" && parsed.Code != "Success" {
		return nil, fmt.Errorf("AWS EC2 seed provider: IMDS credentials failed: %s", parsed.Code)
	}
	return temporary("IMDS credentials", parsed.AccessKeyID, parsed.SecretAccessKey, parsed.Token, parsed.Expiration)
}

func (chain *credentialChain) send(ctx context.Context, action, method, rawURL string, headers map[string]string, body []byte, secrets ...string) (Response, error) {
	response, err := request(ctx, method, rawURL, headers, body)
	if err != nil {
		if isTimeout(err) {
			return Response{}, fmt.Errorf("AWS EC2 seed provider: %s timed out", action)
		}
		return Response{}, fmt.Errorf("AWS EC2 seed provider: %s failed: %s", action, redact(err.Error(), secrets...))
	}
	return response, nil
}

func (chain *credentialChain) present(name string) bool {
	return chain.env.Getenv(name) != ""
}

type credsJSON struct {
	Code            string
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string
	Token           string
	Expiration      string
}

func parseCredsJSON(response Response, action string) (credsJSON, error) {
	var parsed credsJSON
	if err := json.Unmarshal(response.Body, &parsed); err != nil {
		return credsJSON{}, fmt.Errorf("AWS EC2 seed provider: invalid %s response", action)
	}
	return parsed, nil
}

func temporary(action, key, secret, token, expiration string) (*Credentials, error) {
	if key == "" || secret == "" || expiration == "" {
		return nil, fmt.Errorf("AWS EC2 seed provider: %s response has no credentials", action)
	}
	expires, err := time.Parse(time.RFC3339, expiration)
	if err != nil {
		return nil, fmt.Errorf("AWS EC2 seed provider: %s returned an invalid expiration: %s", action, expiration)
	}
	return &Credentials{
		AccessKeyID:     key,
		SecretAccessKey: secret,
		SessionToken:    token,
		Expiration:      expires,
	}, nil
}

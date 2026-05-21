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

package aerospike_test

import (
	"errors"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Ported from the Java client's TestStringMasking. Each test exercises one
// privilege boundary on a bin protected by a server-side masking rule:
//   - read with `read-masked` should observe the real value;
//   - read without it should observe the masked value;
//   - modify without `write-masked` should fail with ROLE_VIOLATION.
//
// The suite bootstraps two extra users (one privileged reader, one
// unprivileged) and opens an additional client per role. The whole suite
// is skipped when security is not enabled or the cluster is older than the
// 8.1.3 build that introduced string ops + masking.
var _ = gg.Describe("String Masking Tests", gg.Ordered, func() {
	const (
		maskedBin     = "pii"
		unmaskedBin   = "public"
		initialValue  = "hello world"
		initialPublic = "visible data"
		maskFunction  = "redact"
		privUser      = "stringops_reader"
		unprivUser    = "stringops_user"
		userPassword  = "stringops_pw1!"
	)

	var (
		ns           = *namespace
		set          = randString(50)
		key          *as.Key
		policy       = as.DefaultStringPolicy
		privClient   *as.Client
		unprivClient *as.Client
		skipped      bool
	)

	applyMaskRule := func(bin, function, extra string) {
		cmd := "masking:namespace=" + ns + ";set=" + set + ";bin=" + bin + ";type=string;function=" + function
		if extra != "" {
			cmd += ";" + extra
		}
		ipol := as.NewInfoPolicy()
		for _, node := range client.GetNodes() {
			_, err := node.RequestInfo(ipol, cmd)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
		// Give the rule time to propagate before exercising it.
		time.Sleep(500 * time.Millisecond)
	}

	removeMaskRule := func(bin string) {
		cmd := "masking:namespace=" + ns + ";set=" + set + ";bin=" + bin + ";type=string;function=remove"
		ipol := as.NewInfoPolicy()
		for _, node := range client.GetNodes() {
			_, _ = node.RequestInfo(ipol, cmd)
		}
		time.Sleep(500 * time.Millisecond)
	}

	newUserClient := func(user string) *as.Client {
		p := *clientPolicy
		p.User = user
		p.Password = userPassword
		c, err := as.NewClientWithPolicy(&p, *host, *port)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return c
	}

	expectRoleViolation := func(err error) {
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue(), "Error should be an AerospikeError")
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.ROLE_VIOLATION))
	}

	gg.BeforeAll(func() {
		if !securityEnabled() {
			gg.Skip("String masking tests require security enabled (Enterprise Edition + access-control).")
			skipped = true
			return
		}
		requiredVersion, err := version.Parse("8.1.2")
		if err != nil {
			gg.Fail("Failed to parse required version")
		}
		nodeVersion := client.GetNodes()[0].GetServerVersion()
		if nodeVersion.IsSmaller(requiredVersion) {
			gg.Skip("String masking requires server version 8.1.3+.")
			skipped = true
			return
		}

		// Drop possibly-leftover users from a previous run.
		_ = client.DropUser(nil, privUser)
		_ = client.DropUser(nil, unprivUser)
		time.Sleep(500 * time.Millisecond)

		gm.Expect(client.CreateUser(nil, privUser, userPassword,
			[]string{"read-write", "read-masked"})).ToNot(gm.HaveOccurred())
		gm.Expect(client.CreateUser(nil, unprivUser, userPassword,
			[]string{"read-write"})).ToNot(gm.HaveOccurred())
		time.Sleep(500 * time.Millisecond)

		privClient = newUserClient(privUser)
		unprivClient = newUserClient(unprivUser)

		applyMaskRule(maskedBin, maskFunction, "")
	})

	gg.AfterAll(func() {
		if skipped {
			return
		}
		removeMaskRule(maskedBin)
		_ = client.DropUser(nil, privUser)
		_ = client.DropUser(nil, unprivUser)
		if privClient != nil {
			privClient.Close()
		}
		if unprivClient != nil {
			unprivClient.Close()
		}
	})

	gg.BeforeEach(func() {
		if skipped {
			gg.Skip("Setup was skipped.")
			return
		}
		var err as.Error
		key, err = as.NewKey(ns, set, "stringmask-key")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key,
			as.NewBin(maskedBin, initialValue),
			as.NewBin(unmaskedBin, initialPublic),
		)).ToNot(gm.HaveOccurred())
	})

	// ============================================================
	// Read ops: privilege gates which value the caller observes
	// ============================================================

	gg.It("read-masked sees the real value via strlen", func() {
		rec, err := privClient.Operate(nil, key, as.StrLenOp(maskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal(len(initialValue)))
	})

	gg.It("read-masked sees the real value via substr", func() {
		rec, err := privClient.Operate(nil, key, as.StrSubstrOp(maskedBin, 0, 5))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal("hello"))
	})

	gg.It("unprivileged sees masked substring", func() {
		rec, err := unprivClient.Operate(nil, key, as.StrSubstrOp(maskedBin, 0, 5))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		s, ok := rec.Bins[maskedBin].(string)
		gm.Expect(ok).To(gm.BeTrue(), "expected string result from masked substr")
		gm.Expect(len(s)).To(gm.Equal(5))
		gm.Expect(s).ToNot(gm.Equal("hello"))
	})

	gg.It("unprivileged find on masked bin does not locate real content", func() {
		rec, err := unprivClient.Operate(nil, key, as.StrFindOp(maskedBin, "world"))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal(-1))
	})

	gg.It("unprivileged contains on masked bin is false", func() {
		rec, err := unprivClient.Operate(nil, key, as.StrContainsOp(maskedBin, "hello"))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal(false))
	})

	gg.It("unprivileged startsWith and endsWith on masked bin are false", func() {
		sw, err := unprivClient.Operate(nil, key, as.StrStartsWithOp(maskedBin, "hello"))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(sw.Bins[maskedBin]).To(gm.Equal(false))

		ew, err := unprivClient.Operate(nil, key, as.StrEndsWithOp(maskedBin, "world"))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(ew.Bins[maskedBin]).To(gm.Equal(false))
	})

	gg.It("unprivileged regexCompare on masked bin does not match real value", func() {
		rec, err := unprivClient.Operate(nil, key, as.StrRegexCompareOp(maskedBin, "hello.*"))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal(false))
	})

	gg.It("strlen is unaffected by redaction", func() {
		// Redact preserves length, so both clients agree on byteLength.
		priv, err := privClient.Operate(nil, key, as.StrByteLengthOp(maskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(priv.Bins[maskedBin]).To(gm.Equal(len(initialValue)))

		unp, err := unprivClient.Operate(nil, key, as.StrByteLengthOp(maskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(unp.Bins[maskedBin]).To(gm.Equal(len(initialValue)))
	})

	// ============================================================
	// Read ops on the unmasked bin — both users see the real data
	// ============================================================

	gg.It("unmasked bin is transparent to both users", func() {
		priv, err := privClient.Operate(nil, key, as.StrLenOp(unmaskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(priv.Bins[unmaskedBin]).To(gm.Equal(len(initialPublic)))

		unp, err := unprivClient.Operate(nil, key, as.StrLenOp(unmaskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(unp.Bins[unmaskedBin]).To(gm.Equal(len(initialPublic)))
	})

	// ============================================================
	// Modify ops: blocked without write-masked
	// ============================================================

	gg.It("write-masked required for upper", func() {
		_, err := unprivClient.Operate(nil, key, as.StrUpperOp(policy, maskedBin))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for insert", func() {
		_, err := unprivClient.Operate(nil, key, as.StrInsertOp(policy, maskedBin, 5, " beautiful"))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for concat", func() {
		_, err := unprivClient.Operate(nil, key, as.StrConcatOp(policy, maskedBin, "!"))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for replace", func() {
		_, err := unprivClient.Operate(nil, key, as.StrReplaceOp(policy, maskedBin, "world", "earth"))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for trim", func() {
		gm.Expect(client.PutBins(nil, key, as.NewBin(maskedBin, "  padded  "))).ToNot(gm.HaveOccurred())
		_, err := unprivClient.Operate(nil, key, as.StrTrimOp(policy, maskedBin))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for padStart", func() {
		_, err := unprivClient.Operate(nil, key, as.StrPadStartOp(policy, maskedBin, 20, "*"))
		expectRoleViolation(err)
	})

	gg.It("write-masked required for regexReplace", func() {
		_, err := unprivClient.Operate(nil, key, as.StrRegexReplaceOp(policy, maskedBin, "[0-9]+", "NUM", as.StringRegexDefault))
		expectRoleViolation(err)
	})

	// ============================================================
	// read-masked still cannot modify; admin still can.
	// ============================================================

	gg.It("read-masked cannot modify", func() {
		_, err := privClient.Operate(nil, key, as.StrUpperOp(policy, maskedBin))
		expectRoleViolation(err)
	})

	gg.It("admin can modify masked bin", func() {
		_, err := client.Operate(nil, key, as.StrUpperOp(policy, maskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal("HELLO WORLD"))
	})

	// ============================================================
	// Modify on unmasked bin succeeds for unprivileged user.
	// ============================================================

	gg.It("unprivileged can modify unmasked bin", func() {
		_, err := unprivClient.Operate(nil, key, as.StrUpperOp(policy, unmaskedBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[unmaskedBin]).To(gm.Equal("VISIBLE DATA"))
		// The masked bin is left untouched.
		gm.Expect(rec.Bins[maskedBin]).To(gm.Equal(initialValue))
	})

	// ============================================================
	// Constant-mask variant: unprivileged sees a fixed string
	// ============================================================

	gg.It("constant mask is observed by unprivileged read", func() {
		const constBin = "secret"
		const constValue = "HIDDEN"
		const real = "real secret data"
		constKey, err := as.NewKey(ns, set, "stringmask-const")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		applyMaskRule(constBin, "constant", "value="+constValue)
		defer func() {
			client.Delete(nil, constKey)
			removeMaskRule(constBin)
		}()

		client.Delete(nil, constKey)
		gm.Expect(client.PutBins(nil, constKey, as.NewBin(constBin, real))).ToNot(gm.HaveOccurred())

		priv, err := privClient.Operate(nil, constKey, as.StrLenOp(constBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(priv.Bins[constBin]).To(gm.Equal(len(real)))

		unp, err := unprivClient.Operate(nil, constKey, as.StrLenOp(constBin))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(unp.Bins[constBin]).To(gm.Equal(len(constValue)))

		privSub, err := privClient.Operate(nil, constKey, as.StrSubstrOp(constBin, 0, 4))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(privSub.Bins[constBin]).To(gm.Equal("real"))

		unpSub, err := unprivClient.Operate(nil, constKey, as.StrSubstrOp(constBin, 0, 4))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(unpSub.Bins[constBin]).To(gm.Equal("HIDD"))
	})
})

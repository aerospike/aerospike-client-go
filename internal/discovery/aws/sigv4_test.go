package aws

import (
	"encoding/hex"
	"testing"
)

const (
	exampleSecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	emptyHash     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func TestSigningKeyMatchesAWSExample(t *testing.T) {
	got := hex.EncodeToString(signingKey(exampleSecret, "20120215", "us-east-1", "iam"))
	if got != "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d" {
		t.Fatalf("signing key: got %s", got)
	}
}

func TestCanonicalRequestAndSignatureMatchIAMListUsersExample(t *testing.T) {
	headers := map[string]string{
		"content-type": "application/x-www-form-urlencoded; charset=utf-8",
		"host":         "iam.amazonaws.com",
		"x-amz-date":   "20150830T123600Z",
	}

	canonical := canonicalRequest("GET", "/", "Action=ListUsers&Version=2010-05-08", headers, hex.EncodeToString(sha256Sum(nil)))
	expectedCanonical := "GET\n" +
		"/\n" +
		"Action=ListUsers&Version=2010-05-08\n" +
		"content-type:application/x-www-form-urlencoded; charset=utf-8\n" +
		"host:iam.amazonaws.com\n" +
		"x-amz-date:20150830T123600Z\n" +
		"\n" +
		"content-type;host;x-amz-date\n" +
		emptyHash
	if canonical != expectedCanonical {
		t.Fatalf("canonical request:\n%s\nwant:\n%s", canonical, expectedCanonical)
	}

	canonicalHash := hex.EncodeToString(sha256Sum([]byte(canonical)))
	if canonicalHash != "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59" {
		t.Fatalf("canonical hash: got %s", canonicalHash)
	}

	credScope := credentialScope("20150830", "us-east-1", "iam")
	toSign := stringToSign("20150830T123600Z", credScope, canonical)
	expectedStringToSign := "AWS4-HMAC-SHA256\n" +
		"20150830T123600Z\n" +
		"20150830/us-east-1/iam/aws4_request\n" +
		"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
	if toSign != expectedStringToSign {
		t.Fatalf("string to sign:\n%s", toSign)
	}

	key := signingKey(exampleSecret, "20150830", "us-east-1", "iam")
	if hex.EncodeToString(key) != "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9" {
		t.Fatalf("signing key: got %s", hex.EncodeToString(key))
	}

	sig := requestSignature(key, toSign)
	if sig != "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7" {
		t.Fatalf("signature: got %s", sig)
	}
}

func TestAuthorizationMatchesGetVanillaSuite(t *testing.T) {
	headers := map[string]string{
		"host":       "example.amazonaws.com",
		"x-amz-date": "20150830T123600Z",
	}

	canonical := canonicalRequest("GET", "/", "", headers, emptyHash)
	expectedCanonical := "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\n" + emptyHash
	if canonical != expectedCanonical {
		t.Fatalf("canonical request:\n%s", canonical)
	}

	toSign := stringToSign("20150830T123600Z", credentialScope("20150830", "us-east-1", "service"), canonical)
	expectedStringToSign := "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n" +
		"bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
	if toSign != expectedStringToSign {
		t.Fatalf("string to sign:\n%s", toSign)
	}

	got := authorization("AKIDEXAMPLE", exampleSecret, "GET", "/", "", headers, []byte{}, "us-east-1", "service", "20150830T123600Z")
	expected := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
		"SignedHeaders=host;x-amz-date, " +
		"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got != expected {
		t.Fatalf("authorization:\n%s", got)
	}
}

func TestEncodePercentEncodesReservedBytes(t *testing.T) {
	if got := encode("tag:aerospike-cluster"); got != "tag%3Aaerospike-cluster" {
		t.Fatalf("got %s", got)
	}
	if got := encode("a b~c_d.e-f/+="); got != "a%20b~c_d.e-f%2F%2B%3D" {
		t.Fatalf("got %s", got)
	}
	if got := encode("é"); got != "%C3%A9" {
		t.Fatalf("got %s", got)
	}
}

func TestAmzDateFormatsUTCTimestamp(t *testing.T) {
	if got := amzDate(1440938160000); got != "20150830T123600Z" {
		t.Fatalf("got %s", got)
	}
}

package aerospike

import "testing"

// TestSetReadHeaderDoesNotMutateCallerPolicy pins the copy in setReadHeader: a
// header read must clear SendKey for itself only, never for the caller's policy,
// which is typically the client's shared default.
func TestSetReadHeaderDoesNotMutateCallerPolicy(t *testing.T) {
	policy := NewPolicy()
	policy.SendKey = true

	key, err := NewKey("test", "sendkeycheck", "k1")
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	cmd := &baseCommand{}
	cmd.dataBuffer = make([]byte, 1024)
	if err := cmd.setReadHeader(policy, key); err != nil {
		t.Fatalf("setReadHeader: %v", err)
	}

	if !policy.SendKey {
		t.Error("setReadHeader cleared SendKey on the caller's policy; it must copy instead")
	}
}

package aerospike

import "testing"

// TestAELWireFormat pins the AEL encoding: a two-element MessagePack array
// holding opcode 128 and the raw UTF-8 source.
func TestAELWireFormat(t *testing.T) {
	e := ExpAEL("$.bin==1")
	b := e.bytes
	if len(b) < 4 {
		t.Fatalf("packed AEL is %d bytes, want more than 4", len(b))
	}
	if b[0] != 0x92 {
		t.Errorf("b[0] = %#x, want 0x92 (two-element array)", b[0])
	}
	if b[1] != 0xcc {
		t.Errorf("b[1] = %#x, want 0xcc (uint8 marker)", b[1])
	}
	if b[2] != 0x80 {
		t.Errorf("b[2] = %#x, want 0x80 (opcode 128)", b[2])
	}
	if want := byte(0xa0 + len("$.bin==1")); b[3] != want {
		t.Errorf("b[3] = %#x, want %#x (fixstr header)", b[3], want)
	}
	if got := string(b[4 : 4+len("$.bin==1")]); got != "$.bin==1" {
		t.Errorf("payload = %q, want the source verbatim", got)
	}
}

// TestAELWireFormatStr8 checks the 32-byte boundary where fixstr gives way to
// str8.
func TestAELWireFormatStr8(t *testing.T) {
	src := ""
	for range 32 {
		src += "a"
	}
	b := ExpAEL(src).bytes
	if b[0] != 0x92 || b[1] != 0xcc || b[2] != 0x80 {
		t.Fatalf("header = % x, want 92 cc 80", b[:3])
	}
	if b[3] != 0xd9 {
		t.Errorf("b[3] = %#x, want 0xd9 (str8)", b[3])
	}
	if b[4] != 32 {
		t.Errorf("b[4] = %d, want 32", b[4])
	}
}

// TestAELWireFormatUTF8 checks that multi-byte source travels verbatim.
func TestAELWireFormatUTF8(t *testing.T) {
	src := "$.café=='é'"
	b := ExpAEL(src).bytes
	if want := byte(0xa0 + len(src)); b[3] != want {
		t.Errorf("b[3] = %#x, want %#x", b[3], want)
	}
	if got := string(b[4 : 4+len(src)]); got != src {
		t.Errorf("payload = %q, want %q", got, src)
	}
}

// TestAELIsAEL checks the discriminator.
func TestAELIsAEL(t *testing.T) {
	if !ExpAEL("$.a==1").IsAEL() {
		t.Error("ExpAEL is not reported as AEL")
	}
	if ExpEq(ExpIntBin("a"), ExpIntVal(1)).IsAEL() {
		t.Error("a client-compiled expression is reported as AEL")
	}
	var nilExp *Expression
	if nilExp.IsAEL() {
		t.Error("a nil expression is reported as AEL")
	}
}

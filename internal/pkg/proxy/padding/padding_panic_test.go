package padding

import (
	"testing"
)

// TestPaddingFactoryHugeRange verifies that a padding scheme with range
// values exceeding zeroBuf size (65536) does not cause a panic.
// Bug: zeroBuf is [65536]byte, but padding ranges are not bounds-checked,
// so zeroBuf[:paddingLen] panics if paddingLen > 65536.
func TestPaddingFactoryHugeRange(t *testing.T) {
	// Create a padding scheme with a range that exceeds 65536
	hugeScheme := []byte("stop=2\n0=70000-70000\n1=70000-70000")
	factory := NewPaddingFactory(hugeScheme)
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("GenerateRecordPayloadSizes panicked with huge range: %v", r)
		}
	}()

	sizes := factory.GenerateRecordPayloadSizes(0)
	if len(sizes) != 1 {
		t.Fatalf("expected 1 size, got %d", len(sizes))
	}
	// The generated size should be clamped to a safe maximum
	if sizes[0] > 65535 {
		t.Fatalf("padding size %d exceeds uint16 max, will cause protocol issues", sizes[0])
	}
}

// TestPaddingFactoryRejectsExcessiveRange verifies that ranges above the
// safe maximum are rejected or clamped during parsing.
func TestPaddingFactoryRejectsExcessiveRange(t *testing.T) {
	hugeScheme := []byte("stop=2\n0=100000-100000")
	factory := NewPaddingFactory(hugeScheme)
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}

	sizes := factory.GenerateRecordPayloadSizes(0)
	for _, sz := range sizes {
		if sz > 65535 {
			t.Fatalf("padding size %d exceeds safe limit", sz)
		}
	}
}

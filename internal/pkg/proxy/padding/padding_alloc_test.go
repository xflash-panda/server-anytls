package padding

import (
	"testing"
)

// RED: GenerateRecordPayloadSizes allocates a new []int slice on every call.
func BenchmarkGenerateRecordPayloadSizes_Allocs(b *testing.B) {
	p := NewPaddingFactory([]byte("stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"))
	if p == nil {
		b.Fatal("failed to create padding factory")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// pkt=2 has the most ranges (9 elements) = worst case allocation
		p.GenerateRecordPayloadSizes(2)
	}
}

// GREEN: AppendRecordPayloadSizes reuses a caller-provided slice. 0 allocs expected.
func BenchmarkAppendRecordPayloadSizes_ReuseBuf(b *testing.B) {
	p := NewPaddingFactory([]byte("stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"))
	if p == nil {
		b.Fatal("failed to create padding factory")
	}

	buf := make([]int, 0, 16)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = p.AppendRecordPayloadSizes(2, buf[:0])
	}
	_ = buf
}

// TestAppendRecordPayloadSizes_EquivalentToGenerate verifies the new method
// produces identical results to the original.
func TestAppendRecordPayloadSizes_EquivalentToGenerate(t *testing.T) {
	p := NewPaddingFactory([]byte("stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"))
	if p == nil {
		t.Fatal("failed to create padding factory")
	}

	// For deterministic ranges (min==max), results must match exactly
	buf := make([]int, 0, 16)
	for pkt := uint32(0); pkt < p.Stop; pkt++ {
		old := p.GenerateRecordPayloadSizes(pkt)
		buf = p.AppendRecordPayloadSizes(pkt, buf[:0])

		if len(old) != len(buf) {
			t.Fatalf("pkt=%d: length mismatch old=%d new=%d", pkt, len(old), len(buf))
		}

		ranges := p.parsedRanges[pkt]
		for i, r := range ranges {
			if i >= len(old) {
				break
			}
			// For fixed ranges, values must match exactly
			if !r.isCheck && r.min == r.max {
				if old[i] != buf[i] {
					t.Fatalf("pkt=%d idx=%d: old=%d new=%d", pkt, i, old[i], buf[i])
				}
			}
			// For check marks, both must be CheckMark
			if r.isCheck {
				if old[i] != CheckMark || buf[i] != CheckMark {
					t.Fatalf("pkt=%d idx=%d: expected CheckMark", pkt, i)
				}
			}
			// For random ranges, just verify within bounds
			if !r.isCheck && r.min != r.max {
				if int64(buf[i]) < r.min || int64(buf[i]) > r.max {
					t.Fatalf("pkt=%d idx=%d: %d out of range [%d,%d]", pkt, i, buf[i], r.min, r.max)
				}
			}
		}
	}
}

// TestAppendRecordPayloadSizes_NoAlloc verifies reuse path does not allocate.
func TestAppendRecordPayloadSizes_NoAlloc(t *testing.T) {
	p := NewPaddingFactory([]byte("stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"))
	if p == nil {
		t.Fatal("failed to create padding factory")
	}

	buf := make([]int, 0, 16)
	allocs := testing.AllocsPerRun(1000, func() {
		buf = p.AppendRecordPayloadSizes(2, buf[:0])
	})
	if allocs > 0 {
		t.Fatalf("expected 0 allocs, got %f", allocs)
	}
}

package padding

import (
	"testing"
)

// BenchmarkGenerateRecordPayloadSizes measures the cost of padding size
// calculation including random number generation and slice growth.
func BenchmarkGenerateRecordPayloadSizes(b *testing.B) {
	p := NewPaddingFactory([]byte("stop=8\n0=30-30\n1=100-400\n2=400-500,c,500-1000\n3=9-9,500-1000\n4=500-1000\n5=500-1000\n6=500-1000\n7=500-1000"))
	if p == nil {
		b.Fatal("failed to create padding factory")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.GenerateRecordPayloadSizes(uint32(i%8 + 1))
	}
}

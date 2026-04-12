//nolint:errcheck
package pipe

import (
	"testing"
)

// BenchmarkPipeThroughput measures end-to-end pipe throughput to capture
// the combined effect of signaling, mutex, and buffer operations.
func BenchmarkPipeThroughput(b *testing.B) {
	r, w := Pipe()

	data := make([]byte, 1024)
	readBuf := make([]byte, 1024)

	go func() {
		for {
			if _, err := r.Read(readBuf); err != nil {
				return
			}
		}
	}()

	b.SetBytes(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(data); err != nil {
			b.Fatal(err)
		}
	}

	w.Close()
}

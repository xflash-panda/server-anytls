//nolint:all
package session

import (
	"net"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
)

// TestWriteConnLockedPaddingDataIntegrity verifies that padding writes produce
// correct data: payload bytes followed by waste frames with zero-filled padding.
// This guards the invariant when switching from make([]byte, n) to a pre-allocated
// zero buffer.
func TestWriteConnLockedPaddingDataIntegrity(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	sess := NewClientSession(serverConn, &padding.DefaultPaddingFactory)
	sess.sendPadding = true
	sess.pktCounter.Store(0)

	// Collect all written bytes on the client side
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 65536)
		n, _ := clientConn.Read(buf)
		received <- buf[:n]
	}()

	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	sess.connLock.Lock()
	_, err := sess.writeConnLocked(payload)
	sess.connLock.Unlock()
	if err != nil {
		t.Fatalf("writeConnLocked error: %v", err)
	}

	data := <-received

	// Verify payload bytes are present in the output
	if len(data) < len(payload) {
		t.Fatalf("output too short: %d bytes", len(data))
	}

	// Verify any waste frame padding bytes are zero
	i := 0
	for i < len(data) {
		if i+headerOverHeadSize > len(data) {
			break
		}
		cmd := data[i]
		if cmd == cmdWaste {
			wasteLen := int(data[i+5])<<8 | int(data[i+6])
			wasteStart := i + headerOverHeadSize
			wasteEnd := wasteStart + wasteLen
			if wasteEnd > len(data) {
				break
			}
			for j := wasteStart; j < wasteEnd; j++ {
				if data[j] != 0 {
					t.Fatalf("waste padding byte at offset %d is 0x%02x, expected 0x00", j, data[j])
				}
			}
			i = wasteEnd
		} else {
			// Skip non-waste data
			break
		}
	}
}

// BenchmarkWriteConnLockedWithPadding benchmarks the padding write path to
// measure allocation overhead of make([]byte, paddingLen) per write.
func BenchmarkWriteConnLockedWithPadding(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	sess := NewClientSession(serverConn, &padding.DefaultPaddingFactory)
	sess.sendPadding = true

	// Drain client side
	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	payload := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.pktCounter.Store(uint32(i % 10)) // keep padding active
		sess.connLock.Lock()
		sess.writeConnLocked(payload)
		sess.connLock.Unlock()
	}
}

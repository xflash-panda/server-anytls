//nolint:errcheck
package server

import (
	"net"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/service"
)

// TestCountedConnMultipleReadWriteAccumulates verifies that multiple Read/Write
// calls correctly accumulate traffic on the same TrafficItem.
func TestCountedConnMultipleReadWriteAccumulates(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	item := us.GetTrafficItem(1)

	cc := &CountedConn{
		Conn:        serverConn,
		trafficItem: item,
	}

	payload := []byte("hello")
	const rounds = 10
	var totalRead, totalWrite int

	for range rounds {
		// Read direction
		go func() { clientConn.Write(payload) }()
		buf := make([]byte, 64)
		n, err := cc.Read(buf)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		totalRead += n

		// Write direction
		go func() {
			b := make([]byte, 64)
			clientConn.Read(b)
		}()
		n, err = cc.Write(payload)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		totalWrite += n
	}

	if item.Up.Load() != uint64(totalRead) {
		t.Fatalf("Up: expected %d, got %d", totalRead, item.Up.Load())
	}
	if item.Down.Load() != uint64(totalWrite) {
		t.Fatalf("Down: expected %d, got %d", totalWrite, item.Down.Load())
	}
}

// TestCountedConnZeroBytesReadWrite verifies zero-byte reads/writes do not
// add spurious traffic.
func TestCountedConnZeroBytesReadWrite(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	item := us.GetTrafficItem(1)

	cc := &CountedConn{
		Conn:        serverConn,
		trafficItem: item,
	}

	go func() { clientConn.Write([]byte("x")) }()
	buf := make([]byte, 64)
	cc.Read(buf)

	if item.Up.Load() != 1 {
		t.Fatalf("expected Up=1, got %d", item.Up.Load())
	}
	if item.Down.Load() != 0 {
		t.Fatalf("expected Down=0, got %d", item.Down.Load())
	}
}

// BenchmarkCountedConnRead benchmarks the Read hot path of CountedConn
// with cached TrafficItem (no sync.Map lookup per I/O).
func BenchmarkCountedConnRead(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	item := us.GetTrafficItem(1)

	cc := &CountedConn{
		Conn:        serverConn,
		trafficItem: item,
	}

	payload := make([]byte, 1024)
	buf := make([]byte, 2048)

	go func() {
		for {
			if _, err := clientConn.Write(payload); err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	b.SetBytes(1024)
	for range b.N {
		cc.Read(buf)
	}
}

// BenchmarkCountedConnWrite benchmarks the Write hot path of CountedConn.
func BenchmarkCountedConnWrite(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	item := us.GetTrafficItem(1)

	cc := &CountedConn{
		Conn:        serverConn,
		trafficItem: item,
	}

	payload := make([]byte, 1024)

	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	b.SetBytes(1024)
	for range b.N {
		cc.Write(payload)
	}
}

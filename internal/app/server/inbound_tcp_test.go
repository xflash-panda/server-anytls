//nolint:errcheck
package server

import (
	"net"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/service"
)

// TestCountedConnDirectPointerTraffic verifies that CountedConn uses a cached
// TrafficItem pointer for traffic tracking, bypassing sync.Map on every I/O.
func TestCountedConnDirectPointerTraffic(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	item := us.GetTrafficItem(1)

	cc := &CountedConn{
		Conn:        server,
		trafficItem: item,
	}

	// Write from client side, read from CountedConn
	payload := []byte("hello")
	go func() { client.Write(payload) }()

	buf := make([]byte, 64)
	n, err := cc.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	// Verify upload traffic recorded via cached pointer
	if item.Up.Load() != uint64(n) {
		t.Fatalf("expected Up=%d, got %d", n, item.Up.Load())
	}

	// Write from CountedConn to client
	go func() {
		b := make([]byte, 64)
		client.Read(b)
	}()

	n, err = cc.Write(payload)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Verify download traffic recorded
	if item.Down.Load() != uint64(n) {
		t.Fatalf("expected Down=%d, got %d", n, item.Down.Load())
	}
}
